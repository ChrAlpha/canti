package docker

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

// requestFence is an intent recorded before a single Docker mutation. A missing
// acknowledgement never permits same-container reuse, regardless of later
// container samples. Controller recovery must preserve this distinction.
type requestFence struct {
	Format                                   int
	Kind, BackendID, EnvironmentID, TargetID string
	Incarnation, Generation                  uint64
}

type requestAcknowledgement struct {
	Format        int
	RequestDigest string
	Proof         string
	Status        int `json:",omitempty"`
	ConfirmedAt   time.Time
}

// beginRequest creates a unique immutable intent. Its directory must be retained
// by the environment authority until the pinned container is explicitly replaced.
func beginRequest(authority string, request requestFence) (string, error) {
	if err := request.validate(); err != nil {
		return "", err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	parent := filepath.Join(authority, "requests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	dir := filepath.Join(parent, hex.EncodeToString(nonce[:]))
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", err
	}
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	if err = publish(filepath.Join(dir, "intent.json"), data, true); err != nil {
		return "", err
	}
	if err = syncDir(parent); err != nil {
		return "", err
	}
	if err = syncDir(authority); err != nil {
		return "", err
	}
	return dir, nil
}

func (r requestFence) validate() error {
	if r.Format != 1 || r.BackendID == "" || r.EnvironmentID == "" || r.TargetID == "" || r.Incarnation == 0 || r.Generation == 0 {
		return errors.New("docker: incomplete request fence")
	}
	switch r.Kind {
	case "exec_start", "environment_stop", "environment_start":
		return nil
	}
	return errors.New("docker: unsupported fenced request")
}

// acknowledgeRequest is called only after the specific non-replayed request
// returns successfully, or (for exec_start only) its exact exec is positively
// observed started. A stopped-container sample is deliberately not a proof.
func acknowledgeRequest(dir string, request requestFence, response int, execStarted bool) error {
	if err := request.validate(); err != nil {
		return err
	}
	proof := ""
	switch {
	case execStarted && request.Kind == "exec_start":
		proof = "exec_started"
	case !execStarted && responseAcknowledges(request.Kind, response):
		proof = "http_success"
	default:
		return errors.New("docker: mutation has no definitive acknowledgement; keep environment quarantined")
	}
	digest, err := requestIdentity(dir, request)
	if err != nil {
		return err
	}
	if settled, err := requestSettled(dir, request); err != nil || settled {
		return err
	}
	ack := requestAcknowledgement{Format: 1, RequestDigest: digest, Proof: proof, Status: response, ConfirmedAt: time.Now().UTC()}
	data, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return publish(filepath.Join(dir, "ack.json"), data, true)
}

// requestSettled survives controller death. It cannot infer settlement from an
// inspect response, an elapsed timeout, or release of the controller's flock.
func requestSettled(dir string, request requestFence) (bool, error) {
	digest, err := requestIdentity(dir, request)
	if err != nil {
		return false, err
	}
	var ack requestAcknowledgement
	found, err := readJSON(filepath.Join(dir, "ack.json"), &ack)
	if err != nil || !found {
		return false, err
	}
	if ack.Format != 1 || ack.RequestDigest != digest || ack.ConfirmedAt.IsZero() || ack.Proof != "http_success" && (ack.Proof != "exec_started" || request.Kind != "exec_start") {
		return false, errors.New("docker: acknowledgement contradicts its request fence")
	}
	if ack.Proof == "http_success" && !responseAcknowledges(request.Kind, ack.Status) {
		return false, errors.New("docker: unexpected endpoint acknowledgement status")
	}
	// A failed publication can have made the name visible before directory sync.
	if err = syncDir(dir); err != nil {
		return false, err
	}
	return true, nil
}

func requestIdentity(dir string, request requestFence) (string, error) {
	if err := request.validate(); err != nil {
		return "", err
	}
	var saved requestFence
	found, err := readJSON(filepath.Join(dir, "intent.json"), &saved)
	if err != nil {
		return "", err
	}
	if !found || saved != request {
		return "", errors.New("docker: missing or mismatched mutation intent")
	}
	data, err := json.Marshal(saved)
	if err != nil {
		return "", err
	}
	return harness.ConfigDigest(append([]byte(filepath.Base(dir)+"\x00"), data...)), nil
}

func responseAcknowledges(kind string, status int) bool {
	switch kind {
	case "exec_start":
		return status == http.StatusOK
	case "environment_stop", "environment_start":
		return status == http.StatusNoContent || status == http.StatusNotModified
	}
	return false
}

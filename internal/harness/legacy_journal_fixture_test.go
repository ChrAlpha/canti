package harness_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// A legacy journal case is a directory testdata/legacy-journals/<producer
// SHA>/<case> holding a journal written by an unmodified historical canti
// binary, the history, artifact, and import roots it left beside it, and the
// manifest that describes how it was produced. Cases are immutable: a new
// fixture gets a new case name.
var legacyDir = filepath.Join("testdata", "legacy-journals")

// The producers of the corpus: the last schema-1 revision, and a semantics-2
// revision whose production code equals the current one at the time of
// generation.
const (
	legacySchema1    = "b9446723f59eb87923d81912b4a7a78fce8dbc68"
	legacySemantics2 = "7624f85e8d0c2ad759c1b18c80a2facc57c9b41a"
)

const (
	legacyManifestName = "manifest.json"
	legacyDBName       = "journal.db.gz"
	legacyRootsName    = "roots.tar.gz"
	legacyMaxDB        = 16 << 20
	legacyMaxCorpus    = 4 << 20
)

type legacyFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type legacyManifest struct {
	Format      int                `json:"format"`
	Case        string             `json:"case"`
	Producer    legacyProducer     `json:"producer"`
	Generator   legacyGenerator    `json:"generator"`
	Paths       legacyPaths        `json:"paths"`
	Environment []string           `json:"environment"`
	Invocations []legacyInvocation `json:"invocations"`
	Replies     []legacyReply      `json:"replies"`
	Checkpoint  legacyCheckpoint   `json:"checkpoint"`
	Journal     legacyJournal      `json:"journal"`
	Capacity    *legacyCapacity    `json:"capacity,omitempty"`
	Coverage    []string           `json:"coverage"`
	Files       []legacyFile       `json:"files"`
	Roots       []legacyFile       `json:"roots"`
}

type legacyProducer struct {
	SHA       string     `json:"sha"`
	Tree      string     `json:"tree"`
	Binary    legacyFile `json:"binary"`
	GoVersion string     `json:"go_version"`
	Build     []string   `json:"build"`
}

type legacyGenerator struct {
	Files     []legacyFile `json:"files"`
	GoVersion string       `json:"go_version"`
	SQLite    string       `json:"sqlite"`
}

type legacyPaths struct {
	Root      string `json:"root"`
	Workspace string `json:"workspace"`
	State     string `json:"state"`
	Imports   string `json:"imports,omitempty"`
}

type legacyInvocation struct {
	Args  []string `json:"args"`
	Exit  string   `json:"exit"`
	Phase string   `json:"phase"`
}

type legacyReply struct {
	Run      int    `json:"run"`
	Step     int    `json:"step"`
	Attempt  int    `json:"attempt"`
	Request  string `json:"request_sha256"`
	Decision string `json:"decision,omitempty"`
	Hold     bool   `json:"hold,omitempty"`
}

type legacyCheckpoint struct {
	Busy         int    `json:"busy"`
	Log          int    `json:"log"`
	Checkpointed int    `json:"checkpointed"`
	Integrity    string `json:"integrity"`
	JournalMode  string `json:"journal_mode"`
}

// legacyJournal summarizes the raw database; the consumer recomputes it from
// its own snapshot before the current store opens the file.
type legacyJournal struct {
	DB          legacyFile       `json:"db"`
	UserVersion int              `json:"user_version"`
	Rows        map[string]int   `json:"rows"`
	Kinds       []string         `json:"kinds"`
	Runs        []legacyRunState `json:"runs"`
}

type legacyRunState struct {
	N          int64  `json:"n"`
	ID         string `json:"id"`
	Semantics  int64  `json:"semantics"`
	SessionID  string `json:"session_id,omitempty"`
	SessionSeq int64  `json:"session_seq,omitempty"`
	InputID    string `json:"input_id,omitempty"`
	Seq        int64  `json:"seq"`
	Phase      string `json:"phase"`
	Step       int    `json:"step"`
	Attempt    int    `json:"attempt"`
	RequestRev int    `json:"request_rev"`
	EffectID   string `json:"effect_id,omitempty"`
	JobID      string `json:"job_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// legacyCapacity is the producer's own evaluation of the handover trigger:
// at the last request the journal holds, and at the next preparation, which
// the producer ran on a discarded copy of the finished state directory.
type legacyCapacity struct {
	ContextBytes int                 `json:"context_bytes"`
	Last         legacyHandoverCheck `json:"last_request"`
	Probe        legacyHandoverCheck `json:"next_request_probe"`
	ProbeArgs    []string            `json:"probe_args"`
}

type legacyHandoverCheck struct {
	Step      int      `json:"step"`
	Status    string   `json:"status"`
	Used      int      `json:"used"`
	Capacity  int      `json:"capacity"`
	Growth    int      `json:"growth"`
	Headroom  int      `json:"headroom"`
	Requested bool     `json:"requested"`
	Reasons   []string `json:"reasons,omitempty"`
	Body      string   `json:"body_sha256"`
}

func legacyHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func legacyFileOf(name string, data []byte) legacyFile {
	return legacyFile{Path: name, Bytes: int64(len(data)), SHA256: legacyHash(data)}
}

func legacyStrict(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// legacyCaseNames lists the <producer SHA>/<case> directories of the corpus.
func legacyCaseNames(t *testing.T) []string {
	t.Helper()
	producers, err := os.ReadDir(legacyDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range producers {
		cases, err := os.ReadDir(filepath.Join(legacyDir, p.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			if !c.IsDir() {
				t.Errorf("%s/%s is not a case directory", p.Name(), c.Name())
				continue
			}
			names = append(names, p.Name()+"/"+c.Name())
		}
	}
	slices.Sort(names)
	return names
}

// legacyCopy is a verified case expanded into a fresh directory: the journal
// at db and the producer's roots under roots.
type legacyCopy struct {
	manifest legacyManifest
	db       string
	roots    string
	files    map[string][]byte
	stored   int64
}

// openLegacyCase verifies every file of a case against its manifest and
// expands the journal and roots into a new temporary directory.
func openLegacyCase(t *testing.T, name string) legacyCopy {
	t.Helper()
	dir := filepath.Join(legacyDir, filepath.FromSlash(name))
	data, err := os.ReadFile(filepath.Join(dir, legacyManifestName))
	if err != nil {
		t.Fatal(err)
	}
	c := legacyCopy{files: map[string][]byte{}}
	if err := legacyStrict(data, &c.manifest); err != nil {
		t.Fatalf("%s: manifest: %v", name, err)
	}
	m := c.manifest
	if m.Format != 1 || m.Case != name || path.Dir(name) != m.Producer.SHA || len(m.Producer.SHA) != 40 {
		t.Fatalf("%s: manifest names format %d, case %q, producer %q", name, m.Format, m.Case, m.Producer.SHA)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, want := []string{}, []string{legacyManifestName}
	for _, e := range entries {
		got = append(got, e.Name())
	}
	stored := map[string][]byte{}
	for _, f := range m.Files {
		want = append(want, f.Path)
		b, err := os.ReadFile(filepath.Join(dir, f.Path))
		if err != nil {
			t.Fatal(err)
		}
		if legacyFileOf(f.Path, b) != f {
			t.Fatalf("%s: %s does not match its manifest entry %+v", name, f.Path, f)
		}
		stored[f.Path] = b
		c.stored += int64(len(b))
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("%s holds %v, its manifest lists %v", name, got, want)
	}
	db := legacyGunzip(t, stored[legacyDBName])
	if legacyFileOf("journal.db", db) != m.Journal.DB {
		t.Fatalf("%s: expanded journal does not match %+v", name, m.Journal.DB)
	}
	tmp := t.TempDir()
	c.db, c.roots = filepath.Join(tmp, "canti.db"), filepath.Join(tmp, "roots")
	if err := os.WriteFile(c.db, db, 0o600); err != nil {
		t.Fatal(err)
	}
	var members []legacyFile
	for name, b := range legacyUntar(t, legacyGunzip(t, stored[legacyRootsName]), c.roots) {
		members = append(members, legacyFileOf(name, b))
		c.files[name] = b
	}
	byPath := func(a, b legacyFile) int { return strings.Compare(a.Path, b.Path) }
	slices.SortFunc(members, byPath)
	if !slices.Equal(members, m.Roots) {
		t.Fatalf("%s: roots hold %+v, the manifest lists %+v", name, members, m.Roots)
	}
	return c
}

func legacyGunzip(t *testing.T, data []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(io.LimitReader(r, legacyMaxDB+1))
	if err == nil && len(out) > legacyMaxDB {
		err = fmt.Errorf("expands beyond %d bytes", legacyMaxDB)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// legacyUntar writes the directories and regular files of a tar archive
// under dir and returns the files by slash-separated name.
func legacyUntar(t *testing.T, data []byte, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := r.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(h.Name, "/")
		if name == "" || path.Clean(name) != name || path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			t.Fatalf("roots entry %q is not a clean relative path", h.Name)
		}
		target := filepath.Join(dir, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(target, 0o755)
		case tar.TypeReg:
			var b []byte
			if b, err = io.ReadAll(r); err == nil {
				files[name] = b
				err = os.MkdirAll(filepath.Dir(target), 0o755)
			}
			if err == nil {
				err = os.WriteFile(target, b, 0o644)
			}
		default:
			err = fmt.Errorf("roots entry %q has type %q", h.Name, h.Typeflag)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

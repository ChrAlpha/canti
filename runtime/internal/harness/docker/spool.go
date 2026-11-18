package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// spoolEvidence is the sole host read boundary for mutable helper evidence.
// Root the command lookup inside the writable spool, then open bounded regular
// files without following final symlinks or blocking on special files.
func spoolEvidence(directory, effectID, name string, limit int64) ([]byte, error) {
	if err := checkID(effectID); err != nil {
		return nil, err
	}
	if name != "output" && name != "completion.json" && name != jobrunner.PreviewFile {
		return nil, errors.New("docker: unknown command evidence file")
	}
	spool, err := os.OpenRoot(filepath.Join(directory, "spool"))
	if err != nil {
		return nil, err
	}
	defer spool.Close()
	command, err := spool.OpenRoot(effectID)
	if err != nil {
		return nil, err
	}
	defer command.Close()
	return jobrunner.ReadEvidence(command, name, limit)
}

// previewEvidence reads the rendered preview the helper published beside its
// output and checks it against the completion record. A missing, replaced,
// truncated or oversized preview never becomes model evidence.
func (x *Attached) previewEvidence(effectID string, rec jobrunner.Record) (string, error) {
	data, err := spoolEvidence(x.cfg.Attachment.Directory, effectID, jobrunner.PreviewFile, jobrunner.MaxPreviewBytes)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(data) || len(data) > jobrunner.MaxPreviewBytes {
		return "", errors.New("docker: command preview evidence is invalid or oversized")
	}
	if !validPreview(data, rec, x.cfg.PreviewLimit) {
		return "", errors.New("docker: command preview evidence does not match its completion record")
	}
	return string(data), nil
}

func validPreview(data []byte, rec jobrunner.Record, limit int) bool {
	if !utf8.Valid(data) || len(data) > limit || len(data) > jobrunner.MaxPreviewBytes {
		return false
	}
	sum := sha256.Sum256(data)
	return rec.PreviewDigest == hex.EncodeToString(sum[:]) && rec.PreviewBytes == len(data) &&
		rec.PreviewLines == jobrunner.PreviewLines(string(data)) && rec.PreviewBytes >= 0 && rec.PreviewLines >= 0 &&
		rec.PreviewBytes <= limit && rec.PreviewLines <= jobrunner.MaxPreviewLines
}

func unavailablePreview(path string) string {
	return "... preview unavailable; read the output artifact at " + path + " ..."
}

func partialUnavailablePreview(path string) string {
	return "... preview unavailable; command was cancelled; output is partial; read the output artifact at " + path + " ..."
}

// cancelledProjection decorates the helper's recorded preview from the
// structured cancellation state. It never searches the command's bytes for a
// marker: PreviewMarked says whether the rendered preview contains one, and
// the recorded totals decide whether the artifact claim would be false.
func cancelledProjection(preview string, rec jobrunner.Record, path string, limit int) string {
	note := "... command was cancelled; output is partial; read the output artifact at " + path + " ..."
	if preview == "" {
		return partialUnavailablePreview(path)
	}
	if rec.PreviewMarked {
		markerStart, markerEnd := rec.PreviewMarkerStart, rec.PreviewMarkerStart+rec.PreviewMarkerLength
		if rec.PreviewCancelledMarker == "" || rec.PreviewMarkerLength <= 0 || markerStart < 0 || markerEnd < markerStart || markerEnd > len(preview) {
			return partialUnavailablePreview(path)
		}
		preview = preview[:markerStart] + rec.PreviewCancelledMarker + preview[markerEnd:]
		if len(preview) > limit || len(preview) > jobrunner.MaxPreviewBytes || jobrunner.PreviewLines(preview) > jobrunner.MaxPreviewLines {
			return partialUnavailablePreview(path)
		}
		return preview
	}
	if !strings.HasSuffix(preview, "\n") {
		preview += "\n"
	}
	preview += note
	if len(preview) > limit || len(preview) > jobrunner.MaxPreviewBytes || jobrunner.PreviewLines(preview) > jobrunner.MaxPreviewLines {
		return partialUnavailablePreview(path)
	}
	return preview
}

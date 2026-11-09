package docker

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
)

// spoolEvidence is the sole host read boundary for mutable helper evidence.
// Root the command lookup inside the writable spool, then open bounded regular
// files without following final symlinks or blocking on special files.
func spoolEvidence(directory, effectID, name string, limit int64) ([]byte, error) {
	if err := checkID(effectID); err != nil {
		return nil, err
	}
	if name != "output" && name != "completion.json" {
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

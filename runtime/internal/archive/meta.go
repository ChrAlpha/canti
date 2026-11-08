package archive

import (
	"cmp"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// A published step is the directory StepDir(step) under
// RunsRoot/<run ID>/StepsDir holding MetaFile, the whole command as
// CommandFile, the whole decision purpose as PurposeFile, and the preview as
// PreviewFile. A step published before PurposeFile existed lacks it.
const (
	StepsDir     = "steps"
	MetaFile     = "meta"
	CommandFile  = "command"
	PurposeFile  = "purpose"
	PreviewFile  = "preview"
	EvidenceFile = "evidence.json"
	EvidenceDir  = "evidence"
)

const unavailable = "unavailable"

var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

func StepDir(step int) string { return fmt.Sprintf("%08d", step) }

// StepMeta is the content of MetaFile. Output is the artifact path of the
// step's output, empty when it is unavailable.
type StepMeta struct {
	Step           int
	Execution      string
	ExitCode       *int
	Resolved       bool
	Output         string
	OutputBytes    int64
	OutputComplete bool
	OutputDetail   string
}

// Encode returns one "key: value" line per field, line breaks in a value
// replaced by spaces.
func (m StepMeta) Encode() string {
	var b strings.Builder
	line := func(key, value string) { b.WriteString(key + ": " + lineBreaks.Replace(value) + "\n") }
	line("step", strconv.Itoa(m.Step))
	line("execution", m.Execution)
	if m.ExitCode != nil {
		line("exit_code", strconv.Itoa(*m.ExitCode))
	}
	line("resolved", strconv.FormatBool(m.Resolved))
	line("output", cmp.Or(m.Output, unavailable))
	line("output_bytes", strconv.FormatInt(m.OutputBytes, 10))
	line("output_complete", strconv.FormatBool(m.OutputComplete))
	if m.OutputDetail != "" {
		line("output_detail", m.OutputDetail)
	}
	return b.String()
}

// decodeOutput returns the output fields of the meta of step that discovery
// reads: the output line, its retained size, and whether it is complete.
func decodeOutput(meta []byte, step int) (string, int64, bool, error) {
	fields := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSuffix(string(meta), "\n"), "\n") {
		key, value, ok := strings.Cut(line, ": ")
		if _, duplicate := fields[key]; !ok || duplicate {
			return "", 0, false, errors.New("invalid original step metadata")
		}
		fields[key] = value
	}
	complete, err := strconv.ParseBool(fields["output_complete"])
	bytes, sizeErr := strconv.ParseInt(fields["output_bytes"], 10, 64)
	if err != nil || sizeErr != nil || bytes < 0 || fields["step"] != strconv.Itoa(step) {
		return "", 0, false, errors.New("invalid original step metadata")
	}
	return fields["output"], bytes, complete, nil
}

// EvidenceManifest makes the exact retrieval observation independently
// navigable in the archive. Files contain the committed bytes, never rereads.
type EvidenceManifest struct {
	Seq          int64            `json:"seq"`
	Query        RetrievalQuery   `json:"query"`
	Sources      []EvidenceSource `json:"sources"`
	Entries      []string         `json:"entries,omitempty"`
	NextOffset   int64            `json:"next_offset"`
	NextCursor   string           `json:"next_cursor,omitempty"`
	More         bool             `json:"more"`
	Incomplete   bool             `json:"incomplete,omitempty"`
	SkippedCount int              `json:"skipped_count,omitempty"`
	Skipped      []Skip           `json:"skipped,omitempty"`
	Error        string           `json:"error,omitempty"`
}

type EvidenceSource struct {
	SourceRange
	Source int    `json:"source"`
	File   string `json:"file"`
}

func EvidencePath(source int) string { return fmt.Sprintf("%s/%04d", EvidenceDir, source) }

// SnapshotTarget is one native historical retrieval snapshot address.
// Source is nil for its manifest or evidence directory.
type SnapshotTarget struct {
	RunID  string
	Step   int
	Source *int
}

// ParseSnapshotPath recognizes only canonical archive layout paths. The safe
// reader still validates and opens the path after the harness repairs it.
func ParseSnapshotPath(name string) (SnapshotTarget, bool) {
	var target SnapshotTarget
	if path.Clean(name) != name {
		return target, false
	}
	relative, ok := strings.CutPrefix(name, RunsRoot+"/")
	if !ok {
		return target, false
	}
	parts := strings.Split(relative, "/")
	if len(parts) != 4 && len(parts) != 5 || parts[0] == "" || parts[1] != StepsDir {
		return target, false
	}
	step, err := strconv.Atoi(parts[2])
	if err != nil || step < 1 || StepDir(step) != parts[2] {
		return target, false
	}
	target = SnapshotTarget{RunID: parts[0], Step: step}
	if len(parts) == 4 {
		return target, parts[3] == EvidenceFile || parts[3] == EvidenceDir
	}
	source, err := strconv.Atoi(parts[4])
	if parts[3] != EvidenceDir || err != nil || source < 0 || source >= MaxSources || parts[4] != fmt.Sprintf("%04d", source) {
		return SnapshotTarget{}, false
	}
	target.Source = &source
	return target, true
}

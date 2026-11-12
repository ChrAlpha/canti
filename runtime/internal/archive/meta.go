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

// InputDir is the published input directory of agent-log position pos.
func InputDir(pos int64) string { return fmt.Sprintf("%012d", pos) }

// ErrInputAbsent reports an input the agent was never shown; it has no
// archive record and none may be created.
var ErrInputAbsent = errors.New("archive: input was not delivered")

// inputPosition parses the input position from a virtual input path.
func inputPosition(name string) (int64, bool) {
	rest, ok := strings.CutPrefix(name, InputsRoot+"/")
	if !ok {
		return 0, false
	}
	first, _, _ := strings.Cut(rest, "/")
	pos, err := strconv.ParseInt(first, 10, 64)
	if err != nil || pos < 1 || InputDir(pos) != first {
		return 0, false
	}
	return pos, true
}

// InputMeta is the immutable content of an input's meta file. Run is the run
// the input opened or targeted at admission, empty for a queued input and an
// observation. A later opening, withdrawal or disposition change is a log
// fact and never rewrites this record.
type InputMeta struct {
	Authority string
	Delivery  string
	Run       string
	InputID   string
	Source    string
	Subject   string
	Admitted  string
}

// Encode returns one "key: value" line per field, line breaks in a value
// replaced by spaces.
func (m InputMeta) Encode() string {
	var b strings.Builder
	line := func(key, value string) { b.WriteString(key + ": " + lineBreaks.Replace(value) + "\n") }
	line("authority", m.Authority)
	line("delivery", m.Delivery)
	line("run", m.Run)
	line("input_id", m.InputID)
	line("source", m.Source)
	line("subject", m.Subject)
	line("admitted", m.Admitted)
	return b.String()
}

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

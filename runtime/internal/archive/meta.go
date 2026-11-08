package archive

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// A published step is the directory StepDir(step) under
// RunsRoot/<run ID>/StepsDir holding MetaFile, the whole command as
// CommandFile, the whole decision purpose as PurposeFile, and the preview as
// PreviewFile. A step published before PurposeFile existed lacks it.
const (
	StepsDir    = "steps"
	MetaFile    = "meta"
	CommandFile = "command"
	PurposeFile = "purpose"
	PreviewFile = "preview"
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

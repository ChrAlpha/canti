package harness

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/internal/archive"
)

// PromptLimits bounds a prompt in bytes. Goal and Constraints cap the
// trimmed goal and constraints, which are refused rather than cut. Every
// other field but Total caps the bytes kept from one input: Command,
// Purpose, and Preview apply to each step, Rejected to both the error and
// the response. A cut input ends with a marker counting the bytes removed.
// Total caps System plus the blocks.
type PromptLimits struct {
	Goal, Constraints, Environment, Command, Preview, Rejected, Total, Purpose int
}

// DefaultPromptLimits returns limits for a 128 KiB prompt.
func DefaultPromptLimits() PromptLimits {
	return PromptLimits{
		Goal: 32 << 10, Constraints: 16 << 10, Environment: 2 << 10, Command: 2 << 10,
		Preview: 4 << 10, Rejected: 2 << 10, Total: 128 << 10, Purpose: 512,
	}
}

// refLimit caps the execution state, output path and detail, artifact and
// runs directories, and run ID.
const refLimit = 1 << 10

var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

const (
	systemRole = `You are an autonomous agent working in a shell workspace. Each request stands alone: the user message holds everything known about the task, and your reply chooses the next step.

`
	systemForms = ` in one of these forms:

{"kind":"exec","purpose":"<what this step is for and what you will do next with its result>","payload":"<one POSIX shell command>"}
Runs the command with /bin/sh -c in /workspace. Each step runs in a fresh process, and only files under /workspace persist between steps: the working directory, environment variables, and anything outside /workspace do not. Standard output and standard error are captured and reported in the next request.

{"kind":"return","purpose":"<why the run ends here>","payload":"<final answer, question, or blocker for the user>"}
Ends the run and hands the payload to the user. It does not claim success: use it to deliver a result, ask a question, or report a blocker.

Keep purpose to one short line. It is shown with the step in later requests, so state the next action you intend once you see the result.`
	systemContext = `

The user message reports the goal, your progress, and the steps of the current context window with a bounded preview of each output. Full outputs are files under the artifact directory named in the Environment section, at the path given for each step, and the step records named there keep the full command, outcome, and output path of every completed step, including the steps of earlier windows.

Work continues across bounded context windows. The handover is the only automatically carried working state. Current goal and constraints remain authoritative. When asked to hand over, use the exact canti:handover action specified in the request; this records guidance and continues the same run.`

	system = systemRole + `Reply with exactly one JSON object and nothing else: no Markdown, no code fences, no text before or after it. The object has three string fields, "kind", "purpose", and "payload",` + systemForms + `

The purpose and payload are JSON strings, so escape quotes, backslashes, and newlines.` + systemContext
	toolSystem = systemRole + `Call the canti_decision tool exactly once per reply; never reply with text. Its input has three string fields, "kind", "purpose", and "payload",` + systemForms + systemContext
)

// Prompt renders the frame within l. System depends only on the decision
// transport and the enabled retrieval of the frame.
// The stable blocks are one with the goal, constraints, and environment and
// one per current-page step, oldest first; the last block holds the progress
// and any rejected response. An oversized page fails visibly. Equal frames
// and limits give equal prompts. The goal and
// constraints are never cut: Prompt fails when either exceeds its limit
// after trimming.
func (f Frame) Prompt(l PromptLimits) (Prompt, error) {
	system := f.contextSystem()
	if err := f.Validate(); err != nil {
		return Prompt{}, err
	}
	if min(l.Goal, l.Constraints, l.Environment, l.Command, l.Preview, l.Rejected, l.Total, l.Purpose) <= 0 {
		return Prompt{}, fmt.Errorf("harness: prompt limits %+v must be positive", l)
	}
	if n := len(strings.TrimSpace(f.Goal)); n > l.Goal {
		return Prompt{}, fmt.Errorf("harness: goal exceeds its %d-byte limit by %d bytes", l.Goal, n-l.Goal)
	}
	if n := len(strings.TrimSpace(f.Constraints)); n > l.Constraints {
		return Prompt{}, fmt.Errorf("harness: constraints exceed their %d-byte limit by %d bytes", l.Constraints, n-l.Constraints)
	}
	capture := ContextBytes{System: len(system), Limit: l.Total}
	head := f.headContext(l, &capture)
	blocks := make([]string, 0, len(f.Steps)+2)
	blocks = append(blocks, head)
	used := len(system) + len(head)
	for _, step := range f.Steps {
		b := f.step(step, l)
		blocks = append(blocks, b)
		used += len(b)
		if f.capture != nil {
			capture.Steps = append(capture.Steps, ContextStepBytes{Step: step.Step, Bytes: len(b)})
		}
	}
	tail := f.tailContext(l, f.progress(f.Earlier), &capture)
	if len(f.Steps) == 0 {
		tail = "None.\n\n" + tail
		capture.EmptySteps = len("None.\n\n")
	}
	used += len(tail)
	if used > l.Total {
		return Prompt{}, fmt.Errorf("harness: current context page exceeds its %d-byte total by %d bytes", l.Total, used-l.Total)
	}
	if f.capture != nil {
		capture.Total = used
		f.capture.Bytes = capture
	}
	return Prompt{System: system, Blocks: append(blocks, tail), Stable: 1 + len(f.Steps)}, nil
}

func (f Frame) head(l PromptLimits) string {
	var capture ContextBytes
	return f.headContext(l, &capture)
}

func (f Frame) headContext(l PromptLimits, capture *ContextBytes) string {
	var b strings.Builder
	capture.Goal = section(&b, "Goal", clip(strings.TrimSpace(f.Goal), math.MaxInt)+"\n")
	if c := strings.TrimSpace(f.Constraints); c != "" {
		capture.Constraints = section(&b, "Constraints", clip(c, math.MaxInt)+"\n")
	}
	capture.Environment = section(&b, "Environment", clip(strings.TrimSpace(f.Environment), l.Environment)+"\n"+
		"Full step outputs are readable under "+inline(f.ArtifactDir)+".\n"+
		"Every completed step of this run, shown or not, is recorded read-only under "+inline(f.RunsDir)+"/"+inline(f.RunID)+"/steps/<step number in 8 digits>/ "+
		"as meta (its execution, exit code, and output path), command (the full command), and preview.\n"+
		"Every run of this state directory, including this one, is recorded under "+inline(f.RunsDir)+"/<run ID>/ with its goal, its steps, and its result once it ends.\n")
	if f.SessionContext != "" {
		capture.Session = section(&b, "Earlier session dialogue", f.SessionContext)
	}
	if f.Window != nil {
		capture.Window = section(&b, "Context window", fmt.Sprintf("This window begins at step %d. Earlier windows remain in the read-only history.\n", f.Window.FirstStep))
		if f.Window.Handover != "" {
			capture.Handover = section(&b, "Handover", "Agent-written continuation guidance, not verified facts. Check original evidence before relying on it.\n"+fullBlock(f.Window.Handover))
		}
	}
	capture.StepHeader = section(&b, "Current window steps", "")
	return b.String()
}

func (f Frame) tail(l PromptLimits, progress string) string {
	var capture ContextBytes
	return f.tailContext(l, progress, &capture)
}

func (f Frame) tailContext(l PromptLimits, progress string, capture *ContextBytes) string {
	var b strings.Builder
	capture.Progress = section(&b, "Progress", progress)
	if f.Window != nil && f.Window.Requested {
		capture.HandoverRequired = section(&b, "Handover required", fmt.Sprintf("Before any more work, reply with exec whose payload is canti:handover followed by one JSON object with run_id %q, first_step %d, and text of at most %d UTF-8 bytes. Aim for at most %d bytes in a few short lines to leave room below the hard limit. Include current objective, work location, unfinished obligations, next action, active risks, and original evidence references. Point to workspace records for completed work instead of recounting it. Do not use return to switch windows. Do not copy the whole history or treat the previous handover as authority.\n", f.RunID, f.Window.FirstStep, f.HandoverBytes, max(1, f.HandoverBytes*3/4)))
	}
	if r := f.Rejected; r != nil {
		capture.Repair = section(&b, "Rejected response", "Your previous reply for this step could not be decoded. "+
			"Your reply must be exactly one decision object, as the system message describes.\n"+
			quote("Error", r.Error, l.Rejected)+quote("Rejected reply", r.Response, l.Rejected))
	}
	return b.String()
}

func section(b *strings.Builder, title, body string) int {
	before := b.Len()
	if b.Len() > 0 {
		b.WriteString("\n")
	}
	b.WriteString("## " + title + "\n" + body)
	return b.Len() - before
}

func (f Frame) progress(hidden int) string {
	s := fmt.Sprintf("Step %d of at most %d, attempt %d of %d.\n", f.Step, f.MaxSteps, f.Attempt, f.MaxAttempts)
	if f.MaxSteps == 0 {
		s = fmt.Sprintf("Step %d (unlimited steps), attempt %d of %d.\n", f.Step, f.Attempt, f.MaxAttempts)
	}
	switch {
	case hidden == 1:
		s += "1 earlier step is not shown.\n"
	case hidden > 1:
		s += strconv.Itoa(hidden) + " earlier steps are not shown.\n"
	}
	return s
}

func (f Frame) step(s StepRecord, l PromptLimits) string {
	o := s.Observation
	var b strings.Builder
	b.Grow(max(0, min(len(s.Command), l.Command)) + max(0, min(len(s.Purpose), l.Purpose)) + max(0, min(len(o.Preview), l.Preview)) + len(f.Retrieved[s.Step]) + 256)
	fmt.Fprintf(&b, "### Step %d\n", s.Step)
	if s.Purpose != "" {
		b.WriteString("Purpose: " + lineBreaks.Replace(clip(s.Purpose, l.Purpose)) + "\n")
	}
	writeQuote(&b, "Command", s.Command, l.Command)
	fmt.Fprintf(&b, "Execution: %s", inline(string(o.Execution)))
	if o.ExitCode != nil {
		fmt.Fprintf(&b, ", exit code %d", *o.ExitCode)
	}
	if s.Resolved {
		b.WriteString(", set by explicit resolution")
	}
	if r := o.Retrieval; r != nil {
		kind := "original ranges"
		if r.Query.Operation == "discover" {
			kind = "candidate ranges"
		}
		fmt.Fprintf(&b, "\nRetrieval: %d %s, %d directory entries", len(r.Sources), kind, len(r.Entries))
	} else if o.Handover != nil {
		b.WriteString("\nHandover proposal recorded")
	} else {
		complete, where := "incomplete", "unavailable"
		if o.Output.Complete {
			complete = "complete"
		}
		if o.Output.Available {
			where = "at " + path.Join(inline(f.ArtifactDir), inline(o.Output.Path))
		}
		fmt.Fprintf(&b, "\nOutput: %d bytes, %s, %s", o.Output.Bytes, complete, where)
		if o.Output.Detail != "" {
			b.WriteString(" (" + inline(o.Output.Detail) + ")")
		}
	}
	if r := o.Retrieval; r != nil && r.Query.Operation == "list" && r.Error == "" {
		entries, _ := json.Marshal(r.Entries)
		b.WriteString("\nEntries: " + string(entries) + "\n")
	} else if o.Retrieval == nil || len(s.Command)+2*max(0, fence(s.Command)-3) > l.Command {
		b.WriteByte('\n')
		writeQuote(&b, "Preview", o.Preview, l.Preview)
		b.WriteByte('\n')
	} else {
		b.WriteByte('\n')
	}
	if r := o.Retrieval; r != nil {
		status, _ := json.Marshal(struct {
			NextOffset   int64          `json:"next_offset"`
			NextCursor   string         `json:"next_cursor,omitempty"`
			More         bool           `json:"more"`
			Incomplete   bool           `json:"incomplete,omitempty"`
			SkippedCount int            `json:"skipped_count,omitempty"`
			Skipped      []archive.Skip `json:"skipped,omitempty"`
			Error        string         `json:"error,omitempty"`
		}{r.NextOffset, r.NextCursor, r.More, r.Incomplete, r.SkippedCount, r.Skipped, clip(r.Error, 512)})
		b.WriteString("Retrieval continuation: " + string(status) + "\n")
	}
	if text := f.Retrieved[s.Step]; text != "" {
		b.WriteString("Retrieved original evidence:\n")
		writeBlock(&b, text, len(text)+2*max(0, fence(text)-3))
	}
	return b.String()
}

func fullBlock(s string) string {
	return block(s, len(s)+2*max(0, fence(s)-3))
}

func quote(label, s string, limit int) string {
	var b strings.Builder
	writeQuote(&b, label, s, limit)
	return b.String()
}

func writeQuote(b *strings.Builder, label, s string, limit int) {
	b.WriteString(label)
	if s == "" {
		b.WriteString(": empty\n")
		return
	}
	b.WriteString(":\n")
	writeBlock(b, s, limit)
}

// block fences s cut to limit, charging fence marks beyond the usual three
// to the limit, so a backtick-heavy input cannot inflate its block.
func block(s string, limit int) string {
	var b strings.Builder
	writeBlock(&b, s, limit)
	return b.String()
}

func writeBlock(b *strings.Builder, s string, limit int) {
	text := clip(s, limit)
	n := fence(text)
	if n > 3 {
		text = clip(s, max(0, limit-2*(n-3)))
		n = fence(text)
	}
	marks := "```"
	if n > 3 {
		marks = strings.Repeat("`", n)
	}
	b.Grow(len(text) + 2*len(marks) + 3)
	b.WriteString(marks)
	b.WriteByte('\n')
	b.WriteString(text)
	if !strings.HasSuffix(text, "\n") {
		b.WriteByte('\n')
	}
	b.WriteString(marks)
	b.WriteByte('\n')
}

// fence returns a backtick fence length that no line of s can close.
func fence(s string) int {
	if !strings.Contains(s, "`") {
		return 3
	}
	longest, run := 2, 0
	for i := range len(s) {
		run++
		if s[i] != '`' {
			run = 0
		}
		longest = max(longest, run)
	}
	return longest + 1
}

func inline(s string) string { return lineBreaks.Replace(clip(s, refLimit)) }

func clip(s string, limit int) string {
	kept, dropped := cut(s, limit)
	if dropped == 0 {
		return kept
	}
	return kept + "…[truncated " + strconv.Itoa(dropped) + " bytes]"
}

// cut returns the longest prefix of s, with invalid bytes replaced, that fits
// in limit bytes without splitting a rune, and the number of bytes of s left.
func cut(s string, limit int) (string, int) {
	end := min(len(s), max(0, limit))
	if end < len(s) {
		for i := end - 1; i >= 0 && end-i < utf8.UTFMax; i-- {
			if utf8.RuneStart(s[i]) {
				_, size := utf8.DecodeRuneInString(s[i:])
				if i+size > end {
					end = i
				}
				break
			}
		}
	}
	if utf8.ValidString(s[:end]) {
		return s[:end], len(s) - end
	}
	var b strings.Builder
	b.Grow(end)
	i := 0
	for i < len(s) {
		r, n := utf8.DecodeRuneInString(s[i:])
		w := s[i : i+n]
		if r == utf8.RuneError && n == 1 {
			w = "\uFFFD"
		}
		if b.Len()+len(w) > limit {
			break
		}
		b.WriteString(w)
		i += n
	}
	return b.String(), len(s) - i
}

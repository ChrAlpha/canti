package harness

import (
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// PromptLimits bounds a prompt in bytes. Every field but Total caps the bytes
// kept from one input: Command and Preview apply to each step, Rejected to
// both the error and the response. A cut input ends with a marker counting
// the bytes removed. Total caps System plus User.
type PromptLimits struct {
	Goal, Constraints, Environment, Command, Preview, Notes, Rejected, Total int
}

// DefaultPromptLimits returns limits for a 64 KiB prompt.
func DefaultPromptLimits() PromptLimits {
	return PromptLimits{
		Goal: 8 << 10, Constraints: 4 << 10, Environment: 2 << 10, Command: 2 << 10,
		Preview: 4 << 10, Notes: 8 << 10, Rejected: 2 << 10, Total: 64 << 10,
	}
}

// refLimit caps the execution state, output path and detail, artifact
// directory, and notes path.
const refLimit = 1 << 10

var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

const system = `You are an autonomous agent working in a shell workspace. Each request stands alone: the user message holds everything known about the task, and your reply chooses the next step.

Reply with exactly one JSON object and nothing else: no Markdown, no code fences, no text before or after it. The object has exactly two string fields, "kind" and "payload", in one of these forms:

{"kind":"exec","payload":"<one POSIX shell command>"}
Runs the command with /bin/sh -c in /workspace. Each step runs in a fresh process, and only files under /workspace persist between steps: the working directory, environment variables, and anything outside /workspace do not. Standard output and standard error are captured and reported in the next request.

{"kind":"return","payload":"<final answer, question, or blocker for the user>"}
Ends the run and hands the payload to the user. It does not claim success: use it to deliver a result, ask a question, or report a blocker.

The payload is a JSON string, so escape quotes, backslashes, and newlines.

The user message reports the goal, your progress, and the most recent steps with a bounded preview of each output. Full outputs are files under the artifact directory named in the Progress section, at the path given for each step; outputs of steps no longer shown remain there.

Use the notes file named in the Notes section for your plan and findings. It persists between steps and is shown back to you in every request, cut to a size limit. The harness does not verify notes.`

// Prompt renders the frame within l. System is the same for every frame.
// When the prompt would exceed l.Total, Prompt drops the oldest shown steps,
// counting them as earlier steps, down to the newest one; then it shrinks
// the notes; then it drops the newest step as well and shrinks the notes
// again, failing if the prompt still does not fit. Equal frames and limits
// give equal prompts.
func (f Frame) Prompt(l PromptLimits) (Prompt, error) {
	if err := f.Validate(); err != nil {
		return Prompt{}, err
	}
	if min(l.Goal, l.Constraints, l.Environment, l.Command, l.Preview, l.Notes, l.Rejected, l.Total) <= 0 {
		return Prompt{}, fmt.Errorf("harness: prompt limits %+v must be positive", l)
	}
	hidden, notes := f.Earlier+len(f.Steps), l.Notes
	fixed, used := len(system)+len(f.user(l, "", "", notes)), 0
	var blocks []string
	for i := len(f.Steps) - 1; i >= 0; i-- {
		b := f.step(f.Steps[i], l)
		if len(blocks) > 0 {
			b += "\n"
			if fixed+len(f.progress(hidden-1))+used+len(b) > l.Total {
				break
			}
		}
		blocks, used, hidden = append(blocks, b), used+len(b), hidden-1
	}
	slices.Reverse(blocks)
	for {
		steps := strings.Join(blocks, "")
		if steps == "" {
			steps = "None.\n"
		}
		user := f.user(l, f.progress(hidden), steps, notes)
		over := len(system) + len(user) - l.Total
		switch kept, _ := cut(f.Notes.Text, notes); {
		case over <= 0:
			return Prompt{System: system, User: user}, nil
		case kept != "":
			notes = max(0, len(kept)-over)
		case len(blocks) > 0:
			blocks, hidden, notes = nil, hidden+len(blocks), l.Notes
		default:
			return Prompt{}, fmt.Errorf("harness: prompt exceeds its %d-byte total by %d bytes", l.Total, over)
		}
	}
}

func (f Frame) user(l PromptLimits, progress, steps string, notes int) string {
	var b strings.Builder
	section := func(title, body string) {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("## " + title + "\n" + body)
	}
	section("Goal", clip(strings.TrimSpace(f.Goal), l.Goal)+"\n")
	if c := strings.TrimSpace(f.Constraints); c != "" {
		section("Constraints", clip(c, l.Constraints)+"\n")
	}
	section("Environment", clip(strings.TrimSpace(f.Environment), l.Environment)+"\n")
	section("Progress", progress)
	section("Recent steps", steps)
	section("Notes", f.notes(notes))
	if r := f.Rejected; r != nil {
		section("Rejected response", "Your previous reply for this step could not be decoded. "+
			"Your reply must be exactly one decision object, as the system message describes.\n"+
			quote("Error", r.Error, l.Rejected)+quote("Rejected reply", r.Response, l.Rejected))
	}
	return b.String()
}

func (f Frame) progress(hidden int) string {
	s := fmt.Sprintf("Step %d of at most %d, attempt %d of %d.\n", f.Step, f.MaxSteps, f.Attempt, f.MaxAttempts)
	switch {
	case hidden == 1:
		s += "1 earlier step is not shown.\n"
	case hidden > 1:
		s += strconv.Itoa(hidden) + " earlier steps are not shown.\n"
	}
	return s + "Full step outputs are readable under " + inline(f.ArtifactDir) + ".\n"
}

func (f Frame) step(s StepRecord, l PromptLimits) string {
	o := s.Observation
	var b strings.Builder
	fmt.Fprintf(&b, "### Step %d\n%sExecution: %s", s.Step, quote("Command", s.Command, l.Command), inline(string(o.Execution)))
	if o.ExitCode != nil {
		fmt.Fprintf(&b, ", exit code %d", *o.ExitCode)
	}
	if s.Resolved {
		b.WriteString(", set by explicit resolution")
	}
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
	b.WriteString("\n" + quote("Preview", o.Preview, l.Preview))
	return b.String()
}

func (f Frame) notes(limit int) string {
	s := "Written by the agent in " + inline(f.Notes.Path) + " and not verified by the harness.\n"
	switch {
	case f.Notes.Text != "":
		s += block(f.Notes.Text, limit)
	case !f.Notes.Truncated:
		return s + "The file is empty or missing.\n"
	}
	if f.Notes.Truncated {
		s += "The file continues beyond what was read.\n"
	}
	return s
}

func quote(label, s string, limit int) string {
	if s == "" {
		return label + ": empty\n"
	}
	return label + ":\n" + block(s, limit)
}

// block fences s cut to limit, charging fence marks beyond the usual three
// to the limit, so a backtick-heavy input cannot inflate its block.
func block(s string, limit int) string {
	text := clip(s, limit)
	if n := fence(text); n > 3 {
		text = clip(s, max(0, limit-2*(n-3)))
	}
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	marks := strings.Repeat("`", fence(text))
	return marks + "\n" + text + marks + "\n"
}

// fence returns a backtick fence length that no line of s can close.
func fence(s string) int {
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
	if len(s) <= limit && utf8.ValidString(s) {
		return s, 0
	}
	var b strings.Builder
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

package harness

import (
	"encoding/json"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

func budgetFrame() Frame {
	return Frame{Goal: "continue work", Environment: "test",
		RunID: "run-1", Step: 1, MaxSteps: math.MaxInt,
		Attempt: 1, MaxAttempts: 3, Window: &Window{FirstStep: 1},
		RetrievalEnabled: true, HandoverBytes: 2048}
}

func TestContextAdmissionReservesMaximumHandover(t *testing.T) {
	l := DefaultPromptLimits()
	f := budgetFrame()
	if err := f.ValidateContext(l); err != nil {
		t.Fatal(err)
	}
	f.Window = &Window{FirstStep: math.MaxInt, Handover: strings.Repeat("`", f.HandoverBytes)}
	if _, err := f.workCapacity(l); err != nil {
		t.Fatal(err)
	}
	f = budgetFrame()
	l.Total = 64 << 10
	f.Goal, f.HandoverBytes = strings.Repeat("x", 23000), 8192
	if _, err := f.Prompt(l); err != nil {
		t.Fatalf("base input should fit: %v", err)
	}
	if err := f.ValidateContext(l); err == nil {
		t.Fatal("accepted input that leaves no working capacity after a legal handover")
	}
}

func TestContextRenderedFieldBounds(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 4, 16, 2048, 4096} {
		for _, text := range []string{"", "plain", "```", strings.Repeat("`", 65536),
			strings.Repeat("\xff", 65536), strings.Repeat("a", 65536), strings.Repeat("\x00", 65536),
			strings.Repeat("`", limit/3) + strings.Repeat("x", 65536)} {
			if actual, bound := len(quote("field", text, limit)), quoteBytes("field", limit); actual > bound {
				t.Fatalf("limit %d: quote %d exceeds %d", limit, actual, bound)
			}
		}
	}
}

func TestContextStepAndTailBounds(t *testing.T) {
	l, f := DefaultPromptLimits(), budgetFrame()
	rng := rand.New(rand.NewSource(1))
	texts := []string{"", strings.Repeat("`", 40000), strings.Repeat("\xff", 40000),
		strings.Repeat("\x00", 40000), strings.Repeat("x", 40000), strings.Repeat("\n", 40000), strings.Repeat("日", 40000)}
	for i := range 300 {
		pick := func() string { return texts[rng.Intn(len(texts))] }
		exit := math.MinInt
		s := StepRecord{Step: math.MaxInt, Command: pick(), Purpose: pick(), Resolved: true, Observation: Observation{
			Execution: core.ExecutionFailedToStart, ExitCode: &exit, Preview: pick(),
			Output: ArtifactRef{Available: true, Bytes: math.MaxInt64, Path: pick(), Detail: pick()},
		}}
		switch i % 4 {
		case 1:
			s.Observation.Retrieval = &archive.Retrieval{Sources: make([]archive.SourceRange, 32),
				NextOffset: math.MaxInt64, NextCursor: strings.Repeat("x", 1024), Error: pick()}
		case 2:
			entries := []string{strings.Repeat("`", archive.MaxBytes-4)}
			encoded, _ := json.Marshal(entries)
			if len(encoded) != archive.MaxBytes {
				t.Fatal("invalid test entry budget")
			}
			s.Observation.Retrieval = &archive.Retrieval{Query: archive.RetrievalQuery{Operation: "list"},
				Entries: entries, NextOffset: math.MaxInt64, More: true}
		case 3:
			s.Observation.Handover = &Handover{}
		}
		if actual, bound := len(f.step(s, l)), f.stepShellBytes(l); actual > bound {
			t.Fatalf("case %d: shell %d exceeds %d", i, actual, bound)
		}
		f.Step, f.Attempt, f.MaxAttempts, f.Earlier = math.MaxInt, math.MaxInt, math.MaxInt, math.MaxInt
		f.MaxSteps = math.MaxInt
		if i%2 == 0 {
			f.MaxSteps = 0
		}
		f.Window = &Window{FirstStep: math.MaxInt, Requested: true}
		f.Rejected = &Rejection{Error: pick(), Response: pick()}
		if actual, bound := len(f.tail(l, f.progress(f.Earlier))), f.tailBytes(l); actual > bound {
			t.Fatalf("case %d: tail %d exceeds %d", i, actual, bound)
		}
		s.Observation = Observation{Execution: core.ExecutionExited, ExitCode: &exit,
			Preview: "Context action failed: " + clip(pick(), 2048)}
		s.Resolved = false
		exit = 1
		if actual, bound := len(f.step(s, l)), f.failedHandoverShellBytes(l); actual > bound {
			t.Fatalf("case %d: failed handover %d exceeds %d", i, actual, bound)
		}
	}
}

func TestContextCapacityIncludesNextStepFailureAndRepair(t *testing.T) {
	l, f := DefaultPromptLimits(), budgetFrame()
	f.Window = &Window{FirstStep: 2, SourceStep: 1, Handover: strings.Repeat("`", f.HandoverBytes)}
	f.Step, f.Earlier = 2, 1
	capacity, err := f.workCapacity(l)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	next := StepRecord{Step: 2, Observation: Observation{Execution: core.ExecutionExited, ExitCode: &exit}}
	used := 0
	for used+len(f.step(next, l)) < capacity {
		used += len(f.step(next, l))
		f.Steps = append(f.Steps, next)
		next.Step++
	}
	next.Command, next.Observation.Preview, next.Purpose = strings.Repeat("x", 40000), strings.Repeat("`", 40000), strings.Repeat("\n", 40000)
	r := &archive.Retrieval{Sources: make([]archive.SourceRange, 32), Evidence: make([]string, 32), NextOffset: math.MaxInt64,
		NextCursor: strings.Repeat("x", 1024), Error: strings.Repeat("\x00", 512)}
	r.Evidence[31] = strings.Repeat("x", evidenceReserve()-len("Retrieved original evidence:\n")-9-len(evidence(r)))
	next.Observation.Retrieval = r
	f.Steps = append(f.Steps, next)
	next.Step++
	exit = 1
	next.Observation = Observation{Execution: core.ExecutionExited, ExitCode: &exit,
		Preview: "Context action failed: " + clip(strings.Repeat("x", 40000), 2048)}
	next.Purpose = strings.Repeat("日", 40000)
	f.Steps = append(f.Steps, next)
	f.Step, f.Attempt = next.Step+1, 2
	f.Window.Requested = true
	f.Rejected = &Rejection{Error: strings.Repeat("x", 40000), Response: strings.Repeat("x", 40000)}
	if _, err := f.Prompt(l); err != nil {
		t.Fatalf("accepted near-capacity page could not render next step, failed handover, and repair: %v", err)
	}
}

func TestFailedHandoverReserveCoversDecoderErrors(t *testing.T) {
	l, f := DefaultPromptLimits(), budgetFrame()
	exit := 1
	for _, backtick := range []string{"`", `\u0060`} {
		for n := 0; n <= 2*actionFailureBytes; n++ {
			payload := `{"` + strings.Repeat(backtick, n) + `":1}`
			var h Handover
			err := decodeAction(payload, &h)
			if err == nil {
				t.Fatal("handover with an unknown field was accepted")
			}
			s := StepRecord{Step: 7, Command: HandoverPrefix + payload, Purpose: strings.Repeat("`", 1<<20), Observation: Observation{Execution: core.ExecutionExited,
				ExitCode: &exit, Preview: actionFailure + clip(err.Error(), actionFailureBytes)}}
			if actual, bound := len(f.step(s, l)), f.failedHandoverShellBytes(l); actual > bound {
				t.Fatalf("%d %s: failed handover %d exceeds reserve %d at a %d-byte preview", n, backtick, actual, bound, l.Preview)
			}
		}
	}
}

func TestProducibleWindowRendersEscapedHandoverFailure(t *testing.T) {
	for _, total := range []int{128 << 10, 256 << 10} {
		l, f := DefaultPromptLimits(), budgetFrame()
		l.Total = total
		f.Window = &Window{FirstStep: 2, SourceStep: 1, Handover: strings.Repeat("`", f.HandoverBytes)}
		f.Step, f.Earlier = 2, 1
		capacity, err := f.workCapacity(l)
		if err != nil {
			t.Fatal(err)
		}
		ok, failed := 0, 1
		next := StepRecord{Step: 2, Command: "true", Observation: Observation{Execution: core.ExecutionExited, ExitCode: &ok}}
		used := 0
		for used+len(f.step(next, l)) < capacity {
			used += len(f.step(next, l))
			f.Steps = append(f.Steps, next)
			next.Step++
		}
		path, sum := "/canti/runs/run-1/steps/00000002/preview", strings.Repeat("a", 64)
		query := archive.RetrievalQuery{Operation: "read", Path: path, Reason: "check earlier evidence", Limit: archive.MaxBytes}
		read := archive.Retrieval{Query: query, Sources: []archive.SourceRange{{Path: path, Length: archive.MaxBytes, SHA256: sum}},
			NextOffset: archive.MaxBytes, More: true}
		command, _ := json.Marshal(query)
		body, _ := json.Marshal(read)
		next.Command = RetrievePrefix + string(command)
		next.Purpose = strings.Repeat("x", 40000)
		next.Observation = Observation{Execution: core.ExecutionExited, ExitCode: &ok, Preview: string(body), Retrieval: &read}
		f.Steps = append(f.Steps, next)
		read.Evidence = []string{strings.Repeat("x", archive.MaxBytes)}
		next.Step++
		payload := `{"` + strings.Repeat(`\u0060`, 1352) + `":1}`
		var h Handover
		decoded := decodeAction(payload, &h)
		if decoded == nil {
			t.Fatal("handover with an unknown field was accepted")
		}
		next.Command, next.Purpose = HandoverPrefix+payload, strings.Repeat("\xff", 40000)
		next.Observation = Observation{Execution: core.ExecutionExited, ExitCode: &failed, Preview: actionFailure + clip(decoded.Error(), actionFailureBytes)}
		f.Steps = append(f.Steps, next)
		f.Step, f.Attempt = next.Step+1, 2
		f.Window.Requested, f.Window.Failures = true, 1
		f.Rejected = &Rejection{Error: strings.Repeat("x", 40000), Response: strings.Repeat("x", 40000)}
		if _, err := f.Prompt(l); err != nil {
			t.Fatalf("%d-byte total: a full window could not render a read, the failed handover, and the retry: %v", total, err)
		}
	}
}

// TestContextFitRequiresTheLargestHandoverWindow requires that Required is
// the smallest total that admits a first page, with and without imports and
// session dialogue, at both handover extremes: at Required the window that
// the largest, all-backtick handover opens at the widest step number keeps
// one byte of capacity, and one byte less is refused although a window that
// an ordinary handover of that length opens at a small step number keeps
// capacity, so the floor is a conservative guarantee, not the least context
// some continuation needs.
func TestContextFitRequiresTheLargestHandoverWindow(t *testing.T) {
	session := budgetFrame()
	session.SessionContext = "Run run-0 (session sequence 1; source /canti/runs/run-0/goal and result; returned):\n" + strings.Repeat("dialogue ", 900)
	for name, f := range map[string]Frame{"default": budgetFrame(), "session": session} {
		for _, handover := range []int{1, 2048, archive.MaxBytes} {
			for _, imports := range []bool{false, true} {
				f.HandoverBytes, f.ImportsEnabled = handover, imports
				l := DefaultPromptLimits()
				fit, err := f.fit(l)
				if err != nil {
					t.Fatal(err)
				}
				largest := f
				largest.Window = &Window{FirstStep: math.MaxInt, Handover: strings.Repeat("`", handover)}
				ordinary := f
				ordinary.Step, ordinary.Window = 3, &Window{FirstStep: 3, SourceStep: 2, Handover: strings.Repeat("a", handover)}
				l.Total = fit.Required()
				capacity, err := largest.workCapacity(l)
				if err != nil || capacity != 1 || f.ValidateContext(l) != nil {
					t.Errorf("%s, %d-byte handover, imports %v: at the %d bytes required the largest window keeps %d bytes: %v, %v",
						name, handover, imports, l.Total, capacity, err, f.ValidateContext(l))
				}
				l.Total--
				want := "at least " + strconv.Itoa(fit.Required()) + " bytes"
				if _, err := largest.workCapacity(l); err == nil || f.ValidateContext(l) == nil || !strings.Contains(f.ValidateContext(l).Error(), want) {
					t.Errorf("%s, %d-byte handover, imports %v: %d bytes are admitted: %v", name, handover, imports, l.Total, f.ValidateContext(l))
				}
				if capacity, err := ordinary.workCapacity(l); err != nil || capacity <= 0 {
					t.Errorf("%s, %d-byte handover, imports %v: an ordinary handover leaves %d bytes at %d: %v", name, handover, imports, capacity, l.Total, err)
				}
			}
		}
	}
}

func TestPurposeReserveIsTheWidestPurposeLine(t *testing.T) {
	f, exit := budgetFrame(), 0
	bare := StepRecord{Step: 7, Command: "true", Observation: Observation{Execution: core.ExecutionExited, ExitCode: &exit}}
	marker := regexp.MustCompile(`^Purpose: .*…\[truncated (\d+) bytes\]$`)
	widestCount := len(strconv.Itoa(math.MaxInt))
	for _, limit := range []int{3, 4, 512, 4096} {
		l := DefaultPromptLimits()
		l.Purpose = limit
		at := strings.Repeat("p", limit)
		widest := 0
		for _, purpose := range []string{"p", at, at + "q", strings.Repeat("\xff", limit+1), at[1:] + "日", at[2:] + "日",
			strings.Repeat("日", 1<<20), "\n" + at, strings.Repeat("\n", 1<<20), strings.Repeat("\r\n", 1<<19),
			strings.Repeat("`", 1<<20), strings.Repeat("\xff", 1<<20)} {
			s := bare
			s.Purpose = purpose
			step := f.step(s, l)
			growth := len(step) - len(f.step(bare, l))
			if growth > purposeBytes(l) {
				t.Fatalf("limit %d, purpose of %d bytes: line of %d bytes exceeds the reserve %d", limit, len(purpose), growth, purposeBytes(l))
			}
			line, _, _ := strings.Cut(strings.TrimPrefix(step, "### Step 7\n"), "\n")
			if m := marker.FindStringSubmatch(line); m != nil {
				growth += widestCount - len(m[1])
			}
			widest = max(widest, growth)
		}
		if widest != purposeBytes(l) {
			t.Fatalf("limit %d: widest purpose line, counting %d-digit truncations, is %d bytes; reserve %d", limit, widestCount, widest, purposeBytes(l))
		}
	}
}

func TestContextReservesCoverPurposeAtTheLimit(t *testing.T) {
	l, f := DefaultPromptLimits(), budgetFrame()
	at := strings.Repeat("p", l.Purpose)
	exit := math.MinInt
	for _, purpose := range []string{at, at + "q", at[1:] + "日", at[2:] + "日", "\n" + at, at + strings.Repeat("\n", 1<<20),
		strings.Repeat("\xff", l.Purpose+1), strings.Repeat("日", 1<<20)} {
		s := StepRecord{Step: math.MaxInt, Command: strings.Repeat("x", 40000), Purpose: purpose, Resolved: true, Observation: Observation{
			Execution: core.ExecutionFailedToStart, ExitCode: &exit, Preview: strings.Repeat("x", 40000),
			Retrieval: &archive.Retrieval{Sources: make([]archive.SourceRange, 32), NextOffset: math.MaxInt64, NextCursor: strings.Repeat("x", 1024),
				Error: strings.Repeat("\x00", 40000), Incomplete: true, SkippedCount: 3 * archive.MaxEntries, Skipped: []archive.Skip{
					{Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"}, {Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"}}},
		}}
		if actual, bound := len(f.step(s, l)), f.stepShellBytes(l); actual > bound {
			t.Fatalf("purpose of %d bytes: step %d exceeds reserve %d", len(purpose), actual, bound)
		}
	}
}

func TestDiscoveryCoverageFitsStepReserve(t *testing.T) {
	frame, limits := budgetFrame(), DefaultPromptLimits()
	exit := math.MinInt
	step := StepRecord{Step: math.MaxInt, Command: strings.Repeat("`", 40000), Resolved: true,
		Observation: Observation{Execution: "failed_to_start", ExitCode: &exit, Preview: strings.Repeat("`", 40000),
			Retrieval: &archive.Retrieval{Query: archive.RetrievalQuery{Operation: "discover", Path: "/canti/runs/run/steps", Query: "needle", Reason: "recover"}, Sources: make([]archive.SourceRange, 32), Incomplete: true,
				NextOffset: math.MaxInt64, NextCursor: strings.Repeat("x", 1024), Error: strings.Repeat("\x00", 512),
				SkippedCount: 3 * archive.MaxEntries, Skipped: []archive.Skip{{Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"}, {Step: archive.MaxEntries, Field: "artifact", Reason: "utf8"}}}}}
	if actual, bound := len(frame.step(step, limits)), frame.stepShellBytes(limits); actual > bound {
		t.Fatalf("coverage shell %d exceeds %d", actual, bound)
	}
}

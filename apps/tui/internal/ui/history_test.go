package ui

import (
	"errors"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestReopenedLineShowsEarlierRunsBeforeThisSession(t *testing.T) {
	exit := 0
	m := model()
	latest := hostrpc.HistoryRun{RunID: "run-3", Status: hostrpc.StatusActive, Phase: "idle", EffectID: "effect-3", Driver: hostrpc.DriverIdle}
	m.seedHistory(chat.History{Cursor: "pos:12", Latest: &latest, Runs: []hostrpc.HistoryRun{
		{RunID: "run-1", Status: hostrpc.StatusReturned, Result: "first answer"},
		{RunID: "run-2", Status: hostrpc.StatusReturned, Result: "second answer"},
		latest,
	}, Items: []hostrpc.Item{
		{Pos: 1, RunID: "run-1", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "first goal"}},
		{Pos: 2, RunID: "run-1", Kind: "decided", Purpose: "inspect the build\nbefore changing it", Command: "go test ./..."},
		{Pos: 3, RunID: "run-1", Kind: "completed", Command: "go test ./...", Execution: "exited", ExitCode: &exit, Preview: "build ok"},
		{Pos: 4, RunID: "run-1", Kind: "returned", Result: "first answer"},
		{Pos: 5, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "second goal"}},
		{Pos: 6, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "steer", Disposition: "steer", Text: "try the tests first"}},
		{Pos: 7, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "follow_up", Disposition: "queued", Text: "and then the docs"}},
		{Pos: 8, RunID: "run-2", Kind: "returned", Result: "second answer"},
		{Pos: 9, RunID: "run-3", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "third goal"}},
		{Pos: 10, RunID: "run-3", Kind: "needs_resolution", Failure: "the outcome of echo is unknown"},
	}})
	got := m.transcript()
	for _, want := range []string{"── earlier history ──", "› first goal", "# inspect the build\n# before changing it", "$ go test ./...", "│ build ok", "└ exit 0", "first answer",
		"── earlier run ──", "› second goal", "› try the tests first", "» and then the docs", "second answer",
		"› third goal", "canti: run run-3 needs resolution for effect effect-3: the outcome of echo is unknown", "── this session ──"} {
		if !strings.Contains(got, want) {
			t.Fatalf("reopened transcript lacks %q:\n%s", want, got)
		}
	}
	if n := strings.Count(got, "first answer"); n != 1 {
		t.Fatalf("first answer appears %d times:\n%s", n, got)
	}
	order := []string{"first goal", "first answer", "second goal", "second answer", "third goal", "── this session ──"}
	at := -1
	for _, want := range order {
		next := strings.Index(got, want)
		if next <= at {
			t.Fatalf("%q is not after the previous run:\n%s", want, got)
		}
		at = next
	}
	if !m.shownTerminal["run-1"] || !m.shownTerminal["run-2"] || m.shownTerminal["run-3"] {
		t.Fatalf("terminal bookkeeping = %v", m.shownTerminal)
	}
}

func TestReopenedRunsShowSummaryOutcomesWhenRowsLackThem(t *testing.T) {
	exit := 0
	m := model()
	m.seedHistory(chat.History{Cursor: "pos:8", Runs: []hostrpc.HistoryRun{
		{RunID: "run-1", Status: hostrpc.StatusStopped, Phase: "stopped", StopReason: "steps_exhausted"},
		{RunID: "run-2", Status: hostrpc.StatusReturned, Phase: "returned", Result: "summary answer", Truncated: true},
		{RunID: "run-3", Status: hostrpc.StatusStopped, Phase: "stopped", StopReason: "budget_exhausted"},
	}, Items: []hostrpc.Item{
		{Pos: 1, RunID: "run-1", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "first goal"}},
		{Pos: 2, RunID: "run-1", Kind: "completed", Command: "true", Execution: "exited", ExitCode: &exit},
		{Pos: 3, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "second goal"}},
		{Pos: 4, RunID: "run-2", Kind: "completed", Command: "true", Execution: "exited", ExitCode: &exit},
		{Pos: 5, RunID: "run-3", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "third goal"}},
		{Pos: 6, RunID: "run-3", Kind: "needs_resolution", Failure: "an effect is unknown"},
	}})
	got := m.transcript()
	for _, want := range []string{"canti: run run-1 stopped: steps_exhausted", "summary answer", `… answer truncated; use "canti history" for the full text`, "canti: run run-3 stopped: budget_exhausted"} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary outcome lacks %q:\n%s", want, got)
		}
	}
	if !m.shownTerminal["run-1"] || !m.shownTerminal["run-2"] || !m.shownTerminal["run-3"] {
		t.Fatalf("terminal bookkeeping = %v", m.shownTerminal)
	}
}

func TestReopenedLineRendersWholeRunsWithinBudget(t *testing.T) {
	m := model()
	runs := []hostrpc.HistoryRun{
		{RunID: "run-1", Status: hostrpc.StatusReturned},
		{RunID: "run-2", Status: hostrpc.StatusReturned},
	}
	items := []hostrpc.Item{
		{Pos: 1, RunID: "run-1", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "run-1 goal"}},
	}
	for i := 0; i < 300; i++ {
		items = append(items, hostrpc.Item{Pos: int64(2 + i), RunID: "run-1", Kind: "completed", Execution: "exited"})
	}
	items = append(items,
		hostrpc.Item{Pos: 400, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "run-2 goal"}},
		hostrpc.Item{Pos: 401, RunID: "run-2", Kind: "returned", Result: "run-2 answer"})
	m.seedHistory(chat.History{Cursor: "pos:401", Runs: runs, Items: items})
	got := m.transcript()
	if strings.Contains(got, "run-1 goal") || strings.Contains(got, "── earlier run ──\n\n── earlier run") {
		t.Fatalf("a rendered fragment of run-1 survived:\n%s", got)
	}
	if !strings.Contains(got, "run-2 goal") || !strings.Contains(got, "run-2 answer") {
		t.Fatalf("the newest whole run was dropped:\n%s", got)
	}
	if !strings.Contains(got, "… earlier transcript omitted") || !strings.Contains(got, "canti history") {
		t.Fatalf("rendered omission notice missing:\n%s", got)
	}
	if len(m.entries) > transcriptEntries || m.bytes > transcriptBytes {
		t.Fatalf("rendered transcript = %d entries, %d bytes", len(m.entries), m.bytes)
	}
}

func TestReopenedLineWithNoFittingRunShowsOmissionAndIdleGuidance(t *testing.T) {
	m := model()
	active := hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Phase: "idle", Driver: hostrpc.DriverIdle}
	items := []hostrpc.Item{{Pos: 1, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "huge goal"}}}
	for i := 0; i < 300; i++ {
		items = append(items, hostrpc.Item{Pos: int64(2 + i), RunID: "run", Kind: "completed", Execution: "exited"})
	}
	m.seedHistory(chat.History{Loaded: true, Latest: &active, Runs: []hostrpc.HistoryRun{active}, Cursor: "pos:301", Items: items})
	got := m.transcript()
	if strings.Contains(got, "huge goal") || strings.Contains(got, "── earlier run ──") {
		t.Fatalf("a run fragment was rendered when no run fits:\n%s", got)
	}
	if !strings.Contains(got, "… earlier transcript omitted") || !strings.Contains(got, "canti history") {
		t.Fatalf("omission notice missing:\n%s", got)
	}
	if !strings.Contains(got, `canti: run run is idle; continue it with "canti resume -run run"`) {
		t.Fatalf("idle guidance missing:\n%s", got)
	}
}

func TestReopenedQueuedFollowUpRendersAfterItsRun(t *testing.T) {
	m := model()
	active := hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Phase: "idle", Driver: hostrpc.DriverIdle}
	m.seedHistory(chat.History{Cursor: "pos:3", Latest: &active, Runs: []hostrpc.HistoryRun{active}, Items: []hostrpc.Item{
		{Pos: 1, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "working goal"}},
		{Pos: 2, RunID: "run", Kind: "completed", Execution: "exited"},
		{Pos: 3, Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "follow_up", Disposition: "queued", Text: "queued next"}},
	}})
	got := m.transcript()
	if !strings.Contains(got, "» queued next") {
		t.Fatalf("queued follow-up missing:\n%s", got)
	}
	if strings.Index(got, "» queued next") < strings.Index(got, "└ exited") {
		t.Fatalf("queued follow-up rendered before the run's step:\n%s", got)
	}
}

func TestReopenedHeldAndWithdrawnInputsShowTheirState(t *testing.T) {
	m := model()
	stopped := hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusStopped, StopReason: "steps_exhausted"}
	m.seedHistory(chat.History{Cursor: "pos:6", Latest: &stopped, Runs: []hostrpc.HistoryRun{stopped}, Items: []hostrpc.Item{
		{Pos: 1, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "goal"}},
		{Pos: 2, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "steer", Disposition: "steer", Text: "held steer"}},
		{Pos: 21, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "steer", Disposition: "steer", Text: "seen steer", Seen: true}},
		{Pos: 3, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "follow_up", Disposition: "queued", Text: "held follow-up"}},
		{Pos: 4, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "steer", Disposition: "withdrawn", Text: "withdrawn steer"}},
		{Pos: 5, Kind: "withdrawn", Withdrawn: []string{"input-1"}},
	}})
	got := m.transcript()
	for _, want := range []string{"› held steer (held)", "» held follow-up (held)", "› seen steer\n", "› withdrawn steer (withdrawn)", "↩ withdrew 1 input(s): input-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("held/withdrawn history lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "seen steer (held)") {
		t.Fatalf("a seen steer was marked held:\n%s", got)
	}
}

func TestRestoredRunBlocksEvictWholeDuringLiveActivity(t *testing.T) {
	m := model()
	runs := []hostrpc.HistoryRun{
		{RunID: "run-1", Status: hostrpc.StatusReturned},
		{RunID: "run-2", Status: hostrpc.StatusReturned},
	}
	m.seedHistory(chat.History{Cursor: "pos:4", Runs: runs, Items: []hostrpc.Item{
		{Pos: 1, RunID: "run-1", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "run-1 goal"}},
		{Pos: 2, RunID: "run-1", Kind: "returned", Result: "run-1 answer"},
		{Pos: 3, RunID: "run-2", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "run-2 goal"}},
		{Pos: 4, RunID: "run-2", Kind: "returned", Result: "run-2 answer"},
	}})
	for range 254 {
		m.add("live activity\n")
	}
	got := m.transcript()
	for _, run := range []string{"run-1", "run-2"} {
		goal := strings.Contains(got, run+" goal")
		answer := strings.Contains(got, run+" answer")
		if goal != answer {
			t.Fatalf("%s was fragmented: goal %v, answer %v:\n%s", run, goal, answer, got)
		}
	}
	if len(m.entries) != len(m.owners) || len(m.entries) > transcriptEntries || m.bytes > transcriptBytes {
		t.Fatalf("transcript = %d entries, %d owners, %d bytes", len(m.entries), len(m.owners), m.bytes)
	}
}

func TestReopenedLineOmissionNamesCantiHistory(t *testing.T) {
	m := model()
	m.seedHistory(chat.History{Omitted: true, Items: []hostrpc.Item{
		{Pos: 2, RunID: "run", Kind: "input", Input: &hostrpc.Input{Authority: "principal", Disposition: "opened", Text: "kept goal"}},
	}})
	got := m.transcript()
	if !strings.Contains(got, "… earlier transcript omitted") || !strings.Contains(got, `use "canti history" for a run's full recorded transitions`) {
		t.Fatalf("omission notice = %q", got)
	}
	if !strings.Contains(got, "kept goal") || !strings.Contains(got, "── this session ──") {
		t.Fatalf("omitted history lost its retained tail: %q", got)
	}
	for range 300 {
		m.add(strings.Repeat("x", 200) + "\n")
	}
	if len(m.entries) > transcriptEntries || m.bytes > transcriptBytes || !m.omitted {
		t.Fatalf("transcript = %d entries, %d bytes, omitted %v", len(m.entries), m.bytes, m.omitted)
	}
}

func TestHistoryLoadFailureLeavesTheEditorReady(t *testing.T) {
	m := model()
	m.seedHistory(chat.History{Err: errors.New("host went away"), Latest: &hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Driver: hostrpc.DriverDriving}, Cursor: "pos:7"})
	got := m.transcript()
	if !strings.Contains(got, "canti: earlier history unavailable: host went away") {
		t.Fatalf("history failure is not diagnostic: %q", got)
	}
	if strings.Contains(got, "earlier history ──") || m.phase != ready {
		t.Fatalf("failed history rendered or blocked the editor: %q, phase %d", got, m.phase)
	}
	if _, _, ok := attachPoint(m.history); ok {
		t.Fatal("a failed history load attached")
	}
	m.editor.SetValue("still editable")
	m = update(m, key("!"))
	if m.editor.Value() != "still editable!" {
		t.Fatalf("editor after a failed history load = %q", m.editor.Value())
	}
}

func TestActiveRunAttachmentUsesTheHistoryCursor(t *testing.T) {
	active := &hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Driver: hostrpc.DriverDriving}
	run, cursor, ok := attachPoint(chat.History{Latest: active, Cursor: "pos:7"})
	if !ok || run.RunID != "run" || cursor != "pos:7" {
		t.Fatalf("attach point = %+v, %q, %v; want the history cursor pos:7", run, cursor, ok)
	}
	if _, cursor, ok = attachPoint(chat.History{Latest: active}); !ok || cursor != "" {
		t.Fatal("an active run without a history cursor is not attached from the start")
	}
	idle := *active
	idle.Driver = hostrpc.DriverIdle
	if _, _, ok = attachPoint(chat.History{Latest: &idle, Cursor: "pos:7"}); ok {
		t.Fatal("an idle active run was attached")
	}
	done := *active
	done.Status = hostrpc.StatusReturned
	if _, _, ok = attachPoint(chat.History{Latest: &done, Cursor: "pos:7"}); ok {
		t.Fatal("a returned run was attached")
	}
	if _, _, ok = attachPoint(chat.History{}); ok {
		t.Fatal("an empty state directory was attached")
	}
	if got := activeNotice(chat.History{Latest: &idle}); !strings.Contains(got, `canti resume -run run`) {
		t.Fatalf("idle active notice = %q", got)
	}
	if got := activeNotice(chat.History{Latest: active}); got != "" {
		t.Fatalf("driving active notice = %q", got)
	}
}

func TestStartupRecheckFollowsOnlyAChangedOrDrivenRun(t *testing.T) {
	active := hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}
	driving := active
	driving.Driver = hostrpc.DriverDriving
	closed := active
	closed.Status = hostrpc.StatusReturned
	newRun := hostrpc.HistoryRun{RunID: "new", Status: hostrpc.StatusActive, Driver: hostrpc.DriverDriving}
	for _, tc := range []struct {
		name string
		h    chat.History
		next *hostrpc.HistoryRun
		want bool
	}{
		{"active became driven", chat.History{Latest: &active}, &driving, true},
		{"active stayed idle", chat.History{Latest: &active}, &active, false},
		{"active closed", chat.History{Latest: &active}, &closed, true},
		{"closed stayed closed", chat.History{Latest: &closed}, &closed, false},
		{"empty gained a driven run", chat.History{}, &driving, true},
		{"empty gained an idle run", chat.History{}, &active, false},
		{"empty gained a closed run", chat.History{}, &closed, true},
		{"closed then a new driven run", chat.History{Latest: &closed}, &newRun, true},
	} {
		if got := needsFollow(tc.h, tc.next); got != tc.want {
			t.Errorf("%s: needsFollow = %v, want %v", tc.name, got, tc.want)
		}
	}
	if needsFollow(chat.History{Latest: &active}, nil) {
		t.Fatal("an absent latest run was followed")
	}
}

func TestStartupRecheckMarksANewFreshLine(t *testing.T) {
	if !newFreshLine(chat.History{LineStart: 1}, &hostrpc.HistoryRun{Line: 2}) {
		t.Fatal("a recorded fresh line after the snapshot was not marked")
	}
	if newFreshLine(chat.History{LineStart: 1}, &hostrpc.HistoryRun{Line: 1}) {
		t.Fatal("the snapshot's own line was marked as a new line")
	}
	if !newFreshLine(chat.History{}, &hostrpc.HistoryRun{Line: 1}) {
		t.Fatal("an empty snapshot did not mark a following line")
	}
	if newFreshLine(chat.History{}, &hostrpc.HistoryRun{Line: 0}) {
		t.Fatal("an absent line was marked")
	}
	if !newFreshLine(chat.History{LineStart: 1}, &hostrpc.HistoryRun{Line: 3}) {
		t.Fatal("a later non-fresh successor on a new line was not marked")
	}
}

func TestFollowActiveStartsInTheFollowedRun(t *testing.T) {
	m := model()
	m.history = chat.History{Loaded: true, Latest: &hostrpc.HistoryRun{RunID: "run", Status: hostrpc.StatusActive, Driver: hostrpc.DriverDriving}, Cursor: "pos:7"}
	m.followActive()
	if m.turn == nil || m.phase != running || m.attachCmd == nil {
		t.Fatalf("followActive = turn %v, phase %d, command %v", m.turn != nil, m.phase, m.attachCmd != nil)
	}
	if m.turn.RunID() != "run" {
		t.Fatalf("followed turn run = %q", m.turn.RunID())
	}
	if cmd := m.Init(); cmd == nil {
		t.Fatal("Init did not start the followed run")
	}
}

func TestAttachFailureLeavesTheEditorReady(t *testing.T) {
	m := model()
	next, cmd := m.complete(chat.Result{AttachFailed: true, Err: errors.New("host went away")})
	m = next.(Model)
	if cmd != nil || m.phase != ready || m.err != nil || m.code != 0 {
		t.Fatalf("attach failure = phase %d, err %v, code %d, cmd %v", m.phase, m.err, m.code, cmd != nil)
	}
	if !strings.Contains(m.transcript(), "following the active run stopped: host went away") {
		t.Fatalf("attach failure diagnostic = %q", m.transcript())
	}
}

func TestLivePrincipalInputsRenderOnceAndRemoteSteersShow(t *testing.T) {
	m := model()
	m.localInputs["local"] = true
	m = update(m, fact{event: chat.Event{Item: &hostrpc.Item{Pos: 1, RunID: "run", Kind: "input", Input: &hostrpc.Input{
		InputID: "local", Authority: "principal", Text: "local echo"}}}})
	if strings.Contains(m.transcript(), "local echo") {
		t.Fatalf("local echo rendered again: %q", m.transcript())
	}
	m = update(m, fact{event: chat.Event{Item: &hostrpc.Item{Pos: 2, RunID: "run", Kind: "input", Input: &hostrpc.Input{
		InputID: "remote", Authority: "principal", Delivery: "steer", Text: "remote steer"}}}})
	if !strings.Contains(m.transcript(), "› remote steer") {
		t.Fatalf("remote steer is not shown: %q", m.transcript())
	}
	m = update(m, fact{event: chat.Event{Item: &hostrpc.Item{Pos: 3, RunID: "run", Kind: "input", Input: &hostrpc.Input{
		Authority: "observation", Text: "evidence"}}}})
	if strings.Contains(m.transcript(), "evidence") {
		t.Fatalf("observation rendered as a principal input: %q", m.transcript())
	}
}

package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func cursorOf(pos int) string { return fmt.Sprintf("pos:%d", pos) }

func goal(run string, pos int64) hostrpc.Item {
	return hostrpc.Item{Pos: pos, RunID: run, Kind: "input", Input: &hostrpc.Input{
		Authority: "principal", Disposition: "opened", Text: "goal of " + run}}
}

func answer(run string, pos int64) hostrpc.Item {
	return hostrpc.Item{Pos: pos, RunID: run, Kind: "returned", Result: "answer of " + run}
}

func summary(run string, ordinal int) hostrpc.HistoryRun {
	return hostrpc.HistoryRun{RunID: run, Ordinal: ordinal, Status: hostrpc.StatusReturned, Phase: "returned", Result: "answer of " + run}
}

func TestHistoryCollectorKeepsWholeRunsWithinTheTranscriptBound(t *testing.T) {
	total := 20
	var items []hostrpc.Item
	var runs []hostrpc.HistoryRun
	for i := total; i >= 1; i-- {
		run := fmt.Sprintf("run-%02d", i)
		items = append(items, goal(run, int64(2*i-1)), answer(run, int64(2*i)))
		runs = append(runs, summary(run, i))
	}
	// The server returns items in agent-log order and newest runs first.
	slices.Reverse(items)
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs[:historyRuns], Items: items, Cursor: cursorOf(2 * total)}, nil
	})
	if h.Err != nil || !h.Omitted || len(h.Runs) != historyRuns || len(h.Items) != 2*historyRuns {
		t.Fatalf("history = %d runs, %d items, omitted %v, err %v", len(h.Runs), len(h.Items), h.Omitted, h.Err)
	}
	if h.Runs[0].RunID != "run-13" || h.Runs[len(h.Runs)-1].RunID != "run-20" {
		t.Fatalf("retained runs = %v, want run-13..run-20", h.Runs)
	}
	if h.Items[0].Pos != 25 || h.Items[len(h.Items)-1].Pos != 40 || h.Items[0].RunID != "run-13" {
		t.Fatalf("retained items start %+v and end %+v, want the whole newest %d runs", h.Items[0], h.Items[len(h.Items)-1], historyRuns)
	}
	if h.Cursor != cursorOf(2*total) || h.Latest == nil || h.Latest.RunID != "run-20" {
		t.Fatalf("cursor/latest = %q, %+v", h.Cursor, h.Latest)
	}
}

func TestHistoryCollectorDropsTheOldestPartialRun(t *testing.T) {
	var items []hostrpc.Item
	var runs []hostrpc.HistoryRun
	// run-01 lost its goal; run-02..run-08 are whole.
	items = append(items, answer("run-01", 2))
	for i := 2; i <= 8; i++ {
		run := fmt.Sprintf("run-%02d", i)
		items = append(items, goal(run, int64(2*i-1)), answer(run, int64(2*i)))
	}
	for i := 8; i >= 1; i-- {
		runs = append(runs, summary(fmt.Sprintf("run-%02d", i), i))
	}
	slices.Reverse(items)
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Items: items, Cursor: "pos:16"}, nil
	})
	if h.Err != nil || !h.Omitted || len(h.Runs) != 7 || len(h.Items) != 14 {
		t.Fatalf("history = %d runs, %d items, omitted %v, err %v; want seven whole runs", len(h.Runs), len(h.Items), h.Omitted, h.Err)
	}
	if h.Items[0].RunID != "run-02" || h.Items[0].Kind != "input" || h.Items[len(h.Items)-1].RunID != "run-08" {
		t.Fatalf("retained items start %+v and end %+v; want run-02 goal through run-08 answer", h.Items[0], h.Items[len(h.Items)-1])
	}
	for _, it := range h.Items {
		if it.RunID == "run-01" {
			t.Fatalf("a fragment of the partial run was retained: %+v", it)
		}
	}
}

func TestHistoryCollectorGroupsAQueuedFollowUpAfterItsPredecessor(t *testing.T) {
	runs := []hostrpc.HistoryRun{summary("run-02", 2), summary("run-01", 1)}
	// run-02's goal was admitted while run-01 was still working, so its log
	// position precedes run-01's answer.
	items := []hostrpc.Item{goal("run-01", 1), goal("run-02", 2), answer("run-01", 3), answer("run-02", 4)}
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Items: items, Cursor: "pos:4"}, nil
	})
	if h.Err != nil || len(h.Items) != 4 {
		t.Fatalf("history = %+v", h)
	}
	want := []string{"run-01", "run-01", "run-02", "run-02"}
	for i, it := range h.Items {
		if it.RunID != want[i] {
			t.Fatalf("item %d belongs to %s, want %s: %+v", i, it.RunID, want[i], h.Items)
		}
	}
	if h.Items[2].Kind != "input" || h.Items[3].Kind != "returned" {
		t.Fatalf("follow-up group = %+v, want its goal then answer", h.Items[2:])
	}
}

func TestHistoryCollectorKeepsAQueuedFollowUpAtTheEnd(t *testing.T) {
	runs := []hostrpc.HistoryRun{{RunID: "run-01", Ordinal: 1, Status: hostrpc.StatusActive, Phase: "idle"}}
	items := []hostrpc.Item{
		goal("run-01", 1),
		{RunID: "run-01", Pos: 2, Kind: "completed", Command: "true"},
		{Pos: 3, Kind: "input", Input: &hostrpc.Input{Authority: "principal", Delivery: "follow_up", Disposition: "queued", Text: "queued next"}},
	}
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Items: items, Cursor: "pos:3"}, nil
	})
	if h.Err != nil || len(h.Items) != 3 {
		t.Fatalf("history = %+v", h)
	}
	last := h.Items[2]
	if last.RunID != "" || last.Kind != "input" || last.Input == nil || last.Input.Text != "queued next" {
		t.Fatalf("queued follow-up = %+v, want it after the run's items", last)
	}
}

func TestHistoryCollectorKeepsALateRunsGoalAcrossPages(t *testing.T) {
	firstRuns := []hostrpc.HistoryRun{{RunID: "run-a", Ordinal: 1, Status: hostrpc.StatusReturned}}
	lateRuns := []hostrpc.HistoryRun{{RunID: "run-b", Ordinal: 2, Status: hostrpc.StatusReturned, Result: "b answer"}}
	pages := 0
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		pages++
		if pages == 1 {
			return hostrpc.HistoryPage{Latest: &firstRuns[0], Runs: firstRuns, Cursor: "pos:2", Older: `{"pos":2}`,
				Items: []hostrpc.Item{goal("run-a", 1), answer("run-a", 2)}}, nil
		}
		return hostrpc.HistoryPage{Runs: lateRuns, Cursor: "pos:4", Omitted: true, Items: []hostrpc.Item{goal("run-b", 3), answer("run-b", 4)}}, nil
	})
	if h.Err != nil || len(h.Items) != 4 || h.Items[2].RunID != "run-b" || h.Items[2].Kind != "input" || h.Items[3].Kind != "returned" {
		t.Fatalf("late run history = %+v", h)
	}
	if len(h.Runs) != 2 || h.Runs[1].RunID != "run-b" {
		t.Fatalf("late run summaries = %+v", h.Runs)
	}
	if !h.Omitted {
		t.Fatal("a later page's omission was dropped")
	}
}

func TestHistoryCollectorReportsOlderPagesAsOmitted(t *testing.T) {
	var items []hostrpc.Item
	var runs []hostrpc.HistoryRun
	for i := 1; i <= historyRuns; i++ {
		run := fmt.Sprintf("run-%02d", i)
		items = append(items, goal(run, int64(2*i-1)), answer(run, int64(2*i)))
		runs = append([]hostrpc.HistoryRun{summary(run, i)}, runs...)
	}
	slices.Reverse(items)
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Items: items, Older: "pos:1", Cursor: "pos:16"}, nil
	})
	if h.Err != nil || !h.Omitted || len(h.Runs) != historyRuns || len(h.Items) != 2*historyRuns {
		t.Fatalf("history = %d runs, %d items, omitted %v, err %v", len(h.Runs), len(h.Items), h.Omitted, h.Err)
	}
}

func TestHistoryCollectorStopsAfterThePageBound(t *testing.T) {
	var runs []hostrpc.HistoryRun
	for i := historyPages; i >= 1; i-- {
		runs = append(runs, summary(fmt.Sprintf("run-%02d", i), i))
	}
	pages := 0
	h := collectHistory(context.Background(), func(_ context.Context, before, _ string, _ int) (hostrpc.HistoryPage, error) {
		pages++
		i := historyPages - pages + 1
		run := fmt.Sprintf("run-%02d", i)
		page := hostrpc.HistoryPage{Items: []hostrpc.Item{goal(run, int64(2*i-1)), answer(run, int64(2*i))},
			Older: "pos:1", Cursor: "pos:16"}
		if pages == 1 {
			page.Latest, page.Runs = &runs[0], runs
		}
		return page, nil
	})
	if h.Err != nil || pages != historyPages || !h.Omitted || len(h.Runs) != historyRuns {
		t.Fatalf("history = %d pages, %d runs, omitted %v, err %v", pages, len(h.Runs), h.Omitted, h.Err)
	}
}

func TestHistoryCollectorReusesTheFirstPagesLineToken(t *testing.T) {
	seen := []string{}
	runs := []hostrpc.HistoryRun{{RunID: "run-01", Ordinal: 1, Status: hostrpc.StatusActive, Phase: "idle"}}
	h := collectHistory(context.Background(), func(_ context.Context, _, line string, _ int) (hostrpc.HistoryPage, error) {
		seen = append(seen, line)
		if len(seen) == 1 {
			return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Line: "line:1:1", Older: "pos:2", Cursor: "pos:3",
				Items: []hostrpc.Item{goal("run-01", 1), answer("run-01", 2)}}, nil
		}
		return hostrpc.HistoryPage{Line: "line:1:1", Cursor: "pos:3", Items: []hostrpc.Item{answer("run-01", 2)}}, nil
	})
	if h.Err != nil || h.Line != "line:1:1" || len(seen) != 2 || seen[0] != "" || seen[1] != "line:1:1" {
		t.Fatalf("line tokens = %v, history line %q, err %v", seen, h.Line, h.Err)
	}
}

func TestHistoryCollectorKeepsWhatItReadWhenAPageFails(t *testing.T) {
	failure := errors.New("host went away")
	runs := []hostrpc.HistoryRun{summary("run-01", 1)}
	pages := 0
	h := collectHistory(context.Background(), func(context.Context, string, string, int) (hostrpc.HistoryPage, error) {
		pages++
		if pages == 2 {
			return hostrpc.HistoryPage{}, failure
		}
		return hostrpc.HistoryPage{Latest: &runs[0], Runs: runs, Items: []hostrpc.Item{goal("run-01", 1), answer("run-01", 2)},
			Older: "pos:1", Cursor: "pos:2"}, nil
	})
	if !errors.Is(h.Err, failure) || len(h.Items) != 2 || len(h.Runs) != 1 || !h.Omitted {
		t.Fatalf("history after a failed page = %+v", h)
	}
}

func TestStartupSkipsTheRecheckAfterAHistoryPageFails(t *testing.T) {
	state := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var ops []string
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				var req hostrpc.Request
				if json.NewDecoder(conn).Decode(&req) != nil {
					return
				}
				mu.Lock()
				ops = append(ops, req.Op)
				mu.Unlock()
				resp := hostrpc.Response{Protocol: hostrpc.Protocol}
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpHistory:
					if req.History != nil && req.History.LatestOnly {
						resp.History = &hostrpc.HistoryPage{Latest: &hostrpc.HistoryRun{RunID: "late"}}
					} else {
						resp.Error = &hostrpc.Error{Code: hostrpc.CodeFailed, Message: "refused"}
					}
				}
				_ = json.NewEncoder(conn).Encode(resp)
			}()
		}
	}()
	c := New(Config{HostMode: Auto, HostWait: time.Second, Workspace: t.TempDir(), State: state})
	c.loadStartup(context.Background())
	if c.history.Err == nil {
		t.Fatal("history failure was lost")
	}
	if c.latest != nil {
		t.Fatalf("a recheck ran after history failed: %+v", c.latest)
	}
	mu.Lock()
	got := slices.Clone(ops)
	mu.Unlock()
	for _, op := range got {
		if op == hostrpc.OpHistory && len(got) == 2 {
			// The only History request is the failed page; no LatestOnly retry
			// is distinguishable here, so the absence of a Latest is checked
			// by the fake's successful answer above.
			return
		}
	}
	if c.latest != nil {
		t.Fatal("a latest-only recheck was sent after a failed history page")
	}
}

func TestLatestHistoryHonorsTheStartupDeadline(t *testing.T) {
	state := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var req hostrpc.Request
				if json.NewDecoder(conn).Decode(&req) != nil {
					return
				}
				resp := hostrpc.Response{Protocol: hostrpc.Protocol}
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpHistory:
					<-release
					return
				default:
					return
				}
				_ = json.NewEncoder(conn).Encode(resp)
			}()
		}
	}()
	c := New(Config{HostMode: Auto, HostWait: time.Second, Workspace: t.TempDir(), State: state})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.loadLatest(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("loadLatest error = %v, want the startup deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("loadLatest took %v beyond its deadline", elapsed)
	}
}

func TestRefreshLatestUsesTheStartupDeadline(t *testing.T) {
	state := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var req hostrpc.Request
				if json.NewDecoder(conn).Decode(&req) != nil {
					return
				}
				resp := hostrpc.Response{Protocol: hostrpc.Protocol}
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpHistory:
					<-release
					return
				default:
					return
				}
				_ = json.NewEncoder(conn).Encode(resp)
			}()
		}
	}()
	c := New(Config{HostMode: Auto, HostWait: time.Second, Workspace: t.TempDir(), State: state})
	c.startupDeadline = time.Now().Add(80 * time.Millisecond)
	start := time.Now()
	if _, err := c.RefreshLatest(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RefreshLatest error = %v, want the startup deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("RefreshLatest took %v; it started a second deadline", elapsed)
	}
}

func TestAttachFollowsFromTheGivenCursor(t *testing.T) {
	state := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var cursors []string
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				var req hostrpc.Request
				if json.NewDecoder(conn).Decode(&req) != nil {
					return
				}
				resp := hostrpc.Response{Protocol: hostrpc.Protocol}
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpProgress:
					mu.Lock()
					cursors = append(cursors, req.Cursor)
					mu.Unlock()
					resp.Page = &hostrpc.Page{Next: "pos:8", Quiescent: true, Items: []hostrpc.Item{{Pos: 8, RunID: "run", Kind: "returned", Result: "answer"}}}
				case hostrpc.OpView:
					resp.Run = &hostrpc.Run{RunID: "run", Status: hostrpc.StatusReturned, Result: "answer", Cursor: "pos:8"}
				default:
					t.Errorf("unexpected operation %q", req.Op)
				}
				_ = json.NewEncoder(conn).Encode(resp)
			}()
		}
	}()
	c := New(Config{HostMode: Auto, HostWait: time.Second, Workspace: t.TempDir(), State: state})
	c.runtime.state = state
	turn, err := c.Attach(context.Background(), hostrpc.HistoryRun{RunID: "run", InputID: "in"}, "pos:7")
	if err != nil {
		t.Fatal(err)
	}
	for {
		e, err := turn.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if e.Result != nil {
			if e.Result.Code != OK || e.Result.Run.Result != "answer" {
				t.Fatalf("attached result = %+v", e.Result)
			}
			break
		}
	}
	mu.Lock()
	got := slices.Clone(cursors)
	mu.Unlock()
	if len(got) != 1 || got[0] != "pos:7" {
		t.Fatalf("follow cursors = %v, want the history cursor pos:7", got)
	}
	listener.Close()
	<-serverDone
}

func TestStartupResolvableClassifiesOnlyHeldInputs(t *testing.T) {
	if !startupResolvable(&hostrpc.RemoteError{Code: hostrpc.CodeHeld, Message: "held"}) {
		t.Fatal("held inputs were not classified as resolvable")
	}
	if !startupResolvable(fmt.Errorf("wrap: %w", hostrpc.ErrHeld)) {
		t.Fatal("wrapped held inputs were not classified as resolvable")
	}
	if startupResolvable(hostrpc.SpecError{Err: errors.New("unsupported context version")}) {
		t.Fatal("an ordinary spec refusal was classified as resolvable")
	}
	if startupResolvable(&hostrpc.RemoteError{Code: hostrpc.CodeSpec, Message: "refused"}) {
		t.Fatal("an ordinary protocol refusal was classified as resolvable")
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/sqlite"
	"github.com/ChrAlpha/canti/runtime/internal/host"
	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// The joint tests drive a carry through the host process: a retrieval of an
// imported file, a required handover that carries the retrieved range into
// the next window, and a return.

const jointNotes = "The parser was rewritten in v2.\nfunc parse is the entry point.\n"

var jointScript = map[int]core.Decision{
	1: {Kind: core.Exec, Purpose: "read", Payload: `canti:retrieve {"operation":"read","path":"/canti/imports/notes","reason":"Read the notes."}`},
	2: {Kind: core.Exec, Purpose: "carry", Payload: `canti:focus {"sources":[{"seq":5,"source":0}]}`},
	3: {Kind: core.Return, Payload: "done"},
}

// jointFixture is a fixture of the carry script whose imports directory
// holds the notes, with the run flags that require a handover after every
// step and import that directory.
func jointFixture(t *testing.T) (f *fixture, notes string, flags []string) {
	t.Helper()
	f = newFixture(t, jointScript)
	dir := t.TempDir()
	notes = filepath.Join(dir, "notes")
	if err := os.WriteFile(notes, []byte(jointNotes), 0o600); err != nil {
		t.Fatal(err)
	}
	return f, notes, []string{"-history-imports", dir}
}

// carryOf returns the transition of h that performed the carry, which it
// requires to be the only one, and the request that followed it.
func carryOf(t *testing.T, h []entry) (performed, next entry) {
	t.Helper()
	n := 0
	for i, e := range h {
		if e.Kind == "performed" && e.Facts.Outcome != nil && e.Facts.Outcome.Focus != nil {
			performed, n = e, n+1
			for _, later := range h[i+1:] {
				if later.Kind == "request_frozen" {
					next = later
					break
				}
			}
		}
	}
	if n != 1 || next.Facts.Request == nil {
		t.Fatalf("%d carry transitions, and the request after the carry %v; history %q", n, next.Facts.Request != nil, kinds(h))
	}
	return performed, next
}

// carryFacts is the outcome and window that carry committed, and the bytes
// the carry took in the request after it, with the run's ID replaced, so
// that carries of two runs compare.
func carryFacts(t *testing.T, carry, next entry) string {
	t.Helper()
	b, err := json.Marshal(struct {
		Outcome *harness.Outcome
		Window  *harness.ContextState
		Carry   int
	}{carry.Facts.Outcome, carry.Facts.ContextState, next.Facts.Request.Context.Bytes.Focus})
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(b), carry.State.RunID, "RUN")
}

// controlCarry is the carry facts of an uninterrupted run of the carry
// script, embedded.
func controlCarry(t *testing.T) string {
	t.Helper()
	f, _, flags := jointFixture(t)
	if r := f.cli(nil, "run", append([]string{"-host", "embedded", "-goal", "g"}, flags...)...); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("control run: %+v", r)
	}
	carry, next := carryOf(t, f.history())
	return carryFacts(t, carry, next)
}

// TestJointCarryIsFollowedAcrossADetachWithoutGapOrRepeat requires that a
// follower of a carry run through the host process that reads part of its
// progress, loses its client, and reads on from its cursor sees exactly the
// items a fresh read from the start shows, among them one decided item with
// the protocol word of the carry action and one completed carry with its
// window, and that a retry of the same input then names the same run without
// a difference and commits nothing.
func TestJointCarryIsFollowedAcrossADetachWithoutGapOrRepeat(t *testing.T) {
	f, _, flags := jointFixture(t)
	f.autoHost()
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.model.decideWith(func(step int, _ string) (core.Decision, error) {
		if step == 2 {
			once.Do(func() { close(reached) })
			<-release
		}
		return jointScript[step], nil
	})
	args := append([]string{"-host", "auto", "-input", "k", "-goal", "g"}, flags...)
	cmd, wait := f.start(nil, "run", args...)
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("the carry's request never came")
	}
	ctx := context.Background()
	c := &hostrpc.Client{Path: f.socket()}
	v, err := c.View(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	read := func(from string) ([]hostrpc.Item, string) {
		var items []hostrpc.Item
		for {
			p, err := c.Progress(ctx, v.RunID, from, 2)
			if err != nil {
				t.Fatal(err)
			}
			items, from = append(items, p.Items...), p.Next
			if !p.More {
				return items, from
			}
		}
	}
	before, cursor := read("")
	cmd.Process.Signal(syscall.SIGKILL)
	wait()
	close(release)
	waitFor(t, func() bool { return f.status().Phase == core.PhaseReturned })
	after, _ := read(cursor)
	all, _ := read("")
	if !reflect.DeepEqual(append(before, after...), all) || len(before) == 0 || len(after) == 0 {
		t.Fatalf("read %d items, detached, read %d more; a fresh read shows %d:\nbefore %+v\nafter %+v\nall %+v", len(before), len(after), len(all), before, after, all)
	}
	carries := 0
	for _, it := range all {
		if it.Kind == "completed" && it.Local && it.Outcome != nil && it.Outcome.Focus && it.Outcome.FocusSources == 1 {
			carries++
		}
	}
	resp, err := c.Call(ctx, hostrpc.Request{Op: hostrpc.OpProgress, Cursor: hostserver.CursorOf(host.Cursor{RunID: v.RunID})})
	if err != nil {
		t.Fatal(err)
	}
	words := 0
	for _, it := range resp.Page.Items {
		if it.Kind == "decided" && it.Action == "focus" {
			words++
		}
	}
	if carries != 1 || words != 1 {
		t.Fatalf("%d completed carries and %d decided items with the carry word", carries, words)
	}
	rows := len(f.history())
	if r := f.cli(nil, "run", args...); r.code != exitOK || r.stdout != "done\n" || strings.Contains(r.stderr, "differs") || len(f.history()) != rows {
		t.Fatalf("a retry of the input: %+v, rows %d -> %d", r, rows, len(f.history()))
	}
	carryOf(t, f.history())
}

// TestJointACarryInterruptedAtItsCommitIsPerformedOnce requires that a carry
// whose commit a host shutdown interrupts, before it lands or after it
// landed with its acknowledgement lost, or whose host is killed right
// before or right after it commits, is performed exactly once after the next
// host resumes the run, with the outcome, window, and carried bytes of an
// uninterrupted run, although the imported source was rewritten in between:
// the carry is re-performed from the committed retrieval by point reads,
// never by reading the source again. A shutdown commits nothing of its own
// and nothing after it.
func TestJointACarryInterruptedAtItsCommitIsPerformedOnce(t *testing.T) {
	want := controlCarry(t)
	for _, tc := range []struct {
		name, crash string
		shutdown    bool
	}{
		{"shutdown before the commit lands", "block:performed@2", true},
		{"shutdown after the commit landed unacknowledged", "lose:performed@2", true},
		{"host killed before the commit", "before:performed@2", false},
		{"host killed after the commit", "after:performed@2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, notes, flags := jointFixture(t)
			f.autoHost()
			marker := filepath.Join(t.TempDir(), "held")
			_, wait := f.start([]string{"CANTI_TEST_CRASH=" + tc.crash, "CANTI_TEST_BLOCKED=" + marker}, "run",
				append([]string{"-host", "auto", "-goal", "g"}, flags...)...)
			if tc.shutdown {
				waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
				held := f.history()
				landed := 0
				for _, e := range held {
					if e.Kind == "performed" && e.Facts.Outcome != nil && e.Facts.Outcome.Focus != nil {
						landed++
					}
				}
				if want := map[bool]int{true: 1, false: 0}[strings.HasPrefix(tc.crash, "lose:")]; landed != want {
					t.Fatalf("%d carry rows while the commit is held, want %d", landed, want)
				}
				if r := f.cli(nil, "host", "-stop"); r.code != exitOK {
					t.Fatalf("host -stop: %+v", r)
				}
				waitFor(t, func() bool { _, err := os.Stat(f.socket()); return os.IsNotExist(err) })
				time.Sleep(300 * time.Millisecond)
				if h := f.history(); !reflect.DeepEqual(h, held) {
					t.Fatalf("the shutdown committed %q", kinds(h[len(held):]))
				}
			}
			if r := wait(); r.code == exitOK {
				t.Fatalf("the client of an interrupted host: %+v", r)
			}
			if err := os.WriteFile(notes, []byte("rewritten after the retrieval\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if r := f.cli(nil, "resume", "-host", "auto"); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("resume: %+v", r)
			}
			h := f.history()
			for _, e := range h {
				if e.Kind == "stop_requested" {
					t.Fatalf("a stop was committed: %q", kinds(h))
				}
			}
			carry, next := carryOf(t, h)
			if got := carryFacts(t, carry, next); got != want {
				t.Fatalf("the carry committed\n%s\nwant, as an uninterrupted run,\n%s", got, want)
			}
		})
	}
}

// smallSendBufferListener bounds each accepted socket's send buffer so a
// progress page fills it independently of the host's default socket buffers.
// The connection and its deadlines remain those of the real Unix socket.
type smallSendBufferListener struct{ net.Listener }

func (l smallSendBufferListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := c.(*net.UnixConn).SetWriteBuffer(4096); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// TestJointAFollowerThatStopsReadingDelaysNoOtherClient requires that a
// follower that sends a full progress request for a page of carry rows larger
// than a connection buffers, and then reads nothing, keeps no other client of
// the host waiting: another client is answered, and the run, driven
// on meanwhile, ends before the host's write timeout of 10 seconds passes.
// After it passes, the stalled connection is ended with its answer cut, and
// a follower that reconnects from the same cursor reads the same items a
// fresh read shows. The real host and server run in-process to control their
// listener; the CLI clients run in subprocesses.
func TestJointAFollowerThatStopsReadingDelaysNoOtherClient(t *testing.T) {
	f, _, flags := jointFixture(t)
	inProcessHost(t, f, nil, func(path string) net.Listener {
		return smallSendBufferListener{listenAt(t)(path)}
	})
	// Every byte of a purpose is a control byte, which JSON writes as six
	// bytes, and every byte of a command a backslash, which it writes as two,
	// so a page of 64 rows, read before the last step, takes about 260 KiB,
	// comfortably more than the fixture's bounded socket send buffer.
	const last = 25
	purpose := strings.Repeat("\x01", sqlite.ProgressCommandBytes)
	backslashes := strings.Repeat(`\\`, sqlite.ProgressCommandBytes/2)
	reached, release := make(chan struct{}), make(chan struct{})
	var released sync.Once
	unhold := func() { released.Do(func() { close(release) }) }
	defer unhold()
	f.model.decideWith(func(step int, _ string) (core.Decision, error) {
		switch {
		case step == last:
			close(reached)
			<-release
			return core.Decision{Kind: core.Return, Payload: "done"}, nil
		case step%2 == 1:
			return core.Decision{Kind: core.Exec, Purpose: purpose,
				Payload: `canti:retrieve {"operation":"read","path":"/canti/imports/notes","reason":"` + backslashes + `"}`}, nil
		default:
			return core.Decision{Kind: core.Exec, Purpose: purpose,
				Payload: fmt.Sprintf(`canti:focus %s{"sources":[{"seq":%d,"source":0}]}`, strings.Repeat(" ", sqlite.ProgressCommandBytes), 5*(step-1))}, nil
		}
	})
	_, wait := f.start(nil, "run", append([]string{"-host", "auto", "-goal", "g", "-max-steps", strconv.Itoa(last)}, flags...)...)
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("the last step's request never came")
	}
	ctx := context.Background()
	c := &hostrpc.Client{Path: f.socket()}
	v, err := c.View(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	from := hostserver.CursorOf(host.Cursor{RunID: v.RunID})
	request, err := json.Marshal(hostrpc.Request{Protocol: hostrpc.Protocol, Op: hostrpc.OpProgress, Cursor: from})
	if err != nil {
		t.Fatal(err)
	}
	stalled, err := net.Dial("unix", f.socket())
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	if _, err := stalled.Write(append(request, '\n')); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	time.Sleep(200 * time.Millisecond)
	if _, err := c.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	unhold()
	if r := wait(); r.code != exitOK || r.stdout != "done\n" {
		t.Fatalf("run: %+v", r)
	}
	if d := time.Since(start); d >= 10*time.Second {
		t.Fatalf("another client and the end of the run took %v beside the stalled follower", d)
	}
	time.Sleep(time.Until(start.Add(11 * time.Second)))
	fresh, err := c.Call(ctx, hostrpc.Request{Op: hostrpc.OpProgress, Cursor: from})
	if err != nil {
		t.Fatal(err)
	}
	whole, err := json.Marshal(fresh)
	if err != nil {
		t.Fatal(err)
	}
	stalled.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, err := io.Copy(io.Discard, stalled)
	if n == 0 || n >= int64(len(whole)) || err != nil {
		t.Fatalf("the stalled follower read %d bytes of an answer of %d, then %v: its connection was not ended with the answer cut", n, len(whole), err)
	}
	carries := 0
	for _, it := range fresh.Page.Items {
		if it.Kind == "completed" && it.Outcome != nil && it.Outcome.Focus && it.Outcome.FocusSources > 0 {
			carries++
		}
	}
	if !fresh.Page.More || carries == 0 {
		t.Fatalf("the page holds %d carries and more %v: it is no full page of carry rows", carries, fresh.Page.More)
	}
	again, err := (&hostrpc.Client{Path: f.socket()}).Call(ctx, hostrpc.Request{Op: hostrpc.OpProgress, Cursor: from})
	if err != nil || !reflect.DeepEqual(again.Page, fresh.Page) {
		t.Fatalf("a follower reconnected from the cursor read %+v, %v; a fresh read shows %+v", again.Page, err, fresh.Page)
	}
}

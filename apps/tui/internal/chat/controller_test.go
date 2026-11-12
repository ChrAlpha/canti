package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

type followFixture struct {
	calls   []string
	waitErr error
}

func (f *followFixture) Progress(_ context.Context, cur string, _ int) (hostrpc.Page, error) {
	f.calls = append(f.calls, "page:"+cur)
	if cur == "" {
		return hostrpc.Page{Next: "a", Items: []hostrpc.Item{{Seq: 1}}}, nil
	}
	return hostrpc.Page{Next: "b", Items: []hostrpc.Item{{Seq: 2}}, Quiescent: true}, nil
}
func (f *followFixture) Wait(context.Context, string) (hostrpc.Wait, error) {
	f.calls = append(f.calls, "wait")
	return hostrpc.Wait{Cursor: "b", Quiescent: true}, f.waitErr
}
func (f *followFixture) View(context.Context, string) (hostrpc.Run, error) {
	f.calls = append(f.calls, "view")
	return hostrpc.Run{Status: hostrpc.StatusReturned, Result: "done"}, nil
}
func (f *followFixture) Stop(context.Context, string, time.Duration) (hostrpc.Run, error) {
	return hostrpc.Run{}, nil
}
func TestFollowDrainsFinalProgressBeforeOutcome(t *testing.T) {
	f := new(followFixture)
	var seq []int64
	out := follow(context.Background(), f, "", func(it hostrpc.Item) { seq = append(seq, it.Seq) })
	if out.Code != OK || len(seq) != 2 || seq[1] != 2 || strings.Join(f.calls, ",") != "page:,wait,page:a,view" {
		t.Fatalf("out=%+v seq=%v calls=%v", out, seq, f.calls)
	}
}
func TestFollowSurfacesWaitFailure(t *testing.T) {
	failure := errors.New("lost host")
	f := &followFixture{waitErr: failure}
	out := follow(context.Background(), f, "", func(hostrpc.Item) {})
	if !errors.Is(out.Err, failure) || out.Code != Error {
		t.Fatalf("out=%+v", out)
	}
}
func TestProgressBoundsPreserveAcceptanceAndFinalOutcome(t *testing.T) {
	turn := newTurn(Snapshot{}, false, func() {})
	turn.accept(hostrpc.Ack{Run: &hostrpc.Run{RunID: "r"}})
	for range 1000 {
		turn.progress(hostrpc.Item{Preview: strings.Repeat("x", 4096)})
	}
	if turn.bytes > progressBytes || len(turn.items) > progressEntries {
		t.Fatal("unbounded queue")
	}
	turn.finish(Result{Code: Resolution, Run: hostrpc.Run{EffectID: "unknown"}})
	first, _ := turn.Next(context.Background())
	if first.Ack == nil {
		t.Fatal("lost acceptance")
	}
	second, _ := turn.Next(context.Background())
	if second.Omitted == 0 {
		t.Fatal("missing omission")
	}
	for {
		e, err := turn.Next(context.Background())
		if err != nil {
			t.Fatal("lost outcome")
		}
		if e.Result != nil {
			if e.Result.Code != Resolution || e.Result.Run.EffectID != "unknown" {
				t.Fatal(e)
			}
			break
		}
	}
}
func TestPendingInterruptSurvivesAcceptance(t *testing.T) {
	canceled := false
	turn := newTurn(Snapshot{}, true, func() { canceled = true })
	turn.Interrupt()
	turn.accept(hostrpc.Ack{})
	select {
	case <-turn.stop:
	default:
		t.Fatal("lost pending stop")
	}
	if canceled {
		t.Fatal("first interrupt abandoned ambiguous submission")
	}
	turn.Interrupt()
	if !canceled {
		t.Fatal("second interrupt did not escape")
	}
}

type stopRace struct {
	waiting, stopping, release chan struct{}
	stopped                    atomic.Bool
	views                      atomic.Int32
}

func (s *stopRace) Progress(context.Context, string, int) (hostrpc.Page, error) {
	if s.stopped.Load() {
		return hostrpc.Page{Next: "done", Quiescent: true, Items: []hostrpc.Item{{Seq: 1, Kind: "completed"}}}, nil
	}
	return hostrpc.Page{Next: "before"}, nil
}
func (s *stopRace) Wait(ctx context.Context, _ string) (hostrpc.Wait, error) {
	close(s.waiting)
	<-ctx.Done()
	return hostrpc.Wait{}, ctx.Err()
}
func (s *stopRace) View(context.Context, string) (hostrpc.Run, error) {
	s.views.Add(1)
	return hostrpc.Run{Status: hostrpc.StatusStopped}, nil
}
func (s *stopRace) Stop(ctx context.Context, _ string, _ time.Duration) (hostrpc.Run, error) {
	close(s.stopping)
	select {
	case <-s.release:
		s.stopped.Store(true)
		return hostrpc.Run{Status: hostrpc.StatusStopped}, nil
	case <-ctx.Done():
		return hostrpc.Run{}, ctx.Err()
	}
}
func TestStopAcknowledgementPrecedesFinalProgressAndReaping(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		t.Run(fmt.Sprint(abandon), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			turn := newTurn(Snapshot{}, true, cancel)
			rpc := &stopRace{waiting: make(chan struct{}), stopping: make(chan struct{}), release: make(chan struct{})}
			c := New(Config{HostMode: Owned})
			done := make(chan Result, 1)
			go func() { done <- c.follow(ctx, rpc, turn, "run", "") }()
			<-rpc.waiting
			turn.Interrupt()
			<-rpc.stopping
			select {
			case out := <-done:
				t.Fatalf("reported before Stop: %+v", out)
			case <-time.After(20 * time.Millisecond):
			}
			if rpc.views.Load() != 0 {
				t.Fatal("View preceded Stop acknowledgement")
			}
			if abandon {
				turn.Interrupt()
			} else {
				close(rpc.release)
			}
			select {
			case out := <-done:
				if abandon {
					if out.Code != Error || !errors.Is(out.Err, context.Canceled) {
						t.Fatal(out)
					}
				} else {
					if out.Code != Stopped || len(turn.items) != 1 || rpc.views.Load() != 1 {
						t.Fatalf("final drain/result: %+v items=%v views=%d", out, turn.items, rpc.views.Load())
					}
				}
			case <-time.After(time.Second):
				t.Fatal("stop/cancel did not finish")
			}
		})
	}
}

func TestExplicitUncertainRetryDrivesItsReplayedIdleRun(t *testing.T) {
	state, err := os.MkdirTemp("", "canti-replay-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(state)
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var submissions, drives atomic.Int32
	var mu sync.Mutex
	var identities []string
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
				case hostrpc.OpSubmit:
					mu.Lock()
					identities = append(identities, req.Submit.InputID)
					mu.Unlock()
					if submissions.Add(1) <= 5 {
						return
					}
					resp.Ack = &hostrpc.Ack{Replayed: true, Run: &hostrpc.Run{RunID: "same", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}}
				case hostrpc.OpDrive:
					drives.Add(1)
					resp.Run = &hostrpc.Run{RunID: "same", Status: hostrpc.StatusReturned}
				case hostrpc.OpProgress:
					resp.Page = &hostrpc.Page{Quiescent: true}
				case hostrpc.OpView:
					resp.Run = &hostrpc.Run{RunID: "same", Status: hostrpc.StatusReturned}
				}
				json.NewEncoder(conn).Encode(resp)
			}()
		}
	}()
	c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
	c.GoalLimit = 100
	snapshot := NewSnapshot("one goal")
	first, err := c.Start(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if out := first.Outcome(); !out.Uncertain {
		t.Fatalf("lost ACK not retained: %+v", out)
	}
	if _, err := c.Start(context.Background(), NewSnapshot("new input")); err == nil {
		t.Fatal("new input bypassed uncertain acceptance")
	}
	changed := snapshot
	changed.Goal = "changed goal"
	if _, err := c.Start(context.Background(), changed); err == nil {
		t.Fatal("reconciliation changed the submitted snapshot")
	}
	second, err := c.Start(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if out := second.Outcome(); out.Code != OK || drives.Load() != 1 {
		t.Fatalf("retry=%+v drives=%d", out, drives.Load())
	}
	listener.Close()
	<-serverDone
	mu.Lock()
	defer mu.Unlock()
	for _, id := range identities {
		if id != snapshot.InputID {
			t.Fatal("retry changed input identity")
		}
	}
}

func TestInputLossDoesNotEscalateAnExplicitStop(t *testing.T) {
	canceled := false
	turn := newTurn(Snapshot{}, true, func() { canceled = true })
	turn.Interrupt()
	turn.InputLost()
	turn.InputLost()
	if canceled {
		t.Fatal("display/input failure escalated a pending stop")
	}
	turn.Interrupt()
	if !canceled {
		t.Fatal("explicit second interrupt did not abandon")
	}
}

func TestFirstOwnedInputLossRequestsStopAndAwaitsCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	turn := newTurn(Snapshot{}, true, cancel)
	rpc := &stopRace{waiting: make(chan struct{}), stopping: make(chan struct{}), release: make(chan struct{})}
	c := New(Config{HostMode: Owned})
	done := make(chan Result, 1)
	go func() { done <- c.follow(ctx, rpc, turn, "run", "") }()
	<-rpc.waiting
	turn.InputLost()
	turn.InputLost()
	<-rpc.stopping
	select {
	case out := <-done:
		t.Fatalf("input loss abandoned cleanup: %+v", out)
	case <-time.After(20 * time.Millisecond):
	}
	if ctx.Err() != nil {
		t.Fatal("input loss escalated to force cancellation")
	}
	close(rpc.release)
	select {
	case out := <-done:
		if out.Code != Stopped || rpc.views.Load() != 1 {
			t.Fatal(out)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish")
	}
}

func protocolHost(t *testing.T, handle func(hostrpc.Request) (hostrpc.Response, bool)) string {
	t.Helper()
	// Only this protocol fixture needs a bounded Unix socket path. Leave the
	// caller's TMPDIR unchanged for build output and all other fixture data.
	state, err := os.MkdirTemp("/tmp", "canti-protocol-")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
	if err != nil {
		os.RemoveAll(state)
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(3 * time.Second))
				var req hostrpc.Request
				if json.NewDecoder(conn).Decode(&req) != nil {
					return
				}
				response, drop := handle(req)
				if drop {
					return
				}
				response.Protocol = hostrpc.Protocol
				json.NewEncoder(conn).Encode(response)
			}()
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done; workers.Wait(); os.RemoveAll(state) })
	return state
}
func TestAdmissionUncertaintySurvivesFailedAndLaterRefusedReplies(t *testing.T) {
	for _, mode := range []string{"failed after admission", "lost then closing", "lost then spec refusal"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			var recover atomic.Bool
			var drives atomic.Int32
			var mu sync.Mutex
			var ids []string
			state := protocolHost(t, func(req hostrpc.Request) (resp hostrpc.Response, drop bool) {
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpSubmit:
					mu.Lock()
					ids = append(ids, req.Submit.InputID)
					mu.Unlock()
					n := calls.Add(1)
					if recover.Load() {
						resp.Ack = &hostrpc.Ack{Replayed: true, Run: &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}}
						break
					}
					if mode != "failed after admission" && n == 1 {
						return resp, true
					}
					code := hostrpc.CodeFailed
					if mode == "lost then closing" {
						code = hostrpc.CodeClosing
					}
					if mode == "lost then spec refusal" {
						code = hostrpc.CodeSpec
					}
					resp.Error = &hostrpc.Error{Code: code, Message: "request refused after an earlier durable admission"}
				case hostrpc.OpDrive:
					drives.Add(1)
					resp.Run = &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusReturned}
				case hostrpc.OpProgress:
					resp.Page = &hostrpc.Page{Quiescent: true}
				case hostrpc.OpView:
					resp.Run = &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusReturned}
				}
				return resp, false
			})
			c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
			c.GoalLimit = 100
			snapshot := NewSnapshot("goal")
			turn, err := c.Start(t.Context(), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			out := turn.Outcome()
			if mode == "lost then spec refusal" {
				// A spec refusal is the host's replay lookup proving the input
				// was never admitted, unlike a closing or transport error.
				if !out.Rejected || out.Uncertain {
					t.Fatalf("authoritative refusal = %+v", out)
				}
				return
			}
			if !out.Uncertain || out.Rejected {
				t.Fatalf("admission certainty changed: %+v", out)
			}
			if _, err = c.Start(t.Context(), NewSnapshot("goal")); err == nil {
				t.Fatal("fresh identity enabled after possible admission")
			}
			recover.Store(true)
			turn, err = c.Start(t.Context(), snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if out := turn.Outcome(); out.Code != OK || drives.Load() != 1 {
				t.Fatalf("reconcile=%+v drives=%d", out, drives.Load())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				if id != snapshot.InputID {
					t.Fatal("changed identity")
				}
			}
		})
	}
}
func TestPendingStopPrecedesReplayDrive(t *testing.T) {
	for _, inputLoss := range []bool{false, true} {
		t.Run(fmt.Sprint(inputLoss), func(t *testing.T) {
			var calls, drives, stops atomic.Int32
			pending := make(chan struct{})
			release := make(chan struct{})
			state := protocolHost(t, func(req hostrpc.Request) (resp hostrpc.Response, drop bool) {
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpSubmit:
					if calls.Add(1) == 1 {
						return resp, true
					}
					close(pending)
					select {
					case <-release:
					case <-t.Context().Done():
						return resp, true
					}
					resp.Ack = &hostrpc.Ack{Replayed: true, Run: &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}}
				case hostrpc.OpDrive:
					drives.Add(1)
					resp.Error = &hostrpc.Error{Code: hostrpc.CodeFailed, Message: "Drive must not precede pending stop"}
				case hostrpc.OpStop:
					stops.Add(1)
					resp.Run = &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusStopped}
				case hostrpc.OpProgress:
					resp.Page = &hostrpc.Page{Quiescent: true}
				case hostrpc.OpView:
					resp.Run = &hostrpc.Run{RunID: "admitted", Status: hostrpc.StatusStopped}
				}
				return resp, false
			})
			c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
			c.GoalLimit = 100
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			turn := newTurn(NewSnapshot("goal"), true, cancel)
			done := make(chan Result, 1)
			go func() { done <- c.run(ctx, turn) }()
			<-pending
			if inputLoss {
				turn.InputLost()
			} else {
				turn.Interrupt()
			}
			close(release)
			select {
			case out := <-done:
				if out.Code != Stopped || stops.Load() != 1 || drives.Load() != 0 {
					t.Fatalf("out=%+v stops=%d drives=%d", out, stops.Load(), drives.Load())
				}
			case <-time.After(time.Second):
				t.Fatal("pending stop did not finish")
			}
		})
	}
}

func TestFirstExplicitInterruptAfterInputLossStillAwaitsStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	turn := newTurn(Snapshot{}, true, cancel)
	rpc := &stopRace{waiting: make(chan struct{}), stopping: make(chan struct{}), release: make(chan struct{})}
	c := New(Config{HostMode: Owned})
	done := make(chan Result, 1)
	go func() { done <- c.follow(ctx, rpc, turn, "run", "") }()
	<-rpc.waiting
	turn.InputLost()
	<-rpc.stopping
	turn.Interrupt()
	select {
	case out := <-done:
		t.Fatalf("first explicit interrupt escalated automatic stop: %+v", out)
	case <-time.After(20 * time.Millisecond):
	}
	turn.Interrupt()
	select {
	case out := <-done:
		if out.Code != Error || !errors.Is(out.Err, context.Canceled) {
			t.Fatal(out)
		}
	case <-time.After(time.Second):
		t.Fatal("second explicit interrupt did not abandon")
	}
}

func TestStopDuringReplayDriveIsHonoredAfterDriveError(t *testing.T) {
	driving, release := make(chan struct{}), make(chan struct{})
	var submits, stops atomic.Int32
	state := protocolHost(t, func(req hostrpc.Request) (resp hostrpc.Response, drop bool) {
		switch req.Op {
		case hostrpc.OpHello:
			resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
		case hostrpc.OpSubmit:
			if submits.Add(1) == 1 {
				return resp, true
			}
			resp.Ack = &hostrpc.Ack{Replayed: true, Run: &hostrpc.Run{RunID: "r", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}}
		case hostrpc.OpDrive:
			close(driving)
			select {
			case <-release:
			case <-t.Context().Done():
				return resp, true
			}
			resp.Error = &hostrpc.Error{Code: hostrpc.CodeFailed, Message: "drive failed"}
		case hostrpc.OpStop:
			stops.Add(1)
			resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped}
		case hostrpc.OpProgress:
			resp.Page = &hostrpc.Page{Quiescent: true}
		case hostrpc.OpView:
			resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped}
		}
		return resp, false
	})
	c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
	c.GoalLimit = 100
	turn, err := c.Start(t.Context(), NewSnapshot("goal"))
	if err != nil {
		t.Fatal(err)
	}
	<-driving
	turn.Interrupt()
	close(release)
	if out := turn.Outcome(); out.Code != Stopped || stops.Load() != 1 {
		t.Fatalf("stop lost after Drive error: %+v stops=%d", out, stops.Load())
	}
}

func TestSetupAdoptsCanonicalStateBeforeConnecting(t *testing.T) {
	aliasRoot, err := os.MkdirTemp("/tmp", "canti-chat-state-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(aliasRoot) })
	alias := filepath.Join(aliasRoot, "state")
	canonical := make(chan string, 1)
	state := protocolHost(t, func(req hostrpc.Request) (hostrpc.Response, bool) {
		switch req.Op {
		case hostrpc.OpValidate:
			// Once validation answers, the original spelling disappears. Startup
			// must connect using the returned identity rather than recreate it.
			if err := os.Remove(alias); err != nil {
				return hostrpc.Response{Error: &hostrpc.Error{Code: hostrpc.CodeFailed, Message: err.Error()}}, false
			}
			return hostrpc.Response{Validated: &hostrpc.Validated{State: <-canonical, Spec: req.Validate.Submission.Spec, GoalLimit: 100}}, false
		case hostrpc.OpHello:
			return hostrpc.Response{Hello: &hostrpc.Hello{Protocol: hostrpc.Protocol}}, false
		}
		return hostrpc.Response{}, false
	})
	canonical <- state
	if err := os.Symlink(state, alias); err != nil {
		t.Fatal(err)
	}
	c := New(Config{State: alias, HostMode: Auto, HostWait: 100 * time.Millisecond, RuntimePath: filepath.Join(aliasRoot, "must-not-start-runtime")})
	if err := c.Setup(t.Context(), t.Context()); err != nil {
		t.Fatal(err)
	}
	if c.cfg.State != state || c.runtime.state != state {
		t.Fatalf("startup retained alias: config=%q runtime=%q,want %q", c.cfg.State, c.runtime.state, state)
	}
	if _, err := os.Lstat(alias); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup recreated an unvalidated State path: %v", err)
	}
}

func TestFreshAppliesOnlyToTheFirstInput(t *testing.T) {
	var fresh []bool
	state := protocolHost(t, func(req hostrpc.Request) (hostrpc.Response, bool) {
		resp := hostrpc.Response{Protocol: hostrpc.Protocol}
		switch req.Op {
		case hostrpc.OpHello:
			resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
		case hostrpc.OpSubmit:
			fresh = append(fresh, req.Submit.Fresh)
			resp.Ack = &hostrpc.Ack{Run: &hostrpc.Run{RunID: "r", Status: hostrpc.StatusReturned, Cursor: "pos:1"}}
		case hostrpc.OpProgress:
			resp.Page = &hostrpc.Page{Next: "pos:1", Quiescent: true}
		case hostrpc.OpView:
			resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusReturned, Result: "done"}
		}
		return resp, false
	})
	c := New(Config{State: state, HostMode: Auto, HostWait: time.Second, Fresh: true})
	c.GoalLimit = 100
	for i := range 2 {
		turn, err := c.Start(t.Context(), NewSnapshot(fmt.Sprintf("goal %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if out := turn.Outcome(); out.Code != OK {
			t.Fatalf("turn %d = %+v", i, out)
		}
	}
	if len(fresh) != 2 || !fresh[0] || fresh[1] {
		t.Fatalf("submissions fresh = %v, want [true false]", fresh)
	}
}

// followUpStop serves the agent log of a run that starts a follow-up and
// records which run a stop targets.
type followUpStop struct {
	followFixture
	turn   *Turn
	stopID string
	pages  int
}

func (f *followUpStop) Progress(ctx context.Context, cursor string, _ int) (hostrpc.Page, error) {
	f.pages++
	if f.pages == 1 {
		f.turn.progress(hostrpc.Item{RunID: "followup", Kind: "input"})
		f.turn.Interrupt()
		<-ctx.Done()
		return hostrpc.Page{}, ctx.Err()
	}
	return hostrpc.Page{Next: "done", Quiescent: true}, nil
}

func (f *followUpStop) Stop(_ context.Context, id string, _ time.Duration) (hostrpc.Run, error) {
	f.stopID = id
	return hostrpc.Run{Status: hostrpc.StatusStopped}, nil
}

func (f *followUpStop) View(context.Context, string) (hostrpc.Run, error) {
	return hostrpc.Run{RunID: "followup", Status: hostrpc.StatusStopped}, nil
}

func TestStopTargetsTheRunTheLogShows(t *testing.T) {
	turn := newTurn(Snapshot{}, false, func() {})
	turn.accept(hostrpc.Ack{Run: &hostrpc.Run{RunID: "original"}})
	f := &followUpStop{turn: turn}
	c := New(Config{})
	c.follow(context.Background(), f, turn, "original", "")
	if f.stopID != "followup" {
		t.Fatalf("stop targeted %q after the follow-up started", f.stopID)
	}
}

func TestRuntimeLifecycleAccessIsSerialized(t *testing.T) {
	done := make(chan struct{})
	close(done)
	a := runtimeClient{starting: done}
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			a.canStartHost()
		}()
	}
	close(gate)
	wg.Wait()
}

func TestSubmitDistinguishesRejectionFromUnknownAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name          string
		response      *hostrpc.Error
		drop          bool
		wantUncertain bool
		wantRejection bool
	}{
		{name: "proven rejection", response: &hostrpc.Error{Code: hostrpc.CodeSpec, Message: "refused"}, wantRejection: true},
		{name: "unknown acceptance", drop: true, wantUncertain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := protocolHost(t, func(req hostrpc.Request) (hostrpc.Response, bool) {
				resp := hostrpc.Response{Protocol: hostrpc.Protocol}
				switch req.Op {
				case hostrpc.OpHello:
					resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
				case hostrpc.OpSubmit:
					if tc.drop {
						return resp, true
					}
					resp.Error = tc.response
				}
				return resp, false
			})
			c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
			c.GoalLimit = 100
			out := c.Submit(context.Background(), NewSnapshot("steer"), "steer")
			if out.Err == nil || out.Uncertain != tc.wantUncertain || out.Rejected != tc.wantRejection {
				t.Fatalf("Submit = %+v", out)
			}
		})
	}
}

func TestLostAcceptanceSurvivesClosingReply(t *testing.T) {
	var calls atomic.Int32
	state := protocolHost(t, func(req hostrpc.Request) (hostrpc.Response, bool) {
		resp := hostrpc.Response{Protocol: hostrpc.Protocol}
		switch req.Op {
		case hostrpc.OpHello:
			resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
		case hostrpc.OpSubmit:
			if calls.Add(1) == 1 {
				return resp, true
			}
			resp.Error = &hostrpc.Error{Code: hostrpc.CodeClosing, Message: "closing"}
		}
		return resp, false
	})
	c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
	c.GoalLimit = 100
	out := c.Submit(context.Background(), NewSnapshot("steer"), "steer")
	if !out.Uncertain || out.Rejected {
		t.Fatalf("lost acceptance was cleared by a closing reply: %+v", out)
	}
}

func TestHeldRefusalIsNotUnknownAcceptance(t *testing.T) {
	state := protocolHost(t, func(req hostrpc.Request) (hostrpc.Response, bool) {
		resp := hostrpc.Response{Protocol: hostrpc.Protocol}
		switch req.Op {
		case hostrpc.OpHello:
			resp.Hello = &hostrpc.Hello{Protocol: hostrpc.Protocol}
		case hostrpc.OpSubmit:
			resp.Error = &hostrpc.Error{Code: hostrpc.CodeHeld, Message: "withdraw held inputs first"}
		}
		return resp, false
	})
	c := New(Config{State: state, HostMode: Auto, HostWait: time.Second})
	c.GoalLimit = 100
	turn, err := c.Start(context.Background(), NewSnapshot("new input"))
	if err != nil {
		t.Fatal(err)
	}
	out := turn.Outcome()
	if out.Uncertain || !out.Rejected {
		t.Fatalf("held refusal = %+v", out)
	}
}

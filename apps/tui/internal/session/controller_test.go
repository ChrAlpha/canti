package session

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

func (f *followFixture) Progress(_ context.Context, _ string, cur string, _ int) (hostrpc.Page, error) {
	f.calls = append(f.calls, "page:"+cur)
	if cur == "" {
		return hostrpc.Page{Next: "a", Items: []hostrpc.Item{{Seq: 1}}}, nil
	}
	return hostrpc.Page{Next: "b", Items: []hostrpc.Item{{Seq: 2}}, Halted: true}, nil
}
func (f *followFixture) Wait(context.Context, string, string) (hostrpc.Wait, error) {
	f.calls = append(f.calls, "wait")
	return hostrpc.Wait{Cursor: "b", Halted: true}, f.waitErr
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
	out := follow(context.Background(), f, "r", func(it hostrpc.Item) { seq = append(seq, it.Seq) })
	if out.Code != OK || len(seq) != 2 || seq[1] != 2 || strings.Join(f.calls, ",") != "page:,wait,page:a,view" {
		t.Fatalf("out=%+v seq=%v calls=%v", out, seq, f.calls)
	}
}
func TestFollowSurfacesWaitFailure(t *testing.T) {
	failure := errors.New("lost host")
	f := &followFixture{waitErr: failure}
	out := follow(context.Background(), f, "r", func(hostrpc.Item) {})
	if !errors.Is(out.Err, failure) || out.Code != Error {
		t.Fatalf("out=%+v", out)
	}
}
func TestProgressBoundsPreserveAcceptanceAndFinalOutcome(t *testing.T) {
	turn := newTurn(Snapshot{}, false, func() {})
	turn.accept(hostrpc.Ack{Run: hostrpc.Run{RunID: "r"}})
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

func (s *stopRace) Progress(context.Context, string, string, int) (hostrpc.Page, error) {
	if s.stopped.Load() {
		return hostrpc.Page{Next: "done", Halted: true, Items: []hostrpc.Item{{Seq: 1, Kind: "completed"}}}, nil
	}
	return hostrpc.Page{Next: "before"}, nil
}
func (s *stopRace) Wait(ctx context.Context, _ string, _ string) (hostrpc.Wait, error) {
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
			go func() { done <- c.follow(ctx, rpc, turn, "run") }()
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
					resp.Ack = &hostrpc.Ack{Replayed: true, Run: hostrpc.Run{RunID: "same", Status: hostrpc.StatusActive, Driver: hostrpc.DriverIdle}}
				case hostrpc.OpDrive:
					drives.Add(1)
					resp.Run = &hostrpc.Run{RunID: "same", Status: hostrpc.StatusReturned}
				case hostrpc.OpProgress:
					resp.Page = &hostrpc.Page{Halted: true}
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
	go func() { done <- c.follow(ctx, rpc, turn, "run") }()
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

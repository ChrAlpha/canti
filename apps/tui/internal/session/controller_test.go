package session

import (
	"context"
	"errors"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
	"strings"
	"testing"
	"time"
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

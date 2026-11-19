package chat

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

type deadlineDrain struct {
	calls []string
}

func (d *deadlineDrain) Progress(_ context.Context, cursor string, _ int) (hostrpc.Page, error) {
	d.calls = append(d.calls, "progress:"+cursor)
	if cursor == "" {
		return hostrpc.Page{Next: "a", Items: []hostrpc.Item{{Seq: 1}}}, nil
	}
	return hostrpc.Page{Next: "b", Quiescent: true, Items: []hostrpc.Item{{Seq: 2}}}, nil
}
func (d *deadlineDrain) Wait(context.Context, string) (hostrpc.Wait, error) {
	d.calls = append(d.calls, "wait")
	return hostrpc.Wait{}, context.DeadlineExceeded
}
func (d *deadlineDrain) View(context.Context, string) (hostrpc.Run, error) {
	d.calls = append(d.calls, "view")
	return hostrpc.Run{Status: hostrpc.StatusReturned, Result: "done"}, nil
}
func (d *deadlineDrain) Stop(context.Context, string, time.Duration) (hostrpc.Run, error) {
	return hostrpc.Run{}, nil
}

func TestWaitDeadlineDrainsTheFinalPageBeforeAClosedView(t *testing.T) {
	d := new(deadlineDrain)
	var seq []int64
	out := follow(context.Background(), d, "", func(it hostrpc.Item) { seq = append(seq, it.Seq) })
	if out.Code != OK || len(seq) != 2 || seq[1] != 2 || out.Cursor != "b" || strings.Join(d.calls, ",") != "progress:,wait,view,progress:a,view" {
		t.Fatalf("out=%+v seq=%v calls=%v", out, seq, d.calls)
	}
}

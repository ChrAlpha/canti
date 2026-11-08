package pipe

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/apps/tui/internal/session"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

func TestPipeHangupDetachesButExplicitSignalStopsAutoRun(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGHUP, syscall.SIGTERM, os.Interrupt} {
		t.Run(sig.String(), func(t *testing.T) {
			state, err := os.MkdirTemp("", "canti-signal-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(state)
			listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
			if err != nil {
				t.Fatal(err)
			}
			var workers sync.WaitGroup
			var once sync.Once
			waiting := make(chan struct{})
			var stops atomic.Int32
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					workers.Add(1)
					go func() {
						defer workers.Done()
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
							resp.Ack = &hostrpc.Ack{Run: hostrpc.Run{RunID: "r", Status: hostrpc.StatusActive, Driver: hostrpc.DriverDriving}}
						case hostrpc.OpProgress:
							resp.Page = &hostrpc.Page{Next: "cursor", Halted: stops.Load() > 0}
						case hostrpc.OpWait:
							once.Do(func() { close(waiting) })
							var b [1]byte
							conn.Read(b[:])
							return
						case hostrpc.OpStop:
							stops.Add(1)
							resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped}
						case hostrpc.OpView:
							resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped}
						}
						json.NewEncoder(conn).Encode(resp)
					}()
				}
			}()
			defer func() { listener.Close(); <-serverDone; workers.Wait() }()
			c := session.New(session.Config{State: state, HostMode: session.Auto, HostWait: time.Second})
			c.GoalLimit = 100
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			signals := make(chan os.Signal, 1)
			done := make(chan int, 1)
			go func() {
				code, err, stop := runInput(ctx, c, "goal", io.Discard, io.Discard, signals, nil)
				if err != nil || !stop {
					code = -1
				}
				done <- code
			}()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("follow did not start")
			}
			signals <- sig
			want := session.Stopped
			if sig == syscall.SIGHUP {
				want = session.Active
			}
			select {
			case code := <-done:
				if code != want {
					t.Fatalf("exit %d want %d", code, want)
				}
			case <-ctx.Done():
				t.Fatal("signal did not end presenter")
			}
			expected := int32(1)
			if sig == syscall.SIGHUP {
				expected = 0
			}
			if stops.Load() != expected {
				t.Fatalf("persistent stops=%d want %d", stops.Load(), expected)
			}
		})
	}
}

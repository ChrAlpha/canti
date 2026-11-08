package pipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
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

// CodeFailed can be returned after durable admission. The mandatory outcome
// must retain its identity even with all reconnect diagnostics suppressed.
func TestUncertainPipeExitPreservesRecoveryIdentityAtQuietLogLevel(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		for _, brokenOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("malformed=%v/output-fails=%v", malformed, brokenOutput), func(t *testing.T) {
				state, err := os.MkdirTemp("", "canti-uncertain-")
				if err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(state)
				listener, err := net.Listen("unix", filepath.Join(state, hostrpc.SocketName))
				if err != nil {
					t.Fatal(err)
				}
				var identity string
				done := make(chan struct{})
				go func() {
					defer close(done)
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
								identity = req.Submit.InputID
								if malformed {
									io.WriteString(conn, "{")
									return
								}
								resp.Error = &hostrpc.Error{Code: hostrpc.CodeFailed, Message: "final response could not be encoded"}
							}
							json.NewEncoder(conn).Encode(resp)
						}()
					}
				}()
				c := session.New(session.Config{State: state, HostMode: session.Auto, HostWait: time.Second, SessionID: "conversation", LogLevel: slog.LevelError})
				c.GoalLimit = 128
				var output strings.Builder
				var diagnostic io.Writer = &output
				failure := errors.New("recovery output unavailable")
				if brokenOutput {
					diagnostic = rejectRecovery{failure}
				}
				code, runErr, stop := runInput(t.Context(), c, "original goal", io.Discard, diagnostic, nil, nil)
				listener.Close()
				<-done
				if code != session.Error || !stop || identity == "" {
					t.Fatalf("uncertain result: code=%d err=%v stop=%v identity=%q", code, runErr, stop, identity)
				}
				if brokenOutput {
					if !errors.Is(runErr, failure) {
						t.Fatalf("recovery write error lost: %v", runErr)
					}
					return
				}
				if runErr != nil || !strings.Contains(output.String(), "canti run -session conversation -input "+identity) || !strings.Contains(output.String(), "original goal and launch flags") || !strings.Contains(output.String(), "before starting new input") {
					t.Fatalf("unsafe uncertain output: %q; err=%v", output.String(), runErr)
				}
				if strings.Contains(output.String(), "Host connection interrupted") {
					t.Fatal("quiet diagnostics were not suppressed")
				}
			})
		}
	}
}

type rejectRecovery struct{ err error }

func (w rejectRecovery) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "acceptance of input") {
		return 0, w.err
	}
	return len(p), nil
}

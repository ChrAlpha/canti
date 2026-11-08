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
	for _, tc := range []struct {
		sig     os.Signal
		blocked bool
	}{
		{syscall.SIGHUP, false}, {syscall.SIGTERM, false}, {os.Interrupt, false},
		{syscall.SIGHUP, true}, {syscall.SIGTERM, true}, {os.Interrupt, true},
	} {
		t.Run(fmt.Sprintf("%s/blocked-stderr=%v", tc.sig, tc.blocked), func(t *testing.T) {
			sig := tc.sig
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
			stopping, releaseStop := make(chan struct{}), make(chan struct{})
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
							close(stopping)
							select {
							case <-releaseStop:
							case <-time.After(2 * time.Second):
							}
							resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped, StopReason: "cancelled"}
						case hostrpc.OpView:
							resp.Run = &hostrpc.Run{RunID: "r", Status: hostrpc.StatusStopped, StopReason: "cancelled"}
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
			var diagnostic io.Writer = io.Discard
			if tc.blocked {
				diagnostic = failedDiagnostic{os.ErrDeadlineExceeded}
			}
			go func() {
				code, err, stop := runInput(ctx, c, "goal", io.Discard, diagnostic, signals, nil)
				if (err != nil) != tc.blocked || tc.blocked && !errors.Is(err, os.ErrDeadlineExceeded) || !stop {
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
			if sig != syscall.SIGHUP {
				select {
				case <-stopping:
				case <-ctx.Done():
					t.Fatal("persistent stop did not begin")
				}
				select {
				case code := <-done:
					t.Fatalf("exited before stop acknowledgment: %d", code)
				default:
				}
				close(releaseStop)
			}
			want := session.Stopped
			if sig == syscall.SIGHUP {
				want = session.Active
				if tc.blocked {
					want = session.Error
				}
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

// No progress text is emitted by the signal fixture. Fail the final diagnostic,
// after Stop and the final View, without turning an empty ACK into input loss.
type failedDiagnostic struct{ err error }

func (w failedDiagnostic) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 0, w.err
}

func TestPipeCancellationExitPreservesOutputFailureBoundaries(t *testing.T) {
	cancelled := session.Result{Code: session.Stopped, Run: hostrpc.Run{RunID: "run", Status: hostrpc.StatusStopped, StopReason: "cancelled"}}
	for _, tc := range []struct {
		name         string
		result       session.Result
		explicitStop bool
		answerFails  bool
		want         int
	}{
		{"explicit cancellation", cancelled, true, false, session.Stopped},
		{"display loss alone", cancelled, false, false, session.Error},
		{"answer failure", session.Result{Code: session.OK, Run: hostrpc.Run{Status: hostrpc.StatusReturned, Result: "answer"}}, true, true, session.Error},
		{"non-cancellation stop", session.Result{Code: session.Stopped, Run: hostrpc.Run{Status: hostrpc.StatusStopped, StopReason: "budget"}}, true, false, session.Error},
		{"abandoned cleanup", session.Result{Code: session.Error, Interrupted: true, Err: context.Canceled}, true, false, session.Error},
		{"uncertain admission", session.Result{Code: session.Error, Uncertain: true}, true, false, session.Error},
		{"unresolved effect", session.Result{Code: session.Resolution, Run: hostrpc.Run{Status: hostrpc.StatusNeedsResolution}}, true, false, session.Error},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, progressFailed := range []bool{false, true} {
				var displayErr error
				var diagnostic io.Writer = failedDiagnostic{os.ErrDeadlineExceeded}
				if progressFailed {
					displayErr = os.ErrDeadlineExceeded
					diagnostic = io.Discard
				}
				var out io.Writer = io.Discard
				answerErr := errors.New("answer write failed")
				if tc.answerFails {
					out = failedDiagnostic{answerErr}
				}
				code, err, stop := finishInput(tc.result, "input", "session", out, diagnostic, displayErr, tc.explicitStop)
				wantErr := error(os.ErrDeadlineExceeded)
				if tc.answerFails {
					wantErr = answerErr
				}
				if code != tc.want || !errors.Is(err, wantErr) || !stop {
					t.Fatalf("progress failed=%v: exit=%d err=%v stop=%v; want exit=%d err=%v", progressFailed, code, err, stop, tc.want, wantErr)
				}
			}
		})
	}
}

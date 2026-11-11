//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

var listening = regexp.MustCompile(`canti host: serving workspace .* as process (\d+)`)

func (f *fixture) hostPIDs() []int {
	f.t.Helper()
	data, _ := os.ReadFile(filepath.Join(f.state, "host.log"))
	var pids []int
	for _, m := range listening.FindAllStringSubmatch(string(data), -1) {
		pid, _ := strconv.Atoi(m[1])
		pids = append(pids, pid)
	}
	return pids
}

func (f *fixture) hostProcesses() []int {
	f.t.Helper()
	fixtureExe := binary(f.t, "CANTI_ACCEPTANCE_RUNTIME_FIXTURE")
	productionExe := binary(f.t, "CANTI_ACCEPTANCE_RUNTIME")
	entries, err := os.ReadDir("/proc")
	if err != nil {
		f.t.Fatal(err)
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		process := filepath.Join("/proc", entry.Name())
		if target, err := os.Readlink(filepath.Join(process, "exe")); err != nil || target != fixtureExe && target != productionExe {
			continue
		}
		data, err := os.ReadFile(filepath.Join(process, "cmdline"))
		args := strings.Split(string(data), "\x00")
		if err == nil && len(args) > 1 && args[1] == "host" && slicesContain(args, f.state) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func (f *fixture) autoHost() {
	f.t.Cleanup(func() {
		pids := f.hostProcesses()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGTERM)
		}
		for _, pid := range pids {
			for deadline := time.Now().Add(20 * time.Second); syscall.Kill(pid, 0) == nil; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(deadline) {
					syscall.Kill(pid, syscall.SIGKILL)
					f.t.Errorf("host %d did not exit after SIGTERM", pid)
					break
				}
			}
		}
	})
}

func (f *fixture) socket() string { return filepath.Join(f.state, hostrpc.SocketName) }

func TestTUIAutoClientWaitsForItsStartingHost(t *testing.T) {
	for _, command := range []string{tuiCommand} {
		t.Run(command, func(t *testing.T) {
			f := newFixture(t, nil)
			f.autoHost()
			marker := filepath.Join(t.TempDir(), "starting")
			t.Cleanup(func() { os.Remove(marker) })
			args := []string{"-host", "auto", "-host-wait", "200ms"}
			if command == "run" {
				args = append(args, "-goal", "g")
			}
			_, wait := f.start([]string{"CANTI_TEST_SETUP=point:host.starting", "CANTI_TEST_BLOCKED=" + marker}, command, args...)
			waitFor(t, func() bool { _, err := os.Stat(marker); return err == nil })
			if r := wait(); r.code != exitError {
				t.Fatalf("startup deadline: %+v", r)
			}
			pids := f.hostProcesses()
			if len(pids) != 1 {
				t.Fatalf("one client left %d pending startup processes: %v", len(pids), pids)
			}
			if err := os.Remove(marker); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				h, err := (&hostrpc.Client{Path: f.socket()}).Hello(context.Background())
				return err == nil && h.PID == pids[0]
			})
			if r := f.cli(nil, "status"); r.code != exitError || !strings.Contains(r.stderr, "no runs") {
				t.Fatalf("startup admitted an input: %+v", r)
			}
		})
	}
}

func TestTUIAutoClientReplacesItsExitedStartingHost(t *testing.T) {
	for _, command := range []string{tuiCommand} {
		t.Run(command, func(t *testing.T) {
			f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "done"}})
			f.autoHost()
			marker := filepath.Join(t.TempDir(), "starting")
			t.Cleanup(func() { os.Remove(marker) })
			args := []string{"-host", "auto"}
			var input io.Reader
			if command == "run" {
				args = append(args, "-goal", "g")
			} else {
				input = strings.NewReader("g\n")
			}
			_, wait := f.startWith(input, []string{"CANTI_TEST_SETUP=point:host.starting", "CANTI_TEST_BLOCKED=" + marker}, command, args...)
			markedPID := func() int { data, _ := os.ReadFile(marker); pid, _ := strconv.Atoi(string(data)); return pid }
			var first, second int
			waitFor(t, func() bool { first = markedPID(); return first > 0 })
			if err := syscall.Kill(first, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { second = markedPID(); return second > 0 && second != first })
			if err := os.Remove(marker); err != nil {
				t.Fatal(err)
			}
			if r := wait(); r.code != exitOK || r.stdout != "done\n" {
				t.Fatalf("replacement startup: %+v", r)
			}
			if pids := f.hostProcesses(); len(pids) != 1 || pids[0] != second {
				t.Fatalf("replacement hosts=%v, want only %d", pids, second)
			}
			if pids := f.hostPIDs(); len(pids) != 1 || pids[0] != second {
				t.Fatalf("serving hosts=%v, want only %d", pids, second)
			}
			if got := f.status(); got.Phase != "returned" || len(f.model.requests()) != 1 {
				t.Fatalf("replacement admitted unexpected work: %+v", got)
			}
		})
	}
}

// The proxy loses the answer only after the real host has durably accepted it.
// Retrying must reconcile that InputID, even if the run returned before retry.
func TestTUIReplaysLostSubmitAcknowledgementWithoutAnotherRun(t *testing.T) {
	f := newFixture(t, map[int]decision{1: {Kind: "return", Payload: "answer"}})
	f.autoHost()
	if r := f.cli(nil, "run", "-host", "auto", "-goal", "initialize host"); r.code != exitOK {
		t.Fatal(r)
	}
	socket := f.socket()
	upstream := filepath.Join(f.state, "upstream.sock")
	if err := os.Rename(socket, upstream); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var mu sync.Mutex
	var ids []string
	var connections sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				var request hostrpc.Request
				if json.NewDecoder(conn).Decode(&request) != nil {
					return
				}
				remote, err := net.DialTimeout("unix", upstream, time.Second)
				if err != nil {
					return
				}
				defer remote.Close()
				remote.SetDeadline(time.Now().Add(3 * time.Second))
				if json.NewEncoder(remote).Encode(request) != nil {
					return
				}
				var response hostrpc.Response
				if json.NewDecoder(remote).Decode(&response) != nil {
					return
				}
				if request.Op == hostrpc.OpSubmit {
					mu.Lock()
					ids = append(ids, request.Submit.InputID)
					mu.Unlock()
					if calls.Add(1) == 1 {
						return
					}
				}
				json.NewEncoder(conn).Encode(response)
			}()
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done; connections.Wait() })
	before := len(f.model.requests())
	_, wait := f.startWith(strings.NewReader("one accepted input\n"), nil, tuiCommand, "-host", "auto")
	if r := wait(); r.code != exitOK || r.stdout != "answer\n" {
		t.Fatalf("lost ACK client: %+v", r)
	}
	if s := f.status(); s.Run.InputID == "" || s.Run.Ordinal != 2 || s.Run.Goal != "one accepted input" {
		t.Fatalf("replayed admission: %+v", s)
	}
	if len(f.model.requests()) != before+1 {
		t.Fatal("lost ACK dispatched the model again")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("submission identities: %q", ids)
	}
}

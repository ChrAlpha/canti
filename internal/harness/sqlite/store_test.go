package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

var limits = core.Limits{MaxSteps: 3, MaxAttempts: 2}

func tempPath(t testing.TB) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "canti.db")
}

func open(t testing.TB, path string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func testRun(id string) harness.Run {
	return harness.Run{
		ID:          id,
		Goal:        "list the workspace",
		Constraints: "read only",
		Budget:      harness.Budget{WallTime: time.Minute, Tokens: 1000, OutputBytes: 1 << 20, JobTimeout: 10 * time.Second},
		Created:     time.Date(2026, 9, 23, 10, 30, 0, 123456789, time.FixedZone("UTC+8", 8*3600)),
		Config:      []byte(`{"executor":"stub"}`),
	}
}

func fresh(t testing.TB, id string, limits core.Limits) core.State {
	t.Helper()
	state, err := core.NewState(id, limits)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func createRun(t testing.TB, s *Store, id string, limits core.Limits) harness.Snapshot {
	t.Helper()
	initial := fresh(t, id, limits)
	if err := s.CreateRun(context.Background(), testRun(id), initial); err != nil {
		t.Fatalf("CreateRun(%q) error = %v", id, err)
	}
	return harness.Snapshot{State: initial}
}

func sameRun(got, want harness.Run) bool {
	if !got.Created.Equal(want.Created) {
		return false
	}
	want.Created = got.Created
	return reflect.DeepEqual(got, want)
}

func TestOpenConfiguresDurableJournal(t *testing.T) {
	s := open(t, tempPath(t))
	want := map[string]string{
		"journal_mode": "wal",
		"synchronous":  "2",
		"foreign_keys": "1",
		"busy_timeout": "5000",
		"user_version": "1",
	}
	for pragma, value := range want {
		var got string
		if err := s.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s error = %v", pragma, err)
		}
		if got != value {
			t.Errorf("PRAGMA %s = %q, want %q", pragma, got, value)
		}
	}
}

func TestOpenRefusesNewerSchema(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA user_version = 2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(ctx, path); !errors.Is(err, ErrIncompatible) {
		if err == nil {
			s.Close()
		}
		t.Fatalf("Open() error = %v, want ErrIncompatible", err)
	}
}

func TestCreateRunRequiresFreshInitialState(t *testing.T) {
	ctx := context.Background()
	s := open(t, tempPath(t))
	initial := fresh(t, "run-1", limits)
	frozen, err := initial.Next(core.RequestFrozen{})
	if err != nil {
		t.Fatal(err)
	}
	later, revised := initial, initial
	later.Step = 2
	revised.RequestRev = 1
	tests := []struct {
		name  string
		state core.State
	}{
		{"invalid", core.State{RunID: "run-1", Phase: core.PhaseIdle}},
		{"other run", fresh(t, "run-2", limits)},
		{"frozen", frozen},
		{"later step", later},
		{"revised request", revised},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.CreateRun(ctx, testRun("run-1"), tt.state); err == nil {
				t.Fatal("CreateRun() succeeded, want error")
			}
		})
	}
	if err := s.CreateRun(ctx, testRun("run-1"), core.State{}); !errors.Is(err, core.ErrInvalidState) {
		t.Fatalf("CreateRun(zero state) error = %v, want ErrInvalidState", err)
	}
	if _, err := s.Run(ctx, "run-1"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Run() error = %v, want ErrNotFound after rejected creates", err)
	}
	if _, err := s.LastRun(ctx); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("LastRun() error = %v, want ErrNotFound", err)
	}
}

func TestCreateRunIsIdempotentAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	run, initial := testRun("run-1"), fresh(t, "run-1", limits)
	for i := range 2 {
		if err := s.CreateRun(ctx, run, initial); err != nil {
			t.Fatalf("CreateRun() #%d error = %v", i+1, err)
		}
	}
	sameInstant := run
	sameInstant.Created = run.Created.UTC()
	if err := s.CreateRun(ctx, sameInstant, initial); err != nil {
		t.Fatalf("CreateRun(same instant in UTC) error = %v", err)
	}
	changed := run
	changed.Goal = "another goal"
	if err := s.CreateRun(ctx, changed, initial); err == nil {
		t.Fatal("CreateRun(changed goal) succeeded, want conflict")
	}
	if err := s.CreateRun(ctx, run, fresh(t, "run-1", core.Limits{MaxSteps: 9, MaxAttempts: 2})); err == nil {
		t.Fatal("CreateRun(changed limits) succeeded, want conflict")
	}
	second := testRun("run-2")
	second.Created = run.Created.Add(-time.Hour)
	if err := s.CreateRun(ctx, second, fresh(t, "run-2", limits)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path)
	got, err := s.Run(ctx, "run-1")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !sameRun(got, run) {
		t.Fatalf("Run() = %#v, want %#v", got, run)
	}
	last, err := s.LastRun(ctx)
	if err != nil {
		t.Fatalf("LastRun() error = %v", err)
	}
	if !sameRun(last, second) {
		t.Fatalf("LastRun() = %#v, want the run created last %#v", last, second)
	}
	if _, err := s.Run(ctx, "missing"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("Run(missing) error = %v, want ErrNotFound", err)
	}
}

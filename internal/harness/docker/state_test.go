package docker

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLatchIsSetOnce(t *testing.T) {
	dir := t.TempDir()
	if got, err := readLatch(dir); got != "" || err != nil {
		t.Fatalf("readLatch() = %q, %v; want unset", got, err)
	}
	if err := setLatch(dir, latchStart); err != nil {
		t.Fatal(err)
	}
	if err := setLatch(dir, latchCancel); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("second setLatch() = %v, want ErrExist", err)
	}
	if got, err := readLatch(dir); got != latchStart || err != nil {
		t.Fatalf("readLatch() = %q, %v; want %q", got, err, latchStart)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "latch" {
		t.Fatalf("latch dir holds %v, %v; want only the latch", entries, err)
	}
}

func TestLatchHasOneConcurrentWinner(t *testing.T) {
	dir := t.TempDir()
	values := []string{latchStart, latchCancel}
	errs := make([]error, 32)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = setLatch(dir, values[i%2]) })
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		switch {
		case err == nil && winner >= 0:
			t.Fatalf("setLatch %d and %d both won", winner, i)
		case err == nil:
			winner = i
		case !errors.Is(err, fs.ErrExist):
			t.Fatalf("setLatch %d = %v, want ErrExist", i, err)
		}
	}
	if got, err := readLatch(dir); winner < 0 || got != values[winner%2] || err != nil {
		t.Fatalf("readLatch() = %q, %v; winner %d", got, err, winner)
	}
}

func TestReadLatchRejectsCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "latch"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLatch(dir); err == nil {
		t.Fatal("readLatch() accepted an empty latch")
	}
}

func TestMarkKeepsFirstRequest(t *testing.T) {
	f := newFake(t)
	dir := f.x.dir("e")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.x.fence(f.effect()); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(filepath.Join(dir, "cancel"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := f.x.fence(f.effect()); err != nil {
		t.Fatal(err)
	}
	if again, err := os.ReadFile(filepath.Join(dir, "cancel")); err != nil || string(again) != string(first) {
		t.Fatalf("marker = %q, %v; want %q", again, err, first)
	}
}

func TestFlockHonorsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	unlock, err := flock(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	began := time.Now()
	if _, err := flock(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended flock() = %v, want DeadlineExceeded", err)
	}
	if waited := time.Since(began); waited > time.Second {
		t.Fatalf("contended flock() waited %v", waited)
	}
	unlock()
	again, err := flock(t.Context(), path)
	if err != nil {
		t.Fatalf("flock() after unlock = %v", err)
	}
	again()
}

func TestLockRejectsDoneContextAndBadIDs(t *testing.T) {
	x := newTestExecutor(t, Config{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := x.lock(ctx, "effect_1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("lock(done) = %v, want Canceled", err)
	}
	for _, id := range []string{"", ".", "..", "a/b", "a b", "a:b"} {
		if _, err := x.lock(t.Context(), id); err == nil {
			t.Fatalf("lock(%q) accepted an invalid effect ID", id)
		}
	}
	if _, err := os.Stat(filepath.Join(x.cfg.State, "jobs", "effect_1")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lock(done) created state: %v", err)
	}
}

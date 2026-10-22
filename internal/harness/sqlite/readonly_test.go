package sqlite

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func readOnly(t testing.TB, path string) *Store {
	t.Helper()
	s, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenReadOnlyDoesNotCreateMissingDatabase(t *testing.T) {
	path := tempPath(t)
	if s, err := OpenReadOnly(context.Background(), path); err == nil {
		s.Close()
		t.Fatal("OpenReadOnly(missing) succeeded")
	}
	files, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(files) != 0 {
		t.Fatalf("directory after failed open = %v, %v; want empty", files, err)
	}
}

func TestOpenReadOnlyPreservesCurrentSchemaAndReadonlyFile(t *testing.T) {
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", limits))
	d.freeze()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	s := readOnly(t, path)
	var version, tables, queryOnly int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'session_runs'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA query_only`).Scan(&queryOnly); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion || tables != 1 || queryOnly != 1 {
		t.Fatalf("version = %d, session tables = %d, query_only = %d", version, tables, queryOnly)
	}
	got, err := s.Request(context.Background(), "run-1", 1, 1)
	if err != nil || !bytes.Equal(got.Body, d.commits[0].Facts.Request.Body) {
		t.Fatalf("Request = %#v, %v", got, err)
	}
	page, err := s.ContextRecords(context.Background(), "run-1", 0, 0, 0, 1)
	if err != nil || page.AsOfSeq != 1 || page.Request.Seq != 1 || len(page.Events) != 0 {
		t.Fatalf("legacy context = %#v, %v", page, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("database changed on read-only open: %v", err)
	}
}

func TestOpenReadOnlyRejectsWritesIncludingIdempotentWrites(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	w := open(t, path)
	run, initial := testRun("run-1"), fresh(t, "run-1", limits)
	if err := w.CreateRun(ctx, run, initial); err != nil {
		t.Fatal(err)
	}
	d := newDriver(t, w, harness.Snapshot{State: initial})
	d.freeze()
	s := readOnly(t, path)
	session := testRun("run-2")
	session.SessionID, session.InputID = "session", "input"
	for name, write := range map[string]func() error{
		"create": func() error { return s.CreateRun(ctx, run, initial) },
		"commit": func() error { return s.Commit(ctx, d.commits[0]) },
		"submit": func() error {
			_, err := s.Submit(ctx, session, fresh(t, session.ID, limits))
			return err
		},
	} {
		if err := write(); !errors.Is(err, ErrReadOnly) {
			t.Errorf("%s error = %v, want ErrReadOnly", name, err)
		}
	}
	s.db.SetMaxIdleConns(0)
	if _, err := s.db.Exec(`DELETE FROM transitions`); err == nil {
		t.Fatal("raw write on a new connection succeeded")
	}
	unchanged(t, w, run.ID, d.commits, harness.Totals{})
}

func TestOpenReadOnlySeesLiveWALAndDoesNotBlockWriter(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", limits))
	d.freeze()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	s := readOnly(t, path)
	if got := latestOf(t, s, "run-1"); got != d.snap {
		t.Fatalf("WAL snapshot = %#v, want %#v", got, d.snap)
	}
	tr := derive(t, d.snap, core.AttemptStarted{Step: 1, Attempt: 1}, harness.Facts{})
	if err := w.commit(ctx, tx, tr); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := latestOf(t, s, "run-1"); got.Seq != tr.Seq || got.State != tr.To {
		t.Fatalf("new WAL commit = %#v, want seq %d", got, tr.Seq)
	}
}

func TestOpenReadOnlyReadsWALWithReadonlyDirectoryAndExistingSidecars(t *testing.T) {
	path := tempPath(t)
	w := open(t, path)
	d := newDriver(t, w, createRun(t, w, "run-1", limits))
	d.freeze()
	files := []string{path, path + "-wal", path + "-shm"}
	t.Cleanup(func() {
		os.Chmod(filepath.Dir(path), 0700)
		for _, name := range files {
			os.Chmod(name, 0600)
		}
	})
	for _, name := range files {
		if err := os.Chmod(name, 0400); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Dir(path), 0500); err != nil {
		t.Fatal(err)
	}
	s := readOnly(t, path)
	page, err := s.ContextRecords(context.Background(), "run-1", 0, 0, 0, 1)
	if err != nil || page.AsOfSeq != d.snap.Seq || !bytes.Equal(page.Request.Facts.Request.Body, d.commits[0].Facts.Request.Body) {
		t.Fatalf("read-only WAL context: %#v, %v", page, err)
	}
}

func TestOpenReadOnlyRejectsUnknownSchemaAndCancelledContext(t *testing.T) {
	path := tempPath(t)
	w := open(t, path)
	if _, err := w.db.Exec(`PRAGMA user_version = 4`); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenReadOnly(context.Background(), path); !errors.Is(err, ErrIncompatible) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("unknown schema error = %v, want ErrIncompatible", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := OpenReadOnly(ctx, path); !errors.Is(err, context.Canceled) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("cancelled open error = %v, want context.Canceled", err)
	}
}

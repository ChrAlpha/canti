package main

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestChatOutputBackpressureIsBounded(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	fillPipe(t, w)
	failed := make(chan error, 1)
	out, restore, err := newChatOutput(w, failed)
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	start := time.Now()
	if _, err := io.WriteString(out, "blocked"); err == nil {
		t.Fatal("blocked output succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("display blocked shutdown")
	}
	select {
	case <-failed:
	default:
		t.Fatal("missing failure notification")
	}
	start = time.Now()
	if _, err := out.Write([]byte("again")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("sticky failure: %v", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("failed display retried")
	}
}

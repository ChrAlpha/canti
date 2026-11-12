package pipe

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
)

func TestQueuedSignalPrecedesRequestingAnEOFReadyPipe(t *testing.T) {
	inputFile, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()
	writer.Close() // EOF is ready before Next is called.
	reader, err := newInputReader(inputFile, io.Discard, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	signals := make(chan os.Signal, 1)
	signals <- os.Interrupt
	entry, code, err := reader.Next(t.Context(), signals, nil)
	if err != nil || code != chat.Stopped || entry.err != nil {
		t.Fatalf("signal lost to EOF: entry=%+v code=%d err=%v", entry, code, err)
	}
	// Once the signal is handled, the same reader still reports its normal EOF.
	entry, code, err = reader.Next(t.Context(), signals, nil)
	if err != nil || code != chat.OK || !errors.Is(entry.err, io.EOF) {
		t.Fatalf("EOF after handled signal: entry=%+v code=%d err=%v", entry, code, err)
	}
}

func TestQueuedSignalDominatesAlreadyDeliveredInputAndEOF(t *testing.T) {
	for _, entry := range []input{{err: io.EOF}, {text: "must not submit", err: io.EOF}, {text: "must not submit"}} {
		// Both channels are populated before arbitration begins. Repeat to exercise
		// either ready-select branch; the required result is independent of schedule.
		for range 128 {
			reader := &inputReader{inputs: make(chan input, 1)}
			reader.inputs <- entry
			signals := make(chan os.Signal, 1)
			signals <- os.Interrupt
			got, code, err := reader.await(context.Background(), signals, nil)
			if err != nil || code != chat.Stopped || got.text != "" || got.err != nil {
				t.Fatalf("completed input bypassed queued signal: entry=%+v code=%d err=%v", got, code, err)
			}
		}
	}
}

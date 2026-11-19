package docker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

type slowReader struct{}

func (slowReader) Read([]byte) (int, error) {
	time.Sleep(20 * time.Millisecond)
	return 0, io.EOF
}

func TestReadExecStreamStopsAtTheContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	var out, errOut bytes.Buffer
	if err := readExecStream(ctx, slowReader{}, &out, &errOut, 1024); err == nil {
		t.Fatal("stream read ignored its deadline")
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/ChrAlpha/canti/internal/hostrpc"
)

func inspectCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	fs.StringVar(&a.hostMode, "host", hostAuto, "where to inspect: auto uses a serving host or inspects locally, embedded inspects in this process")
	return func(ctx context.Context) (int, error) {
		if a.hostMode != hostAuto && a.hostMode != hostEmbedded {
			return exitUsage, fmt.Errorf("-host must be %s or %s", hostEmbedded, hostAuto)
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if a.remoteMode() {
			path, err := hostrpc.SocketPath(a.state)
			if err != nil {
				return exitError, err
			}
			sample, err := (&hostrpc.Client{Path: path}).Inspect(call, a.runID)
			if err == nil {
				return exitOK, a.encoder().Encode(sample)
			}
			if !hostrpc.Unavailable(err) {
				return exitError, err
			}
		}
		// A cold inspection never starts an autonomous Host: startup ResumeOpen
		// would otherwise finish pending stops and commit Run progress.
		h, err := a.openHost(call, false)
		if err != nil {
			return exitError, err
		}
		defer h.Close()
		sample, err := h.Inspect(call, a.runID)
		if err != nil {
			return exitError, err
		}
		return exitOK, a.encoder().Encode(hostrpc.InspectionOf(sample))
	}
}

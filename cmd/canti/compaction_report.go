package main

import (
	"context"
	"errors"
	"flag"
	"io"

	"github.com/ChrAlpha/canti/internal/harness"
)

func compactionsCommand(fs *flag.FlagSet, a *app) action {
	runFlag(fs, a)
	var id string
	var after int64
	var limit int
	var raw, bodies bool
	fs.StringVar(&id, "id", "", "inspect this compaction ID")
	fs.Int64Var(&after, "after", 0, "read operations after this diagnostic sequence")
	fs.IntVar(&limit, "limit", 20, "maximum operations in the page (1..100, also bounded by persisted bytes)")
	fs.BoolVar(&raw, "raw", false, "write exact frozen summary request bytes; requires -id")
	fs.BoolVar(&bodies, "bodies", false, "include frozen request and response bytes as base64")
	return func(ctx context.Context) (int, error) {
		if after < 0 || limit < 1 || limit > 100 || raw && (id == "" || bodies) || id != "" && after != 0 {
			return exitUsage, errors.New("invalid compaction pagination or body flags")
		}
		store, run, _, err := a.read(ctx)
		if err != nil {
			return exitError, err
		}
		defer store.Close()
		if id != "" {
			c, err := store.Compaction(ctx, run.ID, id)
			if err != nil {
				return exitError, err
			}
			if raw {
				return a.writeCompactionRequest(c)
			}
			if !bodies {
				c.OmitBodies()
			}
			return exitOK, a.encoder().Encode(c)
		}
		records, err := store.Compactions(ctx, run.ID, after, limit)
		if err != nil {
			return exitError, err
		}
		next := int64(0)
		for i := range records {
			next = records[i].Sequence
			if !bodies {
				records[i].Operation.OmitBodies()
			}
		}
		return exitOK, a.encoder().Encode(map[string]any{"run_id": run.ID, "records": records, "next_after": next})
	}
}

func (a *app) writeCompactionRequest(c harness.Compaction) (int, error) {
	n, err := a.stdout.Write(c.Plan.Request.Body)
	if err == nil && n != len(c.Plan.Request.Body) {
		err = io.ErrShortWrite
	}
	return exitOK, err
}

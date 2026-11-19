// canti-job is the one-shot capture helper used by prepared environments.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/jobrunner"
	"github.com/ChrAlpha/canti/runtime/internal/project"
)

func main() { os.Exit(run(os.Args[1:], true)) }
func run(args []string, exitOnDeadline bool) int {
	fs := flag.NewFlagSet("canti-job", flag.ContinueOnError)
	var c jobrunner.Config
	version := fs.Bool("version", false, "print helper protocol")
	projectRead := fs.Bool("project-read", false, "read the workspace project context")
	root := fs.String("root", "", "workspace mount point for -project-read")
	mounts := fs.String("mounts", "", "bounded JSON mount metadata for -project-read")
	deadline := fs.Duration("deadline", 9*time.Second, "project read deadline")
	fs.StringVar(&c.Directory, "directory", "", "command evidence directory")
	fs.StringVar(&c.EffectID, "effect", "", "accepted effect ID")
	fs.StringVar(&c.Spec, "spec", "", "accepted specification")
	fs.Uint64Var(&c.Generation, "generation", 0, "command generation")
	fs.Int64Var(&c.OutputLimit, "output-limit", 8<<20, "retained output bytes")
	fs.IntVar(&c.PreviewLimit, "preview-limit", 4096, "preview bytes")
	fs.StringVar(&c.Workdir, "workdir", "", "directory the command shell enters")
	fs.StringVar(&c.Init, "init", "", "optional shell initialization file")
	if err := fs.Parse(args); err != nil {
		return 125
	}
	if *version {
		fmt.Println(jobrunner.Protocol)
		return 0
	}
	if *projectRead {
		if fs.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "canti-job: -project-read takes -root and no command")
			return 125
		}
		return runProjectRead(*root, c.Workdir, *mounts, *deadline, exitOnDeadline)
	}

	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "canti-job: exactly one command is required")
		return 125
	}
	c.Command = fs.Arg(0)
	rec, err := jobrunner.Run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "canti-job:", err)
		return 125
	}
	return *rec.ExitCode
}

func runProjectRead(root, workdir, mounts string, deadline time.Duration, exitOnDeadline bool) int {
	if root == "" {
		fmt.Fprintln(os.Stderr, "canti-job: -project-read takes -root and no command")
		return 125
	}
	if workdir == "" {
		workdir = root
	}
	var visible []project.ContainerMount
	if mounts != "" {
		if err := json.Unmarshal([]byte(mounts), &visible); err != nil {
			fmt.Fprintln(os.Stderr, "canti-job: invalid -mounts:", err)
			return 125
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	stopWatchdog := watchProjectDeadline(ctx, exitOnDeadline, os.Exit)
	defer stopWatchdog()
	snapshot, err := project.ReadContainer(ctx, project.ContainerRequest{Root: root, Workdir: workdir, Mounts: visible})
	if err == nil {
		snapshot, err = snapshot.FitEncoded(project.MaxSnapshotEncodedBytes)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "canti-job:", err)
		return 125
	}
	if deadline, ok := ctx.Deadline(); ok {
		// The watchdog below bounds a stream type that cannot take a deadline.
		_ = os.Stdout.SetWriteDeadline(deadline)
	}
	if err := json.NewEncoder(os.Stdout).Encode(snapshot); err != nil {
		fmt.Fprintln(os.Stderr, "canti-job:", err)
		return 125
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "canti-job:", err)
		return 125
	}
	return 0
}

// watchProjectDeadline exits only when ctx's own deadline expires. The stop
// function closes the watchdog and joins it, so normal completion cannot race
// with a canceled context and exit after a successful read.
func watchProjectDeadline(ctx context.Context, enabled bool, exit func(int)) func() {
	if !enabled {
		return func() {}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				exit(125)
			}
		case <-stop:
		}
	}()
	return func() {
		once.Do(func() { close(stop) })
		wg.Wait()
	}
}

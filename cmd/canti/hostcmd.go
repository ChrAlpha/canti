package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/internal/harness/docker"
	"github.com/ChrAlpha/canti/internal/host"
	"github.com/ChrAlpha/canti/internal/hostrpc"
)

// The points the host command reports to checkpoint.
const (
	pointHostWaiting = "host.waiting"   // the first Hello failed, before waiting for a host
	pointLocked      = "host.locked"    // the host holds the lock, before it listens
	pointListening   = "host.listening" // the host listens, before it serves
)

func hostCommand(fs *flag.FlagSet, a *app) action {
	idle := fs.Duration("idle", 0, "shut down after this `long` without a client, a drive, or a stop; 0 serves until a signal or -stop")
	shutdown := fs.Duration("shutdown-timeout", 30*time.Second, "`deadline` for the drives to end at shutdown; past it the process exits holding the lock")
	stopHost := fs.Bool("stop", false, "ask the host serving the state directory to shut down")
	return func(ctx context.Context) (int, error) {
		path, err := hostrpc.SocketPath(a.state)
		if err != nil {
			return exitError, err
		}
		if *stopHost {
			if err := (&hostrpc.Client{Path: path}).ShutdownHost(ctx); err != nil {
				if hostrpc.Unavailable(err) {
					err = fmt.Errorf("no host serves workspace %s", a.workspace)
				}
				return exitError, err
			}
			return exitOK, nil
		}
		if *idle < 0 || *shutdown <= 0 {
			return exitUsage, errors.New("-idle must not be negative and -shutdown-timeout must be positive")
		}
		if err := docker.CheckLayout(a.workspace, a.state); err != nil {
			return exitError, err
		}
		if err := os.MkdirAll(a.state, 0o700); err != nil {
			return exitError, err
		}
		if err := a.bindCredentials(); err != nil {
			return exitError, err
		}
		return a.serve(ctx, path, *idle, *shutdown)
	}
}

// serve runs the host of the state directory on the socket at path until a
// signal, a client's request, or idleness ends it. The lock decides between
// hosts that start together: only its holder removes a stale socket and
// listens.
func (a *app) serve(ctx context.Context, path string, idle, shutdown time.Duration) (int, error) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	h, err := a.openHost(ctx, true)
	if err != nil {
		return exitError, err
	}
	checkpoint(pointLocked)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		h.Close()
		return exitError, err
	}
	ln, err := net.Listen("unix", path)
	if err == nil {
		err = os.Chmod(path, 0o600)
	}
	if err != nil {
		h.Close()
		return exitError, err
	}
	checkpoint(pointListening)
	fmt.Fprintf(a.stderr, "canti host: serving workspace %s on %s as process %d\n", a.workspace, path, os.Getpid())
	done := make(chan struct{}, 1)
	srv := &hostrpc.Server{Host: h, Version: version, Logger: a.log, Shutdown: func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}}
	serving := make(chan error, 1)
	go func() { serving <- srv.Serve(ctx, ln) }()
	go a.resumeOpen(h)
	var tick <-chan time.Time
	if idle > 0 {
		t := time.NewTicker(min(idle/4+time.Millisecond, time.Second))
		defer t.Stop()
		tick = t.C
	}
	var served error
wait:
	for {
		select {
		case s := <-signals:
			a.log.Info("host shutting down", "signal", s.String())
			break wait
		case <-done:
			a.log.Info("host shutting down at a client's request")
			break wait
		case <-tick:
			if srv.Idle(idle) {
				a.log.Info("host shutting down after being idle", "idle", idle)
				break wait
			}
		case served = <-serving:
			a.log.Warn("host shutting down: accepting connections failed", "error", served)
			break wait
		}
	}
	return a.shutdownHost(srv, h, serving, served, shutdown)
}

// shutdownHost ends the server and then the host within one deadline: the
// server stops accepting, removes its socket, and ends every request and
// connection; the host ends its drives and operations, every request among
// them, and only once all of them have ended releases the state directory.
// Past the deadline, the process exits holding the lock, which the kernel
// releases when it dies.
func (a *app) shutdownHost(srv *hostrpc.Server, h *host.Host, serving <-chan error, served error, timeout time.Duration) (int, error) {
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		return exitError, fmt.Errorf("%w; the process exits holding the lock until it ends", err)
	}
	if served == nil {
		select {
		case served = <-serving:
		case <-ctx.Done():
			return exitError, fmt.Errorf("%w: connections did not end; the process exits", host.ErrNotQuiescent)
		}
	}
	if served != nil {
		return exitError, served
	}
	return exitOK, nil
}

// bindCredentials binds each model API key of the host's own environment,
// which credentials read and removed at start, to the provider that the
// API's base URL variable names at this moment, or else its adapter's
// default, and reports each pair without its key. A host that a client
// starts has no key in its environment and binds nothing.
func (a *app) bindCredentials() error {
	for _, api := range host.APINames {
		k := a.cred.For(api)
		if k == (host.Keys{}) {
			continue
		}
		p := cmp.Or(baseURL(api), host.DefaultProvider(api))
		if err := host.CheckProvider(p); err != nil {
			return err
		}
		if a.bound == nil {
			a.bound = map[host.Pair]host.Keys{}
		}
		a.bound[host.Pair{API: api, Provider: p}] = k
		fmt.Fprintf(a.stderr, "canti host: binds its %s credential to %s\n", api, p)
	}
	return nil
}

// resumeOpen continues the open run that the host can continue by itself:
// it finishes a pending stop, which needs no credential, within the stop
// cleanup deadline, or starts driving a run that one of the host's
// credentials is bound to. Every other open run waits for a client.
func (a *app) resumeOpen(h *host.Host) {
	ctx, cancel := context.WithTimeout(context.Background(), stopCleanup)
	defer cancel()
	if err := h.ResumeOpen(ctx); err != nil && !errors.Is(err, host.ErrClosing) {
		a.log.Warn("continuing the open run failed", "error", err)
	}
}

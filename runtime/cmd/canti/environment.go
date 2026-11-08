package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ChrAlpha/canti/runtime/internal/harness/docker"
)

func environmentCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	for _, name := range credentialVariables {
		os.Unsetenv(name)
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, "Usage: canti environment init|attach|inspect|stop|start|detach|retire -environment DIRECTORY [flags]")
		return exitUsage
	}
	operation := args[0]
	fs := flag.NewFlagSet("canti environment "+operation, flag.ContinueOnError)
	var parseErrors strings.Builder
	fs.SetOutput(&parseErrors)
	fs.Usage = func() {}
	directory := fs.String("environment", "", "host-local attachment directory; required")
	var helper, container, endpoint, init string
	switch operation {
	case "init":
		fs.StringVar(&helper, "helper", "", "canti-job executable; defaults beside canti")
	case "attach":
		fs.StringVar(&container, "container", "", "explicitly prepared running container name or ID; required")
		fs.StringVar(&endpoint, "docker-host", "", "local Unix Docker endpoint; defaults to selected Docker context")
		fs.StringVar(&init, "init", "", "absolute shell initialization file inside the environment")
	case "inspect", "stop", "start", "detach", "retire":
	default:
		fmt.Fprintln(stderr, "canti: unknown environment operation")
		return exitUsage
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(stdout, "Usage: canti environment %s [flags]\n\n", operation)
			fs.SetOutput(stdout)
			fs.PrintDefaults()
			return exitOK
		}
		fmt.Fprint(stderr, parseErrors.String())
		return exitUsage
	}
	if fs.NArg() != 0 || *directory == "" {
		fmt.Fprintln(stderr, "canti: -environment DIRECTORY is required and positional arguments are not accepted")
		return exitUsage
	}
	dir, err := filepath.Abs(*directory)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	call, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	encoder := json.NewEncoder(stdout)
	switch operation {
	case "init":
		err = initializeCLIEnvironment(dir, helper, encoder)
	case "attach":
		if container == "" {
			fmt.Fprintln(stderr, "canti: -container is required")
			return exitUsage
		}
		if endpoint == "" {
			endpoint, err = selectedDockerEndpoint(call)
			if err != nil {
				break
			}
		}
		var attached docker.Attachment
		attached, err = docker.AttachEnvironment(call, dir, endpoint, container, init)
		if err == nil {
			err = encoder.Encode(map[string]any{"directory": attached.Directory, "container_id": attached.ContainerID, "daemon_id": attached.DaemonID, "incarnation": attached.Incarnation, "endpoint": attached.Endpoint})
		}
	default:
		var status docker.EnvironmentStatus
		status, err = docker.ControlEnvironment(call, dir, operation)
		if err == nil {
			err = encoder.Encode(status)
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	return exitOK
}

// Docker selection belongs to the CLI, not the host or resumed executor. Once
// resolved, only a credential-free local Unix endpoint enters stored wiring.
func selectedDockerEndpoint(ctx context.Context) (string, error) {
	selected := os.Getenv("DOCKER_CONTEXT")
	endpoint := os.Getenv("DOCKER_HOST")
	if selected != "" || endpoint == "" {
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		args := []string{"context", "inspect"}
		if selected != "" {
			args = append(args, selected)
		}
		args = append(args, "--format", "{{.Endpoints.docker.Host}}")
		command := exec.CommandContext(call, "docker", args...)
		command.WaitDelay = time.Second
		output := &boundedText{limit: 4096}
		command.Stdout = output
		command.Stderr = io.Discard
		if err := command.Run(); err != nil || output.overflow {
			return "", errors.New("cannot resolve Docker context; name a local endpoint with -docker-host")
		}
		endpoint = strings.TrimSpace(output.String())
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "unix" || u.Host != "" || u.User != nil || !filepath.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("persistent attachment requires a local Unix Docker endpoint")
	}
	return endpoint, nil
}

type boundedText struct {
	strings.Builder
	limit    int
	overflow bool
}

func (b *boundedText) Write(data []byte) (int, error) {
	n := min(len(data), max(0, b.limit-b.Len()))
	if n < len(data) {
		b.overflow = true
	}
	_, _ = b.Builder.Write(data[:n])
	return len(data), nil
}

func initializeCLIEnvironment(dir, helper string, encoder *json.Encoder) error {
	if helper == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		helper = filepath.Join(filepath.Dir(exe), "canti-job")
	}
	if err := docker.InitEnvironment(dir, helper); err != nil {
		return err
	}
	identity, err := docker.EnvironmentIdentity(dir)
	if err != nil {
		return err
	}
	return encoder.Encode(map[string]any{"directory": dir, "helper_protocol": 1, "authority_id": identity})
}

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnvironmentInitializationIsIndependentOfRunState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "control")
	helper := filepath.Join(t.TempDir(), "canti-job")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\necho 1\n"), 0555); err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	code := cli(context.Background(), []string{"environment", "init", "-environment", dir, "-helper", helper}, &out, &diagnostics)
	if code != exitOK || !strings.Contains(out.String(), `"helper_protocol":1`) {
		t.Fatalf("init=%d %s %s", code, out.String(), diagnostics.String())
	}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == "canti.db" {
			t.Fatal("environment init created a run journal")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestEnvironmentInvalidFlagsDoNotCreateState(t *testing.T) {
	for _, arguments := range [][]string{{"init"}, {"attach"}, {"start", "-container", "wrong"}, {"unknown"}} {
		dir := filepath.Join(t.TempDir(), "control")
		args := append([]string{"environment"}, arguments...)
		if len(arguments) > 1 || arguments[0] == "attach" {
			args = append(args, "-environment", dir)
		}
		var out, diagnostics bytes.Buffer
		if code := cli(context.Background(), args, &out, &diagnostics); code != exitUsage {
			t.Fatalf("%v = %d: %s", args, code, diagnostics.String())
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("invalid environment flags created directories")
		}
	}
}
func TestEnvironmentHelpDescribesExplicitRetirement(t *testing.T) {
	var out, diagnostics bytes.Buffer
	if code := cli(context.Background(), []string{"environment", "retire", "-h"}, &out, &diagnostics); code != exitOK || !strings.Contains(out.String(), "canti environment retire") || diagnostics.Len() != 0 {
		t.Fatalf("help=%d %s %s", code, out.String(), diagnostics.String())
	}
}
func TestEnvironmentEndpointSelectionPinsExplicitLocalHost(t *testing.T) {
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "unix:///task-owned.sock")
	endpoint, err := selectedDockerEndpoint(context.Background())
	if err != nil || endpoint != "unix:///task-owned.sock" {
		t.Fatalf("endpoint=%q: %v", endpoint, err)
	}
	t.Setenv("DOCKER_HOST", "tcp://remote.invalid:2375")
	if _, err = selectedDockerEndpoint(context.Background()); err == nil {
		t.Fatal("accepted remote endpoint for local authority")
	}
}

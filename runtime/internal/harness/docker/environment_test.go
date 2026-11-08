package docker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnvironmentInitializationPinsTheHelperAndNeverResetsAuthority(t *testing.T) {
	dir := t.TempDir()
	helper := filepath.Join(t.TempDir(), "canti-job")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\necho 1\n"), 0555); err != nil {
		t.Fatal(err)
	}
	if err := InitEnvironment(dir, helper); err != nil {
		t.Fatal(err)
	}
	m, s, err := readEnvironment(dir)
	if err != nil || s.Gate != "unattached" || m.HelperProtocol != 1 {
		t.Fatalf("init=%+v %+v: %v", m, s, err)
	}
	if err = InitEnvironment(dir, "/missing/new-helper"); err != nil {
		t.Fatalf("idempotent init read a new source: %v", err)
	}
	if err = os.WriteFile(filepath.Join(dir, "authority", "environment.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "authority", "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err = InitEnvironment(dir, helper); err == nil {
		t.Fatal("reset an existing attachment after losing its manifest")
	}
}
func TestEnvironmentInitializationRefusesAnUnsupportedHelper(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "wrong-helper")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\necho 2\n"), 0555); err != nil {
		t.Fatal(err)
	}
	if err := InitEnvironment(t.TempDir(), helper); err == nil {
		t.Fatal("accepted unsupported helper protocol")
	}
}

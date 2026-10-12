// Package dockertest puts a proxy between the docker CLI and the Docker
// daemon, so a test can answer a daemon request in the daemon's place.
package dockertest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Proxy returns a directory holding a docker CLI that reaches the daemon of
// the docker CLI on PATH through a proxy, where intercept answers a request
// when it reports true. The proxy speaks plain HTTP, so the test is skipped
// for a daemon reached other than over a unix socket or tcp, and whenever the
// environment or the current context sets TLS.
func Proxy(t testing.TB, intercept func(http.ResponseWriter, *http.Request) bool) string {
	t.Helper()
	docker, err := exec.LookPath("docker")
	if err == nil {
		docker, err = filepath.Abs(docker)
	}
	if err != nil {
		t.Fatal(err)
	}
	host, tls := os.Getenv("DOCKER_HOST"), os.Getenv("DOCKER_TLS_VERIFY")+os.Getenv("DOCKER_TLS") != ""
	if host == "" {
		out, err := exec.Command(docker, "context", "inspect", "--format", "{{.Endpoints.docker.Host}} {{len .TLSMaterial}} {{.Endpoints.docker.SkipTLSVerify}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		f := strings.Fields(string(out))
		if len(f) != 3 {
			t.Fatalf("dockertest: docker context inspect = %q", out)
		}
		host, tls = f[0], tls || f[1] != "0" || f[2] == "true"
	}
	network, addr, _ := strings.Cut(host, "://")
	switch {
	case network != "unix" && network != "tcp":
		t.Skipf("dockertest: only a daemon on a unix socket or tcp can be proxied, not %s", host)
	case tls:
		t.Skipf("dockertest: the proxy speaks plain HTTP, and TLS is set for %s", host)
	}
	upstream := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	proxy := &httputil.ReverseProxy{
		Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(&url.URL{Scheme: "http", Host: "docker"}) },
		Transport:     upstream,
		FlushInterval: -1,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !intercept(w, r) {
			proxy.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		upstream.CloseIdleConnections()
	})
	dir := t.TempDir()
	shim := fmt.Sprintf("#!/bin/sh\nexec '%s' -H tcp://%s \"$@\"\n", docker, srv.Listener.Addr())
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// Reply answers with status and body, a JSON document, and reports true.
func Reply(w http.ResponseWriter, status int, body string) bool {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	io.WriteString(w, body)
	return true
}

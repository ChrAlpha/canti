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
// when it reports true. A daemon reached other than over a unix socket or
// plain tcp cannot be proxied, and the test is skipped.
func Proxy(t testing.TB, intercept func(http.ResponseWriter, *http.Request) bool) string {
	t.Helper()
	docker, err := exec.LookPath("docker")
	if err == nil {
		docker, err = filepath.Abs(docker)
	}
	if err != nil {
		t.Fatal(err)
	}
	host := os.Getenv("DOCKER_HOST")
	if host == "" {
		out, err := exec.Command(docker, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
		if err != nil {
			t.Fatal(err)
		}
		host = strings.TrimSpace(string(out))
	}
	network, addr, _ := strings.Cut(host, "://")
	if network != "unix" && network != "tcp" || os.Getenv("DOCKER_TLS_VERIFY") != "" {
		t.Skipf("dockertest: only a daemon on a unix socket or plain tcp can be proxied, not %s", host)
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

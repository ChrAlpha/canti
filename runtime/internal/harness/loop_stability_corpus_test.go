//go:build integration

package harness_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

func stabilityCorpus(ctx context.Context, t *testing.T) (archive.Reader, []string, string) {
	t.Helper()
	manifest := os.Getenv("CANTI_STABILITY_CORPUS")
	if manifest == "" {
		root := t.TempDir()
		body := []byte("STABILITY_EVIDENCE_SENTINEL π\n" + strings.Repeat("line ```fence``` text.\n", 340))
		if err := os.WriteFile(filepath.Join(root, "evidence.txt"), body, 0600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(body)
		return archive.Reader{Imports: root}, []string{"evidence.txt"}, hex.EncodeToString(hash[:])
	}
	if unsafeStabilityPath(manifest) {
		t.Fatal("unsafe stability manifest path")
	}
	info, err := os.Lstat(manifest)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<20 {
		t.Fatalf("invalid stability manifest: %v", err)
	}
	file, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64<<20+1))
	if err != nil || len(data) > 64<<20 {
		t.Fatalf("stability manifest exceeds 64 MiB: %v", err)
	}
	manifestHash := sha256.Sum256(data)
	var c struct {
		Format string            `json:"format"`
		Root   string            `json:"root"`
		Hashes map[string]string `json:"hashes"`
	}
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c.Format != "retrieval-quality/1" || !filepath.IsAbs(c.Root) || len(c.Hashes) == 0 || len(c.Hashes) > 4096 || unsafeStabilityPath(c.Root) {
		t.Fatal("invalid stability corpus shape")
	}
	r := archive.Reader{Imports: c.Root}
	paths := make([]string, 0, len(c.Hashes))
	for name := range c.Hashes {
		if name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") || unsafeStabilityPath(name) {
			t.Fatal("unsafe stability source path")
		}
		paths = append(paths, name)
	}
	slices.Sort(paths)
	var total int64
	for _, name := range paths {
		h := sha256.New()
		var offset int64
		for {
			result, err := r.Retrieve(ctx, archive.RetrievalQuery{Operation: "read", Path: "/canti/imports/" + name, Reason: "verify frozen stability source", Offset: offset, Limit: 8192})
			if err != nil || result.Error != "" || len(result.Sources) != 1 {
				t.Fatalf("stability source read failed: %v", err)
			}
			source, text := result.Sources[0], result.Evidence[0]
			if source.Offset != offset || len(text) != source.Length {
				t.Fatal("stability source evidence does not match its range")
			}
			h.Write([]byte(text))
			total += int64(len(text))
			if total > 64<<20 {
				t.Fatal("stability sources exceed 64 MiB")
			}
			if !result.More {
				break
			}
			if result.NextOffset <= offset {
				t.Fatal("stability source cursor did not advance")
			}
			offset = result.NextOffset
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != c.Hashes[name] {
			t.Fatal("stability source hash mismatch")
		}
	}
	return r, paths, hex.EncodeToString(manifestHash[:])
}

func unsafeStabilityPath(name string) bool {
	for _, part := range strings.Split(strings.ReplaceAll(name, "\\", "/"), "/") {
		part = strings.ToLower(part)
		if part == ".env" || strings.HasPrefix(part, ".env.") || strings.Contains(part, "secret") || part == "credentials" {
			return true
		}
	}
	return false
}

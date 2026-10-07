package archive

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

const (
	qualityMaxEntries = 4096
	qualityMaxBytes   = 64 << 20
)

func validQualityIOPath(name string) error {
	for _, part := range strings.Split(name, string(filepath.Separator)) {
		if forbiddenRetrievalName(part) {
			return fmt.Errorf("forbidden quality file path: %q", name)
		}
	}
	return nil
}

func readQualityCorpus(filename string) (qualityCorpus, error) {
	if err := validQualityIOPath(filename); err != nil {
		return qualityCorpus{}, err
	}
	f, err := os.Open(filename)
	if err != nil {
		return qualityCorpus{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, qualityMaxBytes+1))
	if err != nil {
		return qualityCorpus{}, err
	}
	if len(data) > qualityMaxBytes {
		return qualityCorpus{}, fmt.Errorf("quality manifest exceeds %d bytes", qualityMaxBytes)
	}
	var c qualityCorpus
	err = json.Unmarshal(data, &c)
	return c, err
}

func validQualityCorpusShape(c qualityCorpus) bool {
	return c.Format == "retrieval-quality/1" && len(c.Cases) > 0 && len(c.Cases) <= qualityMaxEntries && len(c.Files) <= qualityMaxEntries
}

func TestQualityCorpusInputBounds(t *testing.T) {
	for _, name := range []string{".env", "archive/.env.local", "archive/../.env"} {
		if err := validQualityIOPath(name); err == nil {
			t.Errorf("forbidden manifest or output path accepted: %q", name)
		}
	}
	filename := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(filename, []byte(`{"format":"retrieval-quality/1","cases":[{"id":"a"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := readQualityCorpus(filename)
	if err != nil || !validQualityCorpusShape(c) {
		t.Fatalf("small manifest rejected: %+v %v", c, err)
	}
	c.Cases = make([]qualityCase, qualityMaxEntries+1)
	if validQualityCorpusShape(c) {
		t.Fatal("unbounded case list accepted")
	}
	c.Cases = c.Cases[:1]
	c.Files = make(map[string]string, qualityMaxEntries+1)
	for i := range qualityMaxEntries + 1 {
		c.Files[fmt.Sprint(i)] = ""
	}
	if validQualityCorpusShape(c) {
		t.Fatal("unbounded inline file list accepted")
	}
	if err := os.Truncate(filename, qualityMaxBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := readQualityCorpus(filename); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}

func validQualityPath(name string) error {
	if name == "." || !filepath.IsLocal(name) || filepath.Clean(name) != name || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("unsafe corpus path: %q", name)
	}
	for _, part := range strings.Split(name, string(filepath.Separator)) {
		if forbiddenRetrievalName(part) {
			return fmt.Errorf("forbidden corpus path: %q", name)
		}
	}
	return nil
}

func verifyQualityCorpus(root string, hashes map[string]string) error {
	if len(hashes) == 0 || len(hashes) > qualityMaxEntries {
		return fmt.Errorf("corpus manifest requires 1..%d files", qualityMaxEntries)
	}
	for name := range hashes {
		if err := validQualityPath(name); err != nil {
			return err
		}
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("corpus root is not a directory: %s", root)
	}
	var names []string
	var bytes int64
	entries := 0
	err = filepath.WalkDir(root, func(full string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if full == root {
			return nil
		}
		entries++
		if entries > qualityMaxEntries {
			return fmt.Errorf("corpus exceeds %d entries", qualityMaxEntries)
		}
		name, err := filepath.Rel(root, full)
		if err != nil {
			return err
		}
		if err := validQualityPath(name); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-regular corpus entry: %s", name)
		}
		if _, ok := hashes[name]; !ok {
			return fmt.Errorf("unlisted corpus file: %s", name)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		if bytes > qualityMaxBytes {
			return fmt.Errorf("corpus exceeds %d bytes", qualityMaxBytes)
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return err
	}
	if len(names) != len(hashes) {
		return fmt.Errorf("corpus manifest lists %d files, found %d", len(hashes), len(names))
	}
	reader := Reader{Imports: root}
	readBytes := int64(0)
	for _, name := range names {
		f, err := reader.open("/canti/imports/" + filepath.ToSlash(name))
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(f, qualityMaxBytes-readBytes+1))
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		readBytes += int64(len(data))
		if readBytes > qualityMaxBytes || digest(data) != hashes[name] {
			return fmt.Errorf("corpus source hash mismatch: %s", name)
		}
	}
	return nil
}

func copyQualityCorpus(root, frozen string, hashes map[string]string) error {
	reader := Reader{Imports: root}
	remaining := int64(qualityMaxBytes)
	for name := range hashes {
		f, err := reader.open("/canti/imports/" + filepath.ToSlash(name))
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(f, remaining+1))
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		remaining -= int64(len(data))
		if remaining < 0 || digest(data) != hashes[name] {
			return fmt.Errorf("corpus source changed while freezing: %s", name)
		}
		full := filepath.Join(frozen, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(full, data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func TestQualityReaderFreezesExternalCorpus(t *testing.T) {
	root := t.TempDir()
	name := filepath.Join(root, "answer.txt")
	if err := os.WriteFile(name, []byte("approved: 30 seconds"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := qualityCorpus{
		Root:   root,
		Hashes: map[string]string{"answer.txt": digest([]byte("approved: 30 seconds"))},
		Cases:  []qualityCase{{ID: "freeze", Split: "test", Query: "approved", Path: "/canti/imports/answer.txt"}},
	}
	r := qualityReader(t, c)
	if err := os.WriteFile(name, []byte("approved: 99 seconds"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := r.open("/canti/imports/answer.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(f)
	f.Close()
	if readErr != nil || string(data) != "approved: 30 seconds" {
		t.Fatalf("reader did not use frozen corpus: %q %v", data, readErr)
	}
}

func TestQualityCorpusManifest(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		full := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("evidence/answer.txt", "approved: 30 seconds")
	write("evidence/distractor.txt", "planning: 15 seconds")
	hashes := map[string]string{
		"evidence/answer.txt":     digest([]byte("approved: 30 seconds")),
		"evidence/distractor.txt": digest([]byte("planning: 15 seconds")),
	}
	if err := verifyQualityCorpus(root, hashes); err != nil {
		t.Fatal(err)
	}
	write("evidence/distractor.txt", "planning: 20 seconds")
	if err := verifyQualityCorpus(root, hashes); err == nil {
		t.Fatal("changed distractor was accepted")
	}
	write("evidence/distractor.txt", "planning: 15 seconds")
	write("evidence/extra.txt", "unlisted searchable text")
	if err := verifyQualityCorpus(root, hashes); err == nil {
		t.Fatal("unlisted searchable file was accepted")
	}
	if err := os.Remove(filepath.Join(root, "evidence/extra.txt")); err != nil {
		t.Fatal(err)
	}
	delete(hashes, "evidence/distractor.txt")
	if err := verifyQualityCorpus(root, hashes); err == nil {
		t.Fatal("missing distractor manifest entry was accepted")
	}
	hashes["evidence/distractor.txt"] = digest([]byte("planning: 15 seconds"))
	if err := os.Remove(filepath.Join(root, "evidence/distractor.txt")); err != nil {
		t.Fatal(err)
	}
	if err := verifyQualityCorpus(root, hashes); err == nil {
		t.Fatal("absent manifest file was accepted")
	}
}

func TestQualityCorpusManifestRejectsUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"../outside", "/absolute", "a/../b", ".", ".env", "a/.env.local"} {
		if err := validQualityPath(name); err == nil {
			t.Errorf("unsafe path accepted directly: %q", name)
		}
		if err := verifyQualityCorpus(root, map[string]string{name: strings.Repeat("0", 64)}); err == nil {
			t.Errorf("unsafe manifest path accepted: %q", name)
		}
	}
	if err := os.Symlink(root, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if err := verifyQualityCorpus(root, map[string]string{"linked/anything": strings.Repeat("0", 64)}); err == nil {
		t.Fatal("symlinked corpus entry was accepted")
	}
	if err := os.Remove(filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
}

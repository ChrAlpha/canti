package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

const (
	RetrievalDefaultBytes = 4096
	RetrievalMaxBytes     = 8192
	retrievalScanBytes    = 256 << 10
	retrievalMaxEntries   = 32768
)

type RetrievalQuery struct {
	Operation  string `json:"operation"`
	Path       string `json:"path"`
	Reason     string `json:"reason"`
	Offset     int64  `json:"offset,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Query      string `json:"query,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	IgnoreCase bool   `json:"ignore_case,omitempty"`
}

type SourceRange struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int    `json:"length"`
	SHA256 string `json:"sha256"`
}

type Retrieval struct {
	Query      RetrievalQuery `json:"query"`
	Sources    []SourceRange  `json:"sources,omitempty"`
	Entries    []string       `json:"entries,omitempty"`
	NextOffset int64          `json:"next_offset"`
	NextCursor string         `json:"next_cursor,omitempty"`
	More       bool           `json:"more"`
	Error      string         `json:"error,omitempty"`
}

type Reader struct {
	History   string
	Artifacts string
	Imports   string
}

func (r Reader) Retrieve(ctx context.Context, q RetrievalQuery) (Retrieval, error) {
	result := Retrieval{Query: q}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	limit := q.Limit
	if limit == 0 {
		limit = RetrievalDefaultBytes
	}
	if strings.TrimSpace(q.Reason) == "" || len(q.Reason) > 1024 || !utf8.ValidString(q.Reason) {
		result.Error = "reason must be nonempty UTF-8 of at most 1024 bytes"
		return result, nil
	}
	if q.Offset < 0 || limit < 1 || limit > RetrievalMaxBytes {
		result.Error = "offset must be nonnegative; limit must be 1..8192 bytes"
		return result, nil
	}
	if q.Operation != "read" && q.Operation != "list" && q.Operation != "search" {
		result.Error = "operation must be read, list, or search"
		return result, nil
	}
	if len(q.Query) > RetrievalMaxBytes || !utf8.ValidString(q.Query) || (q.Operation != "search" && q.Query != "") {
		result.Error = "query must be UTF-8 of at most 8192 bytes and only applies to search"
		return result, nil
	}
	if q.Operation == "search" && (q.Query == "" || len(q.Query) > limit || !utf8.ValidString(q.Query) || strings.ContainsRune(q.Query, 0)) {
		result.Error = "search query must be nonempty UTF-8 and fit within limit"
		return result, nil
	}
	if q.IgnoreCase && q.Operation != "search" {
		result.Error = "ignore_case only applies to search"
		return result, nil
	}
	if q.IgnoreCase && queryMatchBytes(q) > limit {
		result.Error = fmt.Sprintf("ignore_case requires a limit of at least %d bytes for folded matches", queryMatchBytes(q))
		return result, nil
	}
	if len(q.Cursor) > 1024 || (q.Cursor != "" && (q.Operation != "search" || q.Offset != 0)) {
		result.Error = "cursor is only valid for directory search with offset zero and at most 1024 bytes"
		return result, nil
	}
	f, err := r.open(q.Path)
	if err == nil {
		defer f.Close()
		var info os.FileInfo
		info, err = f.Stat()
		if err == nil {
			switch {
			case q.Operation == "search" && info.IsDir():
				err = r.searchDirectory(ctx, f, limit, &result)
			case q.Operation == "list" && info.IsDir():
				err = listRetrieval(ctx, f, limit, &result)
			case q.Cursor != "":
				err = errors.New("cursor requires a directory search")
			case q.Operation != "list" && info.Mode().IsRegular():
				err = readRetrieval(ctx, f, info.Size(), limit, &result)
			default:
				err = errors.New("list requires a directory; read requires a regular file; search requires either")
			}
		}
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if err != nil {
		result.Sources, result.Entries = nil, nil
		result.More = false
		result.NextCursor = ""
		result.Error = err.Error()
	}
	return result, nil
}

func (r Reader) Expand(ctx context.Context, source SourceRange) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if source.Offset < 0 || source.Length < 0 || source.Length > RetrievalMaxBytes {
		return "", errors.New("invalid source range")
	}
	f, err := r.open(source.Path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("source is not a regular file")
	}
	data := make([]byte, source.Length)
	if _, err := f.ReadAt(data, source.Offset); err != nil {
		return "", fmt.Errorf("source range unavailable: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if digest(data) != source.SHA256 {
		return "", errors.New("source range changed: sha256 mismatch")
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return "", errors.New("source range is not UTF-8 text")
	}
	return string(data), nil
}

func (r Reader) open(name string) (*os.File, error) {
	if !utf8.ValidString(name) || len(name) > 4096 || path.Clean(name) != name {
		return nil, errors.New("path must be a clean absolute UTF-8 archive path")
	}
	for _, component := range strings.Split(name, "/") {
		if forbiddenRetrievalName(component) {
			return nil, errors.New("environment files are not readable")
		}
	}
	for _, root := range []struct{ virtual, host string }{
		{"/canti/runs", r.History}, {"/canti/artifacts", r.Artifacts}, {"/canti/imports", r.Imports},
	} {
		if name != root.virtual && !strings.HasPrefix(name, root.virtual+"/") {
			continue
		}
		if root.host == "" {
			return nil, errors.New("archive root is unavailable")
		}
		base, err := os.OpenRoot(root.host)
		if err != nil {
			return nil, fmt.Errorf("open archive root: %w", err)
		}
		defer base.Close()
		rel := strings.TrimPrefix(strings.TrimPrefix(name, root.virtual), "/")
		if rel == "" {
			rel = "."
		}
		prefix := ""
		for _, component := range strings.Split(rel, "/") {
			prefix = path.Join(prefix, component)
			info, err := base.Lstat(prefix)
			if err != nil {
				return nil, fmt.Errorf("archive path unavailable: %w", err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, errors.New("archive symlinks are not readable")
			}
		}
		return base.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	}
	return nil, errors.New("path is outside configured archive roots")
}

func listRetrieval(ctx context.Context, f *os.File, limit int, result *Retrieval) error {
	if limit < 2 {
		return errors.New("list limit is too small for entries")
	}
	names, err := retrievalDirectory(ctx, f, false)
	if err != nil {
		return err
	}
	if result.Query.Offset > int64(len(names)) {
		return errors.New("list offset is beyond directory entries")
	}
	result.NextOffset = result.Query.Offset
	result.Entries = []string{}
	encodedBytes := 2
	for result.NextOffset < int64(len(names)) {
		name := path.Join(result.Query.Path, names[result.NextOffset])
		encoded, err := json.Marshal(name)
		if err != nil {
			return err
		}
		cost := len(encoded)
		if len(result.Entries) > 0 {
			cost++
		}
		if encodedBytes+cost > limit {
			if len(result.Entries) == 0 {
				return errors.New("list limit is too small for next path")
			}
			break
		}
		result.Entries = append(result.Entries, name)
		encodedBytes += cost
		result.NextOffset++
	}
	result.More = result.NextOffset < int64(len(names))
	return nil
}

func retrievalDirectory(ctx context.Context, f *os.File, filesOnly bool) ([]string, error) {
	var names []string
	scanned := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := f.ReadDir(256)
		scanned += len(entries)
		for _, entry := range entries {
			if !utf8.ValidString(entry.Name()) {
				return nil, errors.New("directory contains a non-UTF-8 path")
			}
			if !forbiddenRetrievalName(entry.Name()) && (entry.Type().IsRegular() || !filesOnly && entry.IsDir()) {
				names = append(names, entry.Name())
			}
		}
		if scanned > retrievalMaxEntries {
			return nil, errors.New("directory exceeds 32768 entries; choose a narrower path")
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(names)
	return names, nil
}

func forbiddenRetrievalName(name string) bool {
	return name == ".env" || strings.HasPrefix(name, ".env.")
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

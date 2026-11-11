package archive

import (
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

// RunsRoot, ArtifactsRoot, ImportsRoot, and InputsRoot are where jobs and
// retrieval see the step records of runs, the artifacts of jobs, an imported
// history, and the admitted agent-log inputs.
const (
	RunsRoot      = "/canti/runs"
	ArtifactsRoot = "/canti/artifacts"
	ImportsRoot   = "/canti/imports"
	InputsRoot    = "/canti/inputs"
)

// DefaultBytes and MaxBytes are the default and largest byte limit of a
// query; a result holds at most MaxSources sources, a directory at most
// MaxEntries entries, and a continuation cursor at most MaxCursorBytes.
const (
	DefaultBytes   = 4096
	MaxBytes       = 8192
	MaxSources     = 32
	MaxEntries     = 32768
	MaxCursorBytes = 1024
	scanBytes      = 256 << 10
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

type Skip struct {
	Step   int    `json:"step"`
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

type Retrieval struct {
	Query        RetrievalQuery `json:"query"`
	Sources      []SourceRange  `json:"sources,omitempty"`
	Evidence     []string       `json:"evidence,omitempty"`
	Entries      []string       `json:"entries,omitempty"`
	NextOffset   int64          `json:"next_offset"`
	NextCursor   string         `json:"next_cursor,omitempty"`
	More         bool           `json:"more"`
	Incomplete   bool           `json:"incomplete,omitempty"`
	SkippedCount int            `json:"skipped_count,omitempty"`
	Skipped      []Skip         `json:"skipped,omitempty"`
	Error        string         `json:"error,omitempty"`
}

// Reader reads the archive under RunsRoot, ArtifactsRoot, and ImportsRoot
// from the host directories History, Artifacts, and Imports; an empty one is
// unavailable. Discovery skips the steps whose command, after leading
// spaces, tabs, line feeds, and carriage returns, begins with one of
// Excluded, each shorter than 64 bytes. It reads 64 bytes of a command and,
// when those leave the comparison open, up to MaxBytes again, charged to its
// scan budget; a command that MaxBytes still leaves open is skipped as
// unclassified, which makes the scan incomplete.
type Reader struct {
	History   string
	Artifacts string
	Imports   string
	Inputs    string
	Excluded  []string
	// RepairInput repairs one missing input record from the journal when the
	// input was delivered; an undelivered input returns ErrInputAbsent.
	RepairInput func(context.Context, int64) error
}

func (r Reader) Retrieve(ctx context.Context, q RetrievalQuery) (Retrieval, error) {
	result := Retrieval{Query: q}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	limit, problem := q.check()
	if problem != "" {
		result.Error = problem
		return result, nil
	}
	err := r.perform(ctx, limit, &result)
	if ctx.Err() != nil {
		return Retrieval{Query: q}, ctx.Err()
	}
	if err != nil {
		result = Retrieval{Query: q, NextOffset: result.NextOffset, Error: err.Error()}
	}
	return result, nil
}

// check returns the byte limit of q, or why q is not a valid query.
func (q RetrievalQuery) check() (int, string) {
	limit := q.Limit
	if limit == 0 {
		limit = DefaultBytes
	}
	search := q.Operation == "search" || q.Operation == "discover"
	switch {
	case strings.TrimSpace(q.Reason) == "" || len(q.Reason) > 1024 || !utf8.ValidString(q.Reason):
		return limit, "reason must be nonempty UTF-8 of at most 1024 bytes"
	case q.Offset < 0 || limit < 1 || limit > MaxBytes:
		return limit, "offset must be nonnegative; limit must be 1..8192 bytes"
	case q.Operation != "read" && q.Operation != "list" && !search:
		return limit, "operation must be read, list, search, or discover"
	case len(q.Query) > MaxBytes || !utf8.ValidString(q.Query) || (!search && q.Query != ""):
		return limit, "query must be UTF-8 of at most 8192 bytes and only applies to search or discover"
	case search && (q.Query == "" || len(q.Query) > limit || strings.ContainsRune(q.Query, 0)):
		return limit, "search query must be nonempty UTF-8 and fit within limit"
	case q.IgnoreCase && !search:
		return limit, "ignore_case only applies to search or discover"
	case q.IgnoreCase && queryMatchBytes(q) > limit:
		return limit, fmt.Sprintf("ignore_case requires a limit of at least %d bytes for folded matches", queryMatchBytes(q))
	case len(q.Cursor) > MaxCursorBytes || (q.Cursor != "" && (!search || q.Offset != 0)):
		return limit, "cursor is only valid for directory search or discover with offset zero and at most 1024 bytes"
	}
	return limit, ""
}

// perform runs the operation of result's query on its path.
func (r Reader) perform(ctx context.Context, limit int, result *Retrieval) error {
	q := result.Query
	if pos, ok := inputPosition(q.Path); ok && r.RepairInput != nil {
		if err := r.RepairInput(ctx, pos); err != nil {
			return fmt.Errorf("input archive unavailable: %w", err)
		}
	}
	f, err := r.open(q.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	switch {
	case q.Operation == "discover":
		return r.discover(ctx, f, limit, result)
	case q.Operation == "search" && info.IsDir():
		return r.searchDirectory(ctx, f, limit, result)
	case q.Operation == "list" && info.IsDir():
		return listRetrieval(ctx, f, limit, result)
	case q.Cursor != "":
		return errors.New("cursor requires a directory search")
	case q.Operation != "list" && info.Mode().IsRegular():
		return readRetrieval(ctx, f, info.Size(), limit, result)
	}
	return errors.New("list requires a directory; read requires a regular file; search requires either")
}

// InputFile reads one input archive file through the same safe opening rules
// as retrieval: canonical input path, no symlink, regular file only,
// nonblocking open, bounded read and cancellation. expected is the maximum
// number of bytes the caller will accept; a longer file is refused.
func (r Reader) InputFile(ctx context.Context, pos int64, name string, expected int) ([]byte, error) {
	if pos < 1 || (name != MetaFile && name != "text") || expected < 0 {
		return nil, errors.New("invalid input archive file")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	virtual := InputsRoot + "/" + InputDir(pos) + "/" + name
	f, err := r.open(virtual)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("input archive entry is not a regular file")
	}
	if info.Size() > int64(expected) {
		return nil, fmt.Errorf("input archive entry is %d bytes, over the %d-byte expected record", info.Size(), expected)
	}
	body, err := io.ReadAll(io.LimitReader(f, int64(expected)+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(body) > expected {
		return nil, fmt.Errorf("input archive entry exceeds the %d-byte expected record", expected)
	}
	return body, nil
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
		{RunsRoot, r.History}, {ArtifactsRoot, r.Artifacts}, {ImportsRoot, r.Imports}, {InputsRoot, r.Inputs},
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
		if scanned > MaxEntries {
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

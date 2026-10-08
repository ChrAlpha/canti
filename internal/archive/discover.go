package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"
)

// DiscoveryScanBytes and DiscoverySteps bound the bytes and the steps one
// discover call scans.
const (
	DiscoveryScanBytes = 8 << 20
	DiscoverySteps     = 256
)

type discoveryCursor struct {
	Binding      string `json:"binding"`
	Through      int    `json:"through"`
	Step         int    `json:"step"`
	Field        int    `json:"field"`
	Offset       int64  `json:"offset"`
	Incomplete   bool   `json:"incomplete,omitempty"`
	SkippedCount int    `json:"skipped_count,omitempty"`
	Skipped      []Skip `json:"skipped,omitempty"`
}

func (r Reader) discover(ctx context.Context, dir *os.File, limit int, result *Retrieval) error {
	q := result.Query
	run, ok := strings.CutPrefix(q.Path, RunsRoot+"/")
	run, ok2 := strings.CutSuffix(run, "/steps")
	if !ok || !ok2 || run == "" || strings.Contains(run, "/") || q.Offset != 0 {
		return errors.New("discover requires /canti/runs/<run>/steps and uses cursor, not offset")
	}
	through, err := stepCount(dir)
	if err != nil {
		return err
	}
	cursor, err := discoveryStart(q, through)
	if err != nil {
		return err
	}
	result.Incomplete = cursor.Incomplete
	result.SkippedCount, result.Skipped = cursor.SkippedCount, append([]Skip(nil), cursor.Skipped...)
	d := discovery{r: r, q: q, cursor: cursor, result: result, matchBytes: queryMatchBytes(q),
		remaining: DiscoveryScanBytes, limit: limit, references: MaxBytes - 2}
	for examined := 0; d.cursor.Step > 0 && examined < DiscoverySteps; examined++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.remaining < MaxBytes+65+d.matchBytes+utf8.UTFMax || len(result.Sources) >= MaxSources-1 {
			break
		}
		if stop, err := d.step(ctx); stop || err != nil {
			if err != nil {
				return err
			}
			break
		}
		d.cursor.Step--
		d.cursor.Field, d.cursor.Offset = 0, 0
	}
	return finishDiscovery(result, d.cursor)
}

// stepCount returns how many step directories dir holds, which must be
// named 1 to that count in eight digits.
func stepCount(dir *os.File) (int, error) {
	entries, err := dir.ReadDir(MaxEntries + 1)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if len(entries) > MaxEntries {
		return 0, errors.New("discover exceeds 32768 step directories")
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		if !entry.IsDir() {
			return 0, errors.New("discover requires original step directories without symlinks")
		}
		names[i] = entry.Name()
	}
	sort.Strings(names)
	for i, name := range names {
		if name != StepDir(i+1) {
			return 0, errors.New("discover requires consecutive original step directories")
		}
	}
	return len(names), nil
}

// discoveryStart returns the cursor that q continues, or the one that starts
// at the newest of through steps.
func discoveryStart(q RetrievalQuery, through int) (discoveryCursor, error) {
	bound, _ := json.Marshal([]any{"discover", q.Path, q.Query, q.IgnoreCase})
	if q.Cursor == "" {
		return discoveryCursor{Binding: digest(bound), Through: through, Step: through}, nil
	}
	var cursor discoveryCursor
	invalid := errors.New("invalid discovery cursor or changed path/query/history; start a new discover without cursor")
	if decodeCursor(q.Cursor, &cursor) != nil || cursor.Binding != digest(bound) ||
		cursor.Through < 1 || cursor.Through > through || cursor.Step < 1 || cursor.Step > cursor.Through || cursor.Field < 0 || cursor.Field > 2 || cursor.Offset < 0 ||
		cursor.SkippedCount < 0 || cursor.SkippedCount > 3*(cursor.Through-cursor.Step)+cursor.Field ||
		cursor.SkippedCount > 0 && !cursor.Incomplete || len(cursor.Skipped) != min(cursor.SkippedCount, 2) {
		return cursor, invalid
	}
	for _, skip := range cursor.Skipped {
		if skip.Step < 1 || skip.Step > cursor.Through || (skip.Field != "command" && skip.Field != "preview" && skip.Field != "artifact") ||
			(skip.Reason != "nul" && skip.Reason != "utf8") {
			return cursor, invalid
		}
	}
	return cursor, nil
}

// discovery is the progress of one discover call: where it continues, the
// bytes it may still scan, present, and spend on source references, and
// its result.
type discovery struct {
	r                                        Reader
	q                                        RetrievalQuery
	cursor                                   discoveryCursor
	result                                   *Retrieval
	matchBytes, remaining, limit, references int
}

// pendingMeta is the meta source of a step, presented before the step's
// first match, and what presenting it costs.
type pendingMeta struct {
	source             SourceRange
	text               string
	bytes, refs, slots int
}

// step searches the fields of the cursor's step from its field and offset,
// and reports whether discovery stops in it.
func (d *discovery) step(ctx context.Context) (bool, error) {
	stepPath := path.Join(d.q.Path, StepDir(d.cursor.Step))
	command, err := d.r.discoveryData(ctx, stepPath+"/"+CommandFile, 64, false)
	if err != nil {
		return false, fmt.Errorf("discover command %d: %w", d.cursor.Step, err)
	}
	d.remaining -= len(command)
	if slices.ContainsFunc(d.r.Excluded, func(prefix string) bool { return strings.HasPrefix(string(command), prefix) }) {
		return false, nil
	}
	meta, files, incomplete, err := d.r.discoverySources(ctx, stepPath, d.cursor.Step)
	if err != nil {
		return false, fmt.Errorf("discover step %d: %w", d.cursor.Step, err)
	}
	d.remaining -= len(meta)
	d.result.Incomplete = d.result.Incomplete || incomplete
	if d.cursor.Field >= len(files) {
		return false, errors.New("discovery cursor field is no longer available; start a new discover without cursor")
	}
	source := sourceRange(stepPath+"/"+MetaFile, 0, meta)
	encoded, _ := json.Marshal(source)
	m := pendingMeta{source: source, text: string(meta), bytes: len(meta), refs: len(encoded) + 1, slots: 1}
	for ; d.cursor.Field < len(files); d.cursor.Field, d.cursor.Offset = d.cursor.Field+1, 0 {
		if d.remaining < d.matchBytes+utf8.UTFMax || len(d.result.Sources)+m.slots >= MaxSources {
			return true, nil
		}
		if stop, err := d.search(ctx, stepPath, files[d.cursor.Field], &m); stop || err != nil {
			return stop, err
		}
	}
	return false, nil
}

// search looks for one match in file from the cursor's offset, presenting
// it after m, and reports whether discovery stops at it.
func (d *discovery) search(ctx context.Context, stepPath, file string, m *pendingMeta) (bool, error) {
	partQuery := d.q
	partQuery.Operation, partQuery.Path, partQuery.Offset, partQuery.Cursor = "search", file, d.cursor.Offset, ""
	part := Retrieval{Query: partQuery}
	partLimit, partRefs := min(max(d.matchBytes, d.limit-m.bytes), max(d.matchBytes, 512)), max(0, d.references-m.refs)
	left, refs, read, err := d.r.searchDirectoryFile(ctx, d.remaining, partLimit, 1, partRefs, &part)
	d.remaining -= read
	if err != nil {
		reason := textFailure(err)
		if reason == "" {
			return false, fmt.Errorf("discover %s: %w", file, err)
		}
		field := "artifact"
		switch file {
		case stepPath + "/" + CommandFile:
			field = "command"
		case stepPath + "/" + PreviewFile:
			field = "preview"
		}
		d.skip(Skip{Step: d.cursor.Step, Field: field, Reason: reason})
		return false, nil
	}
	if len(part.Sources) > 0 {
		if m.bytes+partLimit-left > d.limit || m.refs > d.references {
			if len(d.result.Sources) == 0 {
				return false, errors.New("discover limit is too small for original metadata and a match")
			}
			return true, nil
		}
		if m.slots != 0 {
			d.result.Sources = append(d.result.Sources, m.source)
			d.result.Evidence = append(d.result.Evidence, m.text)
		}
		d.result.Sources = append(d.result.Sources, part.Sources...)
		d.result.Evidence = append(d.result.Evidence, part.Evidence...)
		d.limit -= m.bytes + partLimit - left
		d.references = refs
		*m = pendingMeta{}
	}
	if part.More && len(part.Sources) == 0 {
		if len(d.result.Sources) == 0 && part.NextOffset == d.cursor.Offset {
			return false, errors.New("discover source reference exceeds presentation limit")
		}
		d.cursor.Offset = part.NextOffset
		return true, nil
	}
	return false, nil
}

// skip records a source discovery could not search as text, keeping the
// first and the latest of them.
func (d *discovery) skip(s Skip) {
	d.result.SkippedCount++
	if len(d.result.Skipped) < 2 {
		d.result.Skipped = append(d.result.Skipped, s)
	} else {
		d.result.Skipped[1] = s
	}
	d.result.Incomplete = true
}

// textFailure names why err refused a source as text, or is empty.
func textFailure(err error) string {
	switch {
	case errors.Is(err, errTextNUL):
		return "nul"
	case errors.Is(err, errTextUTF8):
		return "utf8"
	}
	return ""
}

func finishDiscovery(result *Retrieval, cursor discoveryCursor) error {
	if cursor.Step > 0 {
		cursor.Incomplete = result.Incomplete
		cursor.SkippedCount, cursor.Skipped = result.SkippedCount, result.Skipped
		result.More = true
		result.NextCursor = encodeCursor(cursor)
	}
	return nil
}

func (r Reader) discoveryData(ctx context.Context, name string, limit int, whole bool) ([]byte, error) {
	f, err := r.open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || whole && info.Size() > int64(limit) {
		return nil, errors.New("discovery source must be a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)))
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err == nil && whole {
		if len(data) != int(info.Size()) {
			return nil, errors.New("discovery source size changed while reading")
		}
		data, err = textPrefix(data, false)
	}
	return data, err
}

func (r Reader) discoverySources(ctx context.Context, stepPath string, step int) ([]byte, []string, bool, error) {
	meta, err := r.discoveryData(ctx, stepPath+"/"+MetaFile, MaxBytes, true)
	if err != nil {
		return nil, nil, false, err
	}
	output, bytes, complete, err := decodeOutput(meta, step)
	if err != nil {
		return nil, nil, false, err
	}
	files := []string{stepPath + "/" + CommandFile}
	if output == unavailable {
		return meta, append(files, stepPath+"/"+PreviewFile), true, nil
	}
	if !strings.HasPrefix(output, ArtifactsRoot+"/") || path.Clean(output) != output {
		return nil, nil, false, errors.New("original output must name an artifact path")
	}
	f, err := r.open(output)
	if err != nil {
		return nil, nil, false, err
	}
	info, err := f.Stat()
	f.Close()
	if err != nil {
		return nil, nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() != bytes {
		return nil, nil, false, errors.New("original output is unavailable or its retained size changed")
	}
	if !complete {
		files = append(files, stepPath+"/"+PreviewFile)
	}
	return meta, append(files, output), !complete, nil
}

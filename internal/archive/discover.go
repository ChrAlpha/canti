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

const discoveryScanBytes = 8 << 20

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
	entries, err := dir.ReadDir(MaxEntries + 1)
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) > MaxEntries {
		return errors.New("discover exceeds 32768 step directories")
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		if !entry.IsDir() {
			return errors.New("discover requires original step directories without symlinks")
		}
		names[i] = entry.Name()
	}
	sort.Strings(names)
	for i, name := range names {
		if name != StepDir(i+1) {
			return errors.New("discover requires consecutive original step directories")
		}
	}
	bound, _ := json.Marshal([]any{"discover", q.Path, q.Query, q.IgnoreCase})
	cursor := discoveryCursor{Binding: digest(bound), Through: len(names), Step: len(names)}
	if q.Cursor != "" {
		cursor = discoveryCursor{}
		if decodeCursor(q.Cursor, &cursor) != nil || cursor.Binding != digest(bound) ||
			cursor.Through < 1 || cursor.Through > len(names) || cursor.Step < 1 || cursor.Step > cursor.Through || cursor.Field < 0 || cursor.Field > 2 || cursor.Offset < 0 ||
			cursor.SkippedCount < 0 || cursor.SkippedCount > 3*(cursor.Through-cursor.Step)+cursor.Field ||
			cursor.SkippedCount > 0 && !cursor.Incomplete || len(cursor.Skipped) != min(cursor.SkippedCount, 2) {
			return errors.New("invalid discovery cursor or changed path/query/history")
		}
		for _, skip := range cursor.Skipped {
			if skip.Step < 1 || skip.Step > cursor.Through || (skip.Field != "command" && skip.Field != "preview" && skip.Field != "artifact") ||
				(skip.Reason != "nul" && skip.Reason != "utf8") {
				return errors.New("invalid discovery cursor or changed path/query/history")
			}
		}
	}
	result.Incomplete = cursor.Incomplete
	result.SkippedCount, result.Skipped = cursor.SkippedCount, append([]Skip(nil), cursor.Skipped...)
	remaining, references := discoveryScanBytes, MaxBytes-2
	matchBytes := queryMatchBytes(q)
	for examined := 0; cursor.Step > 0 && examined < 256; examined++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if remaining < MaxBytes+65+matchBytes+utf8.UTFMax || len(result.Sources) >= 31 {
			break
		}
		stepPath := path.Join(q.Path, StepDir(cursor.Step))
		command, err := r.discoveryData(ctx, stepPath+"/"+CommandFile, 64, false)
		if err != nil {
			return fmt.Errorf("discover command %d: %w", cursor.Step, err)
		}
		remaining -= len(command)
		if slices.ContainsFunc(r.Excluded, func(prefix string) bool { return strings.HasPrefix(string(command), prefix) }) {
			cursor.Step--
			cursor.Field, cursor.Offset = 0, 0
			continue
		}
		meta, files, incomplete, err := r.discoverySources(ctx, stepPath, cursor.Step)
		if err != nil {
			return fmt.Errorf("discover step %d: %w", cursor.Step, err)
		}
		remaining -= len(meta)
		result.Incomplete = result.Incomplete || incomplete
		if cursor.Field >= len(files) {
			return errors.New("discovery cursor field is no longer available")
		}
		metaSource := sourceRange(stepPath+"/"+MetaFile, 0, meta)
		metaJSON, _ := json.Marshal(metaSource)
		metaBytes, metaRefs, metaSlots := len(meta), len(metaJSON)+1, 1
		for cursor.Field < len(files) {
			if remaining < matchBytes+utf8.UTFMax || len(result.Sources)+metaSlots >= 32 {
				return finishDiscovery(result, cursor)
			}
			partQuery := q
			partQuery.Operation, partQuery.Path, partQuery.Offset, partQuery.Cursor = "search", files[cursor.Field], cursor.Offset, ""
			part := Retrieval{Query: partQuery}
			partLimit, partRefs := min(max(matchBytes, limit-metaBytes), max(matchBytes, 512)), max(0, references-metaRefs)
			left, refs, read, err := r.searchDirectoryFile(ctx, remaining, partLimit, 1, partRefs, &part)
			if err != nil {
				if errors.Is(err, errTextNUL) || errors.Is(err, errTextUTF8) {
					reason := "utf8"
					if errors.Is(err, errTextNUL) {
						reason = "nul"
					}
					field := "artifact"
					if partQuery.Path == stepPath+"/"+CommandFile {
						field = "command"
					}
					if partQuery.Path == stepPath+"/"+PreviewFile {
						field = "preview"
					}
					skip := Skip{Step: cursor.Step, Field: field, Reason: reason}
					result.SkippedCount++
					if len(result.Skipped) == 0 {
						result.Skipped = append(result.Skipped, skip)
					} else if len(result.Skipped) == 1 {
						result.Skipped = append(result.Skipped, skip)
					} else {
						result.Skipped[1] = skip
					}
					result.Incomplete = true
					cursor.Field++
					cursor.Offset = 0
					remaining -= read
					continue
				}
				return fmt.Errorf("discover %s: %w", partQuery.Path, err)
			}
			remaining -= read
			if len(part.Sources) > 0 {
				if metaBytes+partLimit-left > limit || metaRefs > references {
					if len(result.Sources) == 0 {
						return errors.New("discover limit is too small for original metadata and a match")
					}
					return finishDiscovery(result, cursor)
				}
				if metaSlots != 0 {
					result.Sources = append(result.Sources, metaSource)
				}
				result.Sources = append(result.Sources, part.Sources...)
				limit -= metaBytes + partLimit - left
				references = refs
				metaBytes, metaRefs, metaSlots = 0, 0, 0
			}
			if part.More && len(part.Sources) == 0 {
				if len(result.Sources) == 0 && part.NextOffset == cursor.Offset {
					return errors.New("discover source reference exceeds presentation limit")
				}
				cursor.Offset = part.NextOffset
				return finishDiscovery(result, cursor)
			}
			cursor.Field++
			cursor.Offset = 0
		}
		cursor.Step--
		cursor.Field, cursor.Offset = 0, 0
	}
	return finishDiscovery(result, cursor)
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

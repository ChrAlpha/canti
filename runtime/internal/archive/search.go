package archive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

type directoryCursor struct {
	Binding string `json:"binding"`
	File    string `json:"file"`
	Offset  int64  `json:"offset"`
}

func (r Reader) searchDirectory(ctx context.Context, dir *os.File, limit int, result *Retrieval) error {
	q := result.Query
	if q.Offset != 0 {
		return errors.New("directory search uses cursor, not offset")
	}
	names, err := retrievalDirectory(ctx, dir, true)
	if err != nil {
		return err
	}
	cursor, err := decodeDirectoryCursor(q)
	if err != nil {
		return err
	}
	index := 0
	if q.Cursor != "" {
		index = sort.SearchStrings(names, cursor.File)
		if index == len(names) || names[index] != cursor.File {
			return errors.New("directory cursor file is no longer available; start a new search without cursor")
		}
	}
	remaining, references := scanBytes, MaxBytes-2
	examined := 0
	matchBytes := queryMatchBytes(q)
	for index < len(names) {
		if err := ctx.Err(); err != nil {
			return err
		}
		cursor.File = names[index]
		if limit < matchBytes || len(result.Sources) == MaxSources || remaining < matchBytes+utf8.UTFMax || examined == 256 {
			break
		}
		fileQuery := q
		fileQuery.Path, fileQuery.Offset, fileQuery.Cursor = path.Join(q.Path, cursor.File), cursor.Offset, ""
		part := Retrieval{Query: fileQuery}
		examined++
		var read int
		limit, references, read, err = r.searchDirectoryFile(ctx, remaining, limit, MaxSources-len(result.Sources), references, &part)
		if err != nil {
			return fmt.Errorf("directory search failed at %s with no results, which is not proof of absence; list the directory and read or search its files one at a time: %w", cursor.File, err)
		}
		remaining -= read
		result.Sources = append(result.Sources, part.Sources...)
		result.Evidence = append(result.Evidence, part.Evidence...)
		if part.More {
			if len(result.Sources) == 0 && part.NextOffset == cursor.Offset {
				return errors.New("source reference exceeds directory search presentation limit")
			}
			cursor.Offset = part.NextOffset
			break
		}
		index++
		cursor.Offset = 0
	}
	if index < len(names) {
		cursor.File = base64.RawURLEncoding.EncodeToString([]byte(names[index]))
		result.More = true
		result.NextCursor = encodeCursor(cursor)
	}
	return nil
}

func (r Reader) searchDirectoryFile(ctx context.Context, scan, limit, sources, references int, result *Retrieval) (int, int, int, error) {
	f, err := r.open(result.Query.Path)
	if err != nil {
		return limit, references, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return limit, references, 0, err
	}
	if !info.Mode().IsRegular() {
		return limit, references, 0, errors.New("source is not a regular file")
	}
	q := result.Query
	if q.Offset > info.Size() {
		return limit, references, 0, errors.New("cursor offset is beyond end of file")
	}
	count := min(int64(scan), info.Size()-q.Offset)
	data := make([]byte, int(count))
	n, err := f.ReadAt(data, q.Offset)
	if err != nil && err != io.EOF {
		return limit, references, n, err
	}
	if int64(n) != count {
		return limit, references, n, errors.New("source size changed while searching")
	}
	if err := ctx.Err(); err != nil {
		return limit, references, n, err
	}
	partial := q.Offset+int64(n) < info.Size()
	data, err = textPrefix(data, partial)
	if err != nil {
		return limit, references, n, err
	}
	end := len(data)
	if partial {
		end = max(0, len(data)-queryMatchBytes(q)+1)
		for end < len(data) && end > 0 && !utf8.RuneStart(data[end]) {
			end--
		}
	}
	limit, references = searchRanges(data, limit, end, sources, references, result)
	result.More = result.NextOffset < info.Size()
	return limit, references, n, nil
}

func decodeDirectoryCursor(q RetrievalQuery) (directoryCursor, error) {
	binding := []string{q.Path, q.Query}
	if q.IgnoreCase {
		binding = append(binding, "ignore_case")
	}
	bound, _ := json.Marshal(binding)
	want := digest(bound)
	if q.Cursor == "" {
		return directoryCursor{Binding: want}, nil
	}
	var cursor directoryCursor
	err := decodeCursor(q.Cursor, &cursor)
	if errors.Is(err, errCursorEncoding) {
		return directoryCursor{}, errors.New("invalid directory cursor encoding; start a new search without cursor")
	}
	filename, fileErr := base64.RawURLEncoding.DecodeString(cursor.File)
	cursor.File = string(filename)
	if err != nil || fileErr != nil || cursor.Binding != want ||
		cursor.Offset < 0 || cursor.File == "" || cursor.File == "." || cursor.File == ".." ||
		strings.ContainsRune(cursor.File, '/') || len(cursor.File) > 255 ||
		!utf8.ValidString(cursor.File) || forbiddenRetrievalName(cursor.File) {
		return directoryCursor{}, errors.New("invalid directory cursor or changed path/query; start a new search without cursor")
	}
	return cursor, nil
}

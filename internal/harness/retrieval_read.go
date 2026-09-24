package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"unicode/utf8"
)

func readRetrieval(ctx context.Context, f *os.File, size int64, limit int, result *Retrieval) error {
	q := result.Query
	if q.Offset > size {
		return errors.New("offset is beyond end of file")
	}
	count := int64(limit)
	if q.Operation == "search" {
		count = retrievalScanBytes + int64(queryMatchBytes(q)) + utf8.UTFMax
	}
	count = min(count, size-q.Offset)
	data := make([]byte, int(count))
	n, err := f.ReadAt(data, q.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err = textPrefix(data[:n], q.Offset+int64(n) < size)
	if err != nil {
		return err
	}
	result.NextOffset = q.Offset
	if q.Operation == "read" {
		if len(data) == 0 && q.Offset < size {
			return errors.New("read limit is too small for next UTF-8 character")
		}
		if len(data) > 0 {
			result.Sources = []SourceRange{sourceRange(q.Path, q.Offset, data)}
		}
		result.NextOffset += int64(len(data))
	} else {
		searchRetrieval(data, limit, result)
	}
	result.More = result.NextOffset < size
	return nil
}

func searchRetrieval(data []byte, limit int, result *Retrieval) {
	end := min(len(data), retrievalScanBytes)
	for end < len(data) && end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	searchRanges(data, limit, end, 32, -1, result)
}

func searchRanges(data []byte, limit, end, maxSources, references int, result *Retrieval) (int, int) {
	q := result.Query
	var folded foldedLiteral
	if q.IgnoreCase {
		folded = newFoldedLiteral(q.Query)
	}
	pos := 0
	for pos < end && len(result.Sources) < maxSources {
		match := bytes.Index(data[pos:], []byte(q.Query))
		length := len(q.Query)
		if q.IgnoreCase {
			match, length = folded.index(data[pos:])
		}
		if match < 0 || pos+match >= end {
			pos = end
			break
		}
		match += pos
		if limit < length {
			pos = match
			break
		}
		padding := min(128, (limit-length)/2)
		start, stop := max(0, match-padding), min(len(data), match+length+padding)
		for start < match && !utf8.RuneStart(data[start]) {
			start++
		}
		for stop < len(data) && !utf8.RuneStart(data[stop]) {
			stop--
		}
		source := sourceRange(q.Path, q.Offset+int64(start), data[start:stop])
		if references >= 0 {
			encoded, _ := json.Marshal(source)
			if len(encoded)+1 > references {
				pos = match
				break
			}
			references -= len(encoded) + 1
		}
		result.Sources = append(result.Sources, source)
		limit -= stop - start
		pos = match + length
		if q.IgnoreCase {
			_, width := utf8.DecodeRune(data[match:])
			pos = match + width
		}
	}
	result.NextOffset = q.Offset + int64(pos)
	return limit, references
}

func textPrefix(data []byte, partial bool) ([]byte, error) {
	for i := 0; i < len(data); {
		if data[i] == 0 {
			return nil, errors.New("source contains NUL; only UTF-8 text is supported")
		}
		_, size := utf8.DecodeRune(data[i:])
		if size == 1 && data[i] >= utf8.RuneSelf {
			if partial && !utf8.FullRune(data[i:]) {
				return data[:i], nil
			}
			return nil, errors.New("source is not UTF-8 text or offset splits a character")
		}
		i += size
	}
	return data, nil
}

func sourceRange(path string, offset int64, data []byte) SourceRange {
	return SourceRange{Path: path, Offset: offset, Length: len(data), SHA256: digest(data)}
}

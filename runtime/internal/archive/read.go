package archive

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
		count = scanBytes + int64(queryMatchBytes(q)) + utf8.UTFMax
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
			result.Evidence = []string{string(data)}
		}
		result.NextOffset += int64(len(data))
	} else {
		searchRetrieval(data, limit, result)
	}
	result.More = result.NextOffset < size
	return nil
}

func searchRetrieval(data []byte, limit int, result *Retrieval) {
	end := min(len(data), scanBytes)
	for end < len(data) && end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	searchRanges(data, limit, end, 32, -1, result)
}

func searchRanges(data []byte, limit, end, maxSources, references int, result *Retrieval) (int, int) {
	q := result.Query
	r := ranges{data: data, q: q, result: result, limit: limit, references: references,
		padding: min(128, (limit-len(q.Query))/2), changed: len(result.Sources)}
	if references >= 0 {
		encoded, _ := json.Marshal(SourceRange{Path: q.Path, SHA256: digest(nil)})
		r.base = len(encoded) - 2
	}
	var folded foldedLiteral
	if q.IgnoreCase {
		folded = newFoldedLiteral(q.Query)
	}
	pos := 0
	for pos < end {
		match, length := -1, len(q.Query)
		if q.IgnoreCase {
			match, length = folded.index(data[pos:])
		} else {
			match = bytes.Index(data[pos:], []byte(q.Query))
		}
		if match < 0 || pos+match >= end {
			pos = end
			break
		}
		match += pos
		if !r.add(match, length, maxSources) {
			pos = match
			break
		}
		pos = match + length
		if q.IgnoreCase {
			_, width := utf8.DecodeRune(data[match:])
			pos = match + width
		}
	}
	result.Evidence = result.Evidence[:min(len(result.Evidence), r.changed)]
	for i := r.changed; i < len(result.Sources); i++ {
		source := &result.Sources[i]
		start := int(source.Offset - q.Offset)
		text := data[start : start+source.Length]
		source.SHA256 = digest(text)
		result.Evidence = append(result.Evidence, string(text))
	}
	result.NextOffset = q.Offset + int64(pos)
	return r.limit, r.references
}

// ranges presents the matches of a search of data as sources of result
// within the bytes it may still present and, unless negative, spend on
// source references beyond base each.
type ranges struct {
	data                                      []byte
	q                                         RetrievalQuery
	result                                    *Retrieval
	limit, references, padding, base, changed int
}

// add presents the match at [match, match+length) as a new source, or
// widens the last source when they overlap, and reports whether it fit.
func (r *ranges) add(match, length, maxSources int) bool {
	sources := r.result.Sources
	start, stop := around(r.data, match, length, r.padding)
	last := len(sources) - 1
	merge := last >= 0 && sources[last].Path == r.q.Path &&
		r.q.Offset+int64(start) <= sources[last].Offset+int64(sources[last].Length)
	if merge {
		previous := sources[last]
		start = int(previous.Offset - r.q.Offset)
		stop = max(stop, start+previous.Length)
		if allowed := start + previous.Length + r.limit; stop > allowed {
			stop = allowed
			for stop > match && !utf8.RuneStart(r.data[stop]) {
				stop--
			}
		}
		if stop < match+length {
			return false
		}
	} else {
		if len(sources) == maxSources || r.limit < length {
			return false
		}
		start, stop = around(r.data, match, length, min(128, (r.limit-length)/2))
	}
	source := SourceRange{Path: r.q.Path, Offset: r.q.Offset + int64(start), Length: stop - start}
	added := stop - start
	if merge {
		added -= sources[last].Length
	}
	if r.references >= 0 {
		available := r.references
		if merge {
			available += r.base + decimalDigits(sources[last].Offset) + decimalDigits(int64(sources[last].Length)) + 1
		}
		encoded := r.base + decimalDigits(source.Offset) + decimalDigits(int64(source.Length))
		if encoded+1 > available {
			return false
		}
		r.references = available - encoded - 1
	}
	if merge {
		sources[last] = source
		r.changed = min(r.changed, last)
	} else {
		r.result.Sources = append(sources, source)
	}
	r.limit -= added
	return true
}

// around returns the range of data padding bytes around [match,
// match+length), shrunk to whole runes.
func around(data []byte, match, length, padding int) (int, int) {
	start, stop := max(0, match-padding), min(len(data), match+length+padding)
	for start < match && !utf8.RuneStart(data[start]) {
		start++
	}
	for stop < len(data) && !utf8.RuneStart(data[stop]) {
		stop--
	}
	return start, stop
}

func decimalDigits(n int64) int {
	digits := 1
	u := uint64(n)
	if n < 0 {
		digits++
		u = uint64(-(n + 1)) + 1
	}
	for u >= 10 {
		u /= 10
		digits++
	}
	return digits
}

var (
	errTextNUL  = errors.New("source contains NUL; only UTF-8 text is supported")
	errTextUTF8 = errors.New("source is not UTF-8 text or offset splits a character")
)

func textPrefix(data []byte, partial bool) ([]byte, error) {
	for i := 0; i < len(data); {
		if data[i] == 0 {
			return nil, errTextNUL
		}
		_, size := utf8.DecodeRune(data[i:])
		if size == 1 && data[i] >= utf8.RuneSelf {
			if partial && !utf8.FullRune(data[i:]) {
				return data[:i], nil
			}
			return nil, errTextUTF8
		}
		i += size
	}
	return data, nil
}

func sourceRange(path string, offset int64, data []byte) SourceRange {
	return SourceRange{Path: path, Offset: offset, Length: len(data), SHA256: digest(data)}
}

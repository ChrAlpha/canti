package archive

import (
	"unicode"
	"unicode/utf8"
)

type foldedLiteral struct {
	pattern []rune
	prefix  []int
	starts  []int
}

func newFoldedLiteral(query string) foldedLiteral {
	pattern := []rune(query)
	prefix := make([]int, len(pattern))
	for i, ch := range pattern {
		pattern[i] = canonicalFold(ch)
		if i == 0 {
			continue
		}
		j := prefix[i-1]
		for j > 0 && pattern[i] != pattern[j] {
			j = prefix[j-1]
		}
		if pattern[i] == pattern[j] {
			j++
		}
		prefix[i] = j
	}
	return foldedLiteral{pattern, prefix, make([]int, len(pattern))}
}

func (f foldedLiteral) index(data []byte) (int, int) {
	matched, seen := 0, 0
	for offset := 0; offset < len(data); {
		ch, width := utf8.DecodeRune(data[offset:])
		ch = canonicalFold(ch)
		f.starts[seen%len(f.pattern)] = offset
		seen++
		for matched > 0 && ch != f.pattern[matched] {
			matched = f.prefix[matched-1]
		}
		if ch == f.pattern[matched] {
			matched++
		}
		if matched == len(f.pattern) {
			start := f.starts[(seen-len(f.pattern))%len(f.pattern)]
			return start, offset + width - start
		}
		offset += width
	}
	return -1, 0
}

func canonicalFold(ch rune) rune {
	if ch < utf8.RuneSelf {
		if ch >= 'a' && ch <= 'z' {
			return ch - ('a' - 'A')
		}
		return ch
	}
	canonical := ch
	for next := unicode.SimpleFold(ch); next != ch; next = unicode.SimpleFold(next) {
		canonical = min(canonical, next)
	}
	return canonical
}

func queryMatchBytes(q RetrievalQuery) int {
	if !q.IgnoreCase {
		return len(q.Query)
	}
	length := 0
	for _, ch := range q.Query {
		width := utf8.RuneLen(ch)
		for next := unicode.SimpleFold(ch); next != ch; next = unicode.SimpleFold(next) {
			width = max(width, utf8.RuneLen(next))
		}
		length += width
	}
	return length
}

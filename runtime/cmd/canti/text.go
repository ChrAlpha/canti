package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// clean drops every character but newlines, tabs, and graphic characters.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, s)
}

const excerptLines, excerptWidth = 8, 200

// excerpt returns at most excerptLines lines of s, each cut to excerptWidth
// bytes. With tail, it returns the last lines as a terminal shows output:
// each cut to what follows its last carriage return and cleaned. Otherwise
// it quotes, as Go does and marked (quoted), each line holding a character
// clean would drop.
func excerpt(s string, tail bool) []string {
	if s = strings.TrimRight(s, "\r\n"); s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l = strings.TrimSuffix(l, "\r"); tail {
			l = clean(l[strings.LastIndexByte(l, '\r')+1:])
		}
		short := l
		clipped := len(l) > excerptWidth
		if clipped {
			k := excerptWidth
			for k > 0 && !utf8.RuneStart(l[k]) {
				k--
			}
			short = l[:k]
		}
		if !tail && strings.ContainsFunc(l, hidden) {
			short = "(quoted) " + strconv.Quote(short)
		}
		if clipped {
			short += "…"
		}
		lines[i] = short
	}
	n := len(lines) - excerptLines
	switch {
	case n <= 0:
		return lines
	case tail:
		return append([]string{"…"}, lines[n:]...)
	}
	return append(lines[:excerptLines], fmt.Sprintf("… %d more %s", n, map[bool]string{true: "line", false: "lines"}[n == 1]))
}

// hidden reports a character that is not a tab or a graphic character.
func hidden(r rune) bool { return r != '\t' && (!unicode.IsGraphic(r) || r == utf8.RuneError) }

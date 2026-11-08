package main

import (
	"strings"
	"testing"
)

func TestExcerpt(t *testing.T) {
	long := strings.Repeat("x", 300)
	for _, tc := range []struct {
		in   string
		tail bool
		want []string
	}{
		{"a\nb\n", false, []string{"a", "b"}},
		{"a\x1b[2Jb\u0085‮\tc", true, []string{"a[2Jb\tc"}},
		{"a\x1b[2Jb\u0085‮\tc\b", false, []string{`(quoted) "a\x1b[2Jb\u0085\u202e\tc\b"`}},
		{`printf 'a\x1bb'`, false, []string{`printf 'a\x1bb'`}},
		{`"printf 'a\x1bb'"`, false, []string{`"printf 'a\x1bb'"`}},
		{"printf 'a\x1bb'", false, []string{`(quoted) "printf 'a\x1bb'"`}},
		{"\b" + strings.Repeat("x", 199) + "TAIL", false, []string{`(quoted) "\b` + strings.Repeat("x", 199) + `"…`}},
		{"\b" + long, false, []string{`(quoted) "\b` + long[:199] + `"…`}},
		{"\b" + long[:199] + "TAIL", false, []string{`(quoted) "\b` + long[:199] + `"…`}},
		{"\b" + long[:198], false, []string{`(quoted) "\b` + long[:198] + `"`}},
		{"\b" + long[:199], false, []string{`(quoted) "\b` + long[:199] + `"`}},
		{"\b" + long[:198] + "é", false, []string{`(quoted) "\b` + long[:198] + `"…`}},
		{"10%\r20%\rdone\r\nnext\r\n", true, []string{"done", "next"}},
		{"rm -rf x;\recho hi\r\n", false, []string{`(quoted) "rm -rf x;\recho hi"`}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "… 1 more line"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", false, []string{"1", "2", "3", "4", "5", "6", "7", "8", "… 2 more lines"}},
		{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", true, []string{"…", "3", "4", "5", "6", "7", "8", "9", "10"}},
		{long, false, []string{long[:200] + "…"}},
		{"é" + long, false, []string{"é" + long[:198] + "…"}},
		{"\xff", true, []string{"�"}},
		{"\xff", false, []string{`(quoted) "\xff"`}},
		{"", false, nil},
	} {
		if got := excerpt(tc.in, tc.tail); strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("excerpt(%q, %v) = %q, want %q", tc.in, tc.tail, got, tc.want)
		}
	}
}

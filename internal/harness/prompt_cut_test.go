package harness

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func referenceBlock(s string, limit int) string {
	clip := func(s string, limit int) string {
		kept, dropped := referenceCut(s, limit)
		if dropped == 0 {
			return kept
		}
		return fmt.Sprintf("%s…[truncated %d bytes]", kept, dropped)
	}
	fence := func(s string) int {
		longest, run := 2, 0
		for i := range len(s) {
			if s[i] == '`' {
				run++
			} else {
				run = 0
			}
			longest = max(longest, run)
		}
		return longest + 1
	}
	text := clip(s, limit)
	if n := fence(text); n > 3 {
		text = clip(s, max(0, limit-2*(n-3)))
	}
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	marks := strings.Repeat("`", fence(text))
	return marks + "\n" + text + marks + "\n"
}

func referenceCut(s string, limit int) (string, int) {
	var out strings.Builder
	consumed := 0
	for consumed < len(s) {
		r, n := utf8.DecodeRuneInString(s[consumed:])
		if out.Len()+utf8.RuneLen(r) > limit {
			break
		}
		out.WriteRune(r)
		consumed += n
	}
	return out.String(), len(s) - consumed
}

func TestCutMatchesRunePrefix(t *testing.T) {
	texts := []string{"", "plain text", "é日🙂", "\xff", "\x80a", "a\x80", "\uFFFD\xff", "\xf0\x90\x80", "x\xe0\x80\x80y"}
	for i := range 256 {
		texts = append(texts, "aé"+string([]byte{byte(i)})+"日🙂")
	}
	for _, text := range texts {
		for limit := -1; limit <= len(text)+4; limit++ {
			got, dropped := cut(text, limit)
			want, wantDropped := referenceCut(text, limit)
			if got != want || dropped != wantDropped {
				t.Fatalf("cut(%q, %d) = (%q, %d), want (%q, %d)", text, limit, got, dropped, want, wantDropped)
			}
		}
	}
}

func FuzzCutMatchesRunePrefix(f *testing.F) {
	for _, text := range []string{"plain text", "é日🙂", "\xff\x80a", "a日\x80", "\uFFFD\xff", "\xf0\x90\x80"} {
		f.Add(text, 3)
	}
	f.Fuzz(func(t *testing.T, text string, limit int) {
		if len(text) > 8192 {
			return
		}
		limit %= len(text) + 5
		got, dropped := cut(text, limit)
		want, wantDropped := referenceCut(text, limit)
		if got != want || dropped != wantDropped {
			t.Fatalf("cut(%q, %d) = (%q, %d), want (%q, %d)", text, limit, got, dropped, want, wantDropped)
		}
	})
}

func FuzzBlockPreservesBytes(f *testing.F) {
	for _, text := range []string{"", "plain text", "é日🙂", "\xff\x80a", "```", "before\n````\nafter", strings.Repeat("`", 100), "\n"} {
		for _, limit := range []int{-1, 0, 3, 8, len(text), 1000} {
			f.Add(text, limit)
		}
	}
	f.Fuzz(func(t *testing.T, text string, limit int) {
		if len(text) > 8192 {
			return
		}
		limit %= len(text) + 5
		if got, want := block(text, limit), referenceBlock(text, limit); got != want {
			t.Fatalf("block(%q, %d) = %q, want %q", text, limit, got, want)
		}
	})
}

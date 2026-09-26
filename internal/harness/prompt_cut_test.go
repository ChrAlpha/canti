package harness

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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

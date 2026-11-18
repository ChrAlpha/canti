package jobrunner

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxPreviewBytes and MaxPreviewLines bound what one step shows of its command
// output. They match the scale of a coding agent's bash tool: at most 2,000
// lines and 50 KiB, whichever is reached first. The marker that explains a
// cut counts toward both limits.
const (
	MaxPreviewBytes = 50 << 10
	MaxPreviewLines = 2000

	// MinPreviewBytes leaves room for the mandatory marker and some content.
	// Configuration and the helper refuse a smaller preview limit.
	MinPreviewBytes = 4 << 10

	// MaxArtifactPath bounds the path a marker names twice. It matches the
	// bound the execution state and retrieval paths use.
	MaxArtifactPath = 1 << 10

	// PreviewFile holds the rendered preview beside the retained output. A
	// preview of control bytes can expand sixfold in JSON, so it is not kept
	// in the bounded completion record.
	PreviewFile = "preview"
)

// markerBudget reserves bytes for the one-line marker independently of the
// artifact path, so the selected content is the same for the same output and
// storage facts on every execution path. cancellationBudget reserves room
// inside that margin for the cancellation status merged into a marked
// preview at publication time.
const (
	markerBudget       = 3 << 10
	cancellationBudget = 64
)

// PreviewOptions are the durable facts the renderer needs besides the output
// itself. Stored is the number of prefix bytes the artifact retained;
// StoredLines describes that prefix for retrievable paging.
type PreviewOptions struct {
	Artifact      string
	Stored        int64
	ArtifactLimit int64
	StoredLines   int64
	Incomplete    bool
	CaptureError  string
	Partial       bool
	// PreviewLimit is the run's configured preview byte limit. A zero value
	// means the renderer's own limit.
	PreviewLimit int
	// PageEnd returns the exclusive raw end of a recommended page starting at
	// start and never past max. It lets the renderer keep a page within the
	// next preview's line and byte limits by finding a line boundary in the
	// stored artifact. A nil function falls back to the byte limit.
	PageEnd func(start, max int64) int64
}

// PreviewBuffer retains the head, true tail, and line facts a preview needs
// without retaining the whole output. Observe is independent of chunk
// boundaries.
type PreviewBuffer struct {
	limit           int
	marked          bool
	markerStart     int
	markerLength    int
	cancelledMarker string
	head, tail      []byte
	total           int64
	newlines        int64
	last            byte
	tailStartLine   bool
}

// NewPreviewBuffer returns a buffer that retains at most limit bytes at each
// end. The caller bounds limit to MaxPreviewBytes.
func NewPreviewBuffer(limit int) PreviewBuffer {
	return PreviewBuffer{limit: max(0, limit), tailStartLine: true}
}

// Observe adds the next bytes of output. It never retains more than limit
// bytes at either end.
func (p *PreviewBuffer) Observe(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	before, previous := p.total, p.last
	p.total += int64(len(chunk))
	p.newlines += int64(bytes.Count(chunk, []byte{'\n'}))
	p.last = chunk[len(chunk)-1]
	if n := p.limit - len(p.head); n > 0 {
		p.head = append(p.head, chunk[:min(n, len(chunk))]...)
	}
	if p.limit == 0 {
		return
	}
	if len(chunk) >= p.limit {
		start := len(chunk) - p.limit
		p.tail = append(p.tail[:0], chunk[start:]...)
		switch {
		case start > 0:
			p.tailStartLine = chunk[start-1] == '\n'
		case before == 0:
			p.tailStartLine = true
		default:
			p.tailStartLine = previous == '\n'
		}
		return
	}
	if len(p.tail) == 0 {
		p.tailStartLine = before == 0 || previous == '\n'
	}
	if over := len(p.tail) + len(chunk) - p.limit; over > 0 {
		p.tailStartLine = p.tail[over-1] == '\n'
		// Reuse the retained buffer instead of reslicing away its capacity;
		// otherwise every chunk reallocates the tail while the file grows.
		p.tail = append(p.tail[:0], p.tail[over:]...)
	}
	p.tail = append(p.tail, chunk...)
}

// Total is the number of bytes observed, not the number retained.
func (p *PreviewBuffer) Total() int64 { return p.total }

// String renders the preview for the facts in o. Output within both limits,
// not cut by the output limit and with no capture provenance to report, is
// returned whole without a marker. A cut preview keeps a head and a tail and
// interrupts them with one marker naming the shown and omitted lines, the
// observed total, the artifact state, its job-readable path, and a bounded
// command for data the artifact still stores.
func (p *PreviewBuffer) String(o PreviewOptions) string {
	p.marked, p.markerStart, p.markerLength, p.cancelledMarker = false, 0, 0, ""
	limit := p.limit
	if limit == 0 {
		return ""
	}
	if o.PreviewLimit == 0 {
		o.PreviewLimit = limit
	}
	lines := p.lines()
	whole := renderRaw(p.head)
	wholeOutput := p.total <= int64(len(p.head))
	needsMarker := !wholeOutput || o.Stored < p.total || o.Incomplete || o.CaptureError != "" || o.Partial || len(whole) > limit || lines > MaxPreviewLines
	if p.total == 0 && !needsMarker && o.Stored == 0 {
		return ""
	}
	if wholeOutput && len(whole) <= limit && lines <= MaxPreviewLines && !needsMarker {
		return whole
	}
	contentBudget := max(0, limit-markerBudget)
	if needsMarker {
		contentBudget = max(0, contentBudget-cancellationBudget)
	}
	if needsMarker && p.total <= int64(contentBudget) && len(whole) <= contentBudget && lines < MaxPreviewLines {
		selected := selection{head: whole, headEnd: lines, headRaw: p.total}
		p.marked = true
		marker := previewMarker(selected, lines, p.total, o)
		cancelled := o
		cancelled.Partial = true
		p.cancelledMarker = previewMarker(selected, lines, p.total, cancelled)
		text := whole
		if text != "" && !strings.HasSuffix(text, "\n") {
			text += "\n"
		}
		p.markerStart, p.markerLength = len(text), len(marker)
		text += marker
		text = p.finish(text)
		if len(text) <= limit && PreviewLines(text) <= MaxPreviewLines {
			return text
		}
	}
	head, headEnd, headPartial, headRaw, _, headShown := selectHead(p.head, p.total > int64(len(p.head)), MaxPreviewLines/2-1, contentBudget/2)
	tailBudget := contentBudget - contentBudget/2
	tail, tailStart, tailPartial, tailPartialRaw, tailShown := selectTail(p.tail, p.tailStartLine, lines, MaxPreviewLines/2, tailBudget)
	selected := selection{head: head, tail: tail, headEnd: headEnd, tailStart: tailStart,
		headPartial: headPartial, tailPartial: tailPartial, headBytes: headRaw, tailBytes: tailPartialRaw, headRaw: headShown, tailRaw: tailShown}
	p.marked = true
	marker := previewMarker(selected, lines, p.total, o)
	cancelled := o
	cancelled.Partial = true
	p.cancelledMarker = previewMarker(selected, lines, p.total, cancelled)
	p.markerStart, p.markerLength = 0, len(marker)
	if selected.headEnd > 0 {
		p.markerStart = len(selected.head) + 1
	}
	text := p.finish(assemblePreview(selected, marker, p.last == '\n'))
	if len(text) <= limit && PreviewLines(text) <= MaxPreviewLines {
		return text
	}
	return utf8Prefix(text, limit)
}

// lines is the number of logical lines observed. A trailing newline
// terminates the last line; it does not start an empty one.
func (p *PreviewBuffer) lines() int64 {
	if p.total == 0 {
		return 0
	}
	if p.last == '\n' {
		return p.newlines
	}
	return p.newlines + 1
}

func (p *PreviewBuffer) finish(text string) string {
	if p.last == '\n' && text != "" && !strings.HasSuffix(text, "\n") {
		return text + "\n"
	}
	return text
}

type selection struct {
	head, tail               string
	headEnd, tailStart       int64
	headPartial, tailPartial bool
	headBytes, tailBytes     int
	headRaw                  int64
	tailRaw                  int64
}

type rawLine struct {
	raw  []byte
	text string
}

func rawLines(b []byte) (lines []rawLine, trailing bool) {
	if len(b) == 0 {
		return nil, false
	}
	trailing = b[len(b)-1] == '\n'
	parts := bytes.Split(b, []byte{'\n'})
	if trailing {
		parts = parts[:len(parts)-1]
	}
	lines = make([]rawLine, len(parts))
	for i, part := range parts {
		lines[i] = rawLine{raw: part, text: strings.ToValidUTF8(string(part), "\uFFFD")}
	}
	return lines, trailing
}

// selectHead keeps whole lines from the start. A line too long for the
// remaining budget is cut at a rune boundary and marked partial.
func selectHead(b []byte, truncated bool, maxLines, maxBytes int) (text string, endLine int64, partial bool, rawShown int, omitByte, shownRaw int64) {
	lines, trailing := rawLines(b)
	if len(lines) == 0 || maxLines <= 0 || maxBytes <= 0 {
		return "", 0, false, 0, 0, 0
	}
	lastPartial := truncated && !trailing
	kept := make([]string, 0, min(len(lines), maxLines))
	used := 0
	rawOffset := int64(0)
	for i, line := range lines {
		if len(kept) >= maxLines {
			break
		}
		cost := len(line.text) + 1
		if used+cost <= maxBytes {
			kept = append(kept, line.text)
			used += cost
			endLine = int64(i + 1)
			partial = lastPartial && i == len(lines)-1
			shownRaw += int64(len(line.raw))
			if i < len(lines)-1 || trailing {
				shownRaw++
			}
			if partial {
				rawShown, omitByte = len(line.raw), rawOffset+int64(len(line.raw))
			}
			rawOffset += int64(len(line.raw)) + 1
			continue
		}
		room := maxBytes - used
		if len(kept) > 0 {
			room--
		}
		if room > 0 {
			piece, shown := cutRawLine(line.raw, room)
			if piece != "" {
				kept = append(kept, piece)
				endLine = int64(i + 1)
				partial, rawShown = true, shown
				omitByte = rawOffset + int64(shown)
				shownRaw += int64(shown)
			}
		}
		break
	}
	return strings.Join(kept, "\n"), endLine, partial, rawShown, omitByte, shownRaw
}

func cutRawLine(raw []byte, maxDisplay int) (string, int) {
	var b strings.Builder
	consumed := 0
	for consumed < len(raw) {
		r, n := utf8.DecodeRune(raw[consumed:])
		text := raw[consumed : consumed+n]
		if r == utf8.RuneError && n == 1 {
			end := consumed + n
			for end < len(raw) {
				next, size := utf8.DecodeRune(raw[end:])
				if next != utf8.RuneError || size != 1 {
					break
				}
				end += size
			}
			n = end - consumed
			text = []byte("\uFFFD")
		}
		if b.Len()+len(text) > maxDisplay {
			break
		}
		b.Write(text)
		consumed += n
	}
	return b.String(), consumed
}

// selectTail keeps whole lines from the end. A tail that begins inside a line
// keeps that suffix only after whole lines, or instead of them when a single
// line is longer than the whole budget.
func selectTail(b []byte, startsLine bool, totalLines int64, maxLines, maxBytes int) (text string, startLine int64, partial bool, partialRaw int, shownRaw int64) {
	lines, trailing := rawLines(b)
	if len(lines) == 0 || maxLines <= 0 || maxBytes <= 0 {
		return "", 0, false, 0, 0
	}
	lineOf := func(i int) int64 {
		if startsLine {
			return totalLines - int64(len(lines)) + 1 + int64(i)
		}
		return totalLines - int64(len(lines)-1) + int64(i)
	}
	firstComplete := 0
	if !startsLine {
		firstComplete = 1
	}
	reversed, startLine, partial, partialRaw, shownRaw, used, allComplete := tailCompleteLines(lines, lineOf, firstComplete, maxLines, maxBytes, trailing)
	if !startsLine && allComplete {
		reversed, startLine, partial, partialRaw, shownRaw, _ = tailFirstSuffix(lines, lineOf, reversed, startLine, partial, partialRaw, shownRaw, used, maxLines, maxBytes, trailing)
	}
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	return strings.Join(reversed, "\n"), startLine, partial, partialRaw, shownRaw
}

func tailCompleteLines(lines []rawLine, lineOf func(int) int64, firstComplete, maxLines, maxBytes int, trailing bool) (reversed []string, startLine int64, partial bool, partialRaw int, shownRaw int64, used int, allComplete bool) {
	reversed = make([]string, 0, min(len(lines), maxLines))
	for i := len(lines) - 1; i >= firstComplete; i-- {
		if len(reversed) >= maxLines {
			return reversed, startLine, partial, partialRaw, shownRaw, used, false
		}
		line := lines[i]
		if used+len(line.text)+1 <= maxBytes {
			reversed = append(reversed, line.text)
			used += len(line.text) + 1
			startLine, partial, partialRaw = lineOf(i), false, 0
			shownRaw += int64(len(line.raw))
			if i < len(lines)-1 || trailing {
				shownRaw++
			}
			continue
		}
		if len(reversed) == 0 {
			if room := maxBytes - used; room > 0 {
				if piece, shown := suffixRawLine(line.raw, room); piece != "" {
					reversed = append(reversed, piece)
					startLine, partial, partialRaw = lineOf(i), true, shown
					shownRaw += int64(shown)
					if i < len(lines)-1 || trailing {
						shownRaw++
					}
				}
			}
		}
		return reversed, startLine, partial, partialRaw, shownRaw, used, false
	}
	return reversed, startLine, partial, partialRaw, shownRaw, used, true
}

// tailFirstSuffix extends a contiguous tail into the suffix of the line before
// it only when every complete line after that suffix was kept. On a no-room
// exit it returns the already selected range unchanged.
func tailFirstSuffix(lines []rawLine, lineOf func(int) int64, reversed []string, startLine int64, partial bool, partialRaw int, shownRaw int64, used, maxLines, maxBytes int, trailing bool) ([]string, int64, bool, int, int64, bool) {
	if len(reversed) == 0 {
		if piece, shown := suffixRawLine(lines[0].raw, maxBytes); piece != "" {
			total := int64(shown)
			if len(lines) > 1 || trailing {
				total++
			}
			return append(reversed, piece), lineOf(0), true, shown, total, true
		}
		return reversed, startLine, partial, partialRaw, shownRaw, false
	}
	if len(reversed) >= maxLines {
		return reversed, startLine, partial, partialRaw, shownRaw, false
	}
	room := maxBytes - used - 1
	if room <= 0 {
		return reversed, startLine, partial, partialRaw, shownRaw, false
	}
	piece, shown := suffixRawLine(lines[0].raw, room)
	if piece == "" {
		return reversed, startLine, partial, partialRaw, shownRaw, false
	}
	if len(lines) > 1 || trailing {
		shownRaw++
	}
	return append(reversed, piece), lineOf(0), true, shown, shownRaw + int64(shown), true
}

func suffixRawLine(raw []byte, maxDisplay int) (string, int) {
	if len(raw) == 0 {
		return "", 0
	}
	start := len(raw)
	used := 0
	for start > 0 {
		r, n := utf8.DecodeLastRune(raw[:start])
		text := raw[start-n : start]
		if r == utf8.RuneError && n == 1 {
			for start-n > 0 {
				prev, size := utf8.DecodeLastRune(raw[:start-n])
				if prev != utf8.RuneError || size != 1 {
					break
				}
				n += size
			}
			text = []byte("\uFFFD")
		}
		if used+len(text) > maxDisplay {
			break
		}
		used += len(text)
		start -= n
	}
	return strings.ToValidUTF8(string(raw[start:]), "\uFFFD"), len(raw) - start
}

func assemblePreview(s selection, marker string, trailingNewline bool) string {
	var b strings.Builder
	if s.headEnd > 0 {
		b.WriteString(s.head)
		b.WriteByte('\n')
	}
	b.WriteString(marker)
	if s.tailStart > 0 {
		b.WriteByte('\n')
		b.WriteString(s.tail)
		if trailingNewline && (s.tail == "" || strings.HasSuffix(s.tail, "\n")) {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// PreviewLines counts logical lines. A trailing newline terminates the last
// line; it does not start an empty one.
func PreviewLines(s string) int {
	if s == "" {
		return 0
	}
	n := strings.Count(s, "\n")
	if !strings.HasSuffix(s, "\n") {
		n++
	}
	return n
}

// pageCommand checks that the artifact is readable, then runs a reader that
// reports its own failure and never exceeds the preview allowance.
func pageCommand(command string) string {
	return command
}

func byteReadCommand(start, end int64, path string) string {
	return fmt.Sprintf("dd if=%s bs=1 skip=%d count=%d 2>/dev/null", path, start-1, end-start+1)
}

// previewMarker is one line so it costs one rendered line, and names the
// omitted range rather than repeating the shown lines.
func previewMarker(s selection, lines, total int64, o PreviewOptions) string {
	var b strings.Builder
	b.WriteString("... output truncated: ")
	switch {
	case s.headEnd > 0 && s.tailStart > 0 && s.tailStart <= s.headEnd:
		fmt.Fprintf(&b, "lines 1-%d and %d-%d of %d (line %d is split), ", s.headEnd, s.tailStart, lines, lines, s.tailStart)
	case s.headEnd > 0 && s.tailStart > 0:
		fmt.Fprintf(&b, "lines 1-%d and %d-%d of %d, ", s.headEnd, s.tailStart, lines, lines)
	case s.headEnd > 0:
		fmt.Fprintf(&b, "lines 1-%d of %d, ", s.headEnd, lines)
	case s.tailStart > 0:
		fmt.Fprintf(&b, "lines %d-%d of %d, ", s.tailStart, lines, lines)
	default:
		fmt.Fprintf(&b, "no lines of %d shown, ", lines)
	}
	if s.headPartial {
		fmt.Fprintf(&b, "head line %d cut after %d bytes, ", s.headEnd, s.headBytes)
	}
	if s.tailPartial {
		fmt.Fprintf(&b, "tail line %d cut to its last %d bytes, ", s.tailStart, s.tailBytes)
	}
	fmt.Fprintf(&b, "%d lines and %d bytes observed; ", lines, total)
	if o.CaptureError != "" {
		fmt.Fprintf(&b, "capture error: %s; ", o.CaptureError)
	} else if o.Incomplete {
		b.WriteString("capture incomplete: inherited output pipes remained open; ")
	}
	if o.Partial {
		b.WriteString("command was cancelled; output is partial; ")
	}
	issue := o.CaptureError != "" || o.Incomplete
	if o.Stored < total {
		var reasons []string
		if o.ArtifactLimit > 0 && o.Stored >= o.ArtifactLimit {
			reasons = append(reasons, "-output-limit")
		}
		if o.CaptureError != "" {
			reasons = append(reasons, "capture error")
		} else if o.Incomplete {
			reasons = append(reasons, "capture incomplete")
		}
		if len(reasons) == 0 {
			reasons = append(reasons, "artifact cut")
		}
		fmt.Fprintf(&b, "artifact keeps %d of %d bytes (%s); ", o.Stored, total, strings.Join(reasons, "; "))
	} else if o.Partial {
		b.WriteString("artifact retained; ")
	} else if issue {
		fmt.Fprintf(&b, "artifact keeps all %d observed bytes, but capture was not complete; ", total)
	} else {
		b.WriteString("artifact complete; ")
	}
	paging(&b, s, lines, total, o)
	return b.String()
}

func paging(b *strings.Builder, s selection, lines, total int64, o PreviewOptions) {
	start := s.headRaw
	if s.headEnd == 0 {
		start = 0
	}
	end := total - s.tailRaw
	if s.tailStart == 0 {
		end = total
	}
	if start < 0 {
		start = 0
	}
	if end > total {
		end = total
	}
	if start < end {
		label := ""
		if s.headEnd > 0 && s.tailStart > 0 && s.tailStart <= s.headEnd {
			label = fmt.Sprintf(" on line %d", s.headEnd)
		} else if first, last := omittedLines(s, lines); first > 0 && first <= last {
			label = fmt.Sprintf(" (lines %d-%d)", first, last)
		}
		if start >= o.Stored {
			fmt.Fprintf(b, "omitted bytes %d-%d%s; artifact stores only the first %d bytes at %s; omitted data is unavailable ...", start+1, end, label, o.Stored, o.Artifact)
			return
		}
		pageEnd := fitPage(o, start, min(end, o.Stored))
		if pageEnd <= start {
			fmt.Fprintf(b, "omitted bytes %d-%d%s; artifact stores only the first %d bytes; omitted data is unavailable ...", start+1, end, label, o.Stored)
			return
		}
		fmt.Fprintf(b, "omitted bytes %d-%d%s; page omitted bytes %d-%d: %s ...", start+1, end, label, start+1, pageEnd, pageCommand(byteReadCommand(start+1, pageEnd, o.Artifact)))
		return
	}
	if o.Stored > 0 {
		pageEnd := fitPage(o, 0, o.Stored)
		if pageEnd <= 0 {
			b.WriteString("artifact stores no output to page ...")
			return
		}
		fmt.Fprintf(b, "page stored artifact bytes 1-%d: %s ...", pageEnd, pageCommand(byteReadCommand(1, pageEnd, o.Artifact)))
		return
	}
	b.WriteString("artifact stores no output to page ...")
}

// renderRaw is the single UTF-8 expansion rule shared by preview rendering
// and recommended pages: an invalid run becomes one replacement character.
func renderRaw(raw []byte) string {
	return strings.ToValidUTF8(string(raw), "\uFFFD")
}

// pageEndForRaw is the largest raw prefix that renders within the preview
// limit and has at most the preview line limit. It scans at rune boundaries so
// it accounts for replacement expansion and a UTF-8 sequence cut at the end.
func pageEndForRaw(raw []byte, previewLimit int) int {
	if previewLimit <= 0 {
		return 0
	}
	rendered, lines, last := 0, 0, 0
	invalidRun := false
	for i := 0; i < len(raw); {
		r, size := utf8.DecodeRune(raw[i:])
		if r == utf8.RuneError && size == 1 {
			if !invalidRun {
				rendered += len("\uFFFD")
				invalidRun = true
			}
		} else {
			if r == '\n' {
				if lines >= MaxPreviewLines {
					return last
				}
				lines++
			}
			rendered += size
			invalidRun = false
		}
		if rendered > previewLimit {
			return last
		}
		i += size
		last = i
		if r == '\n' && lines == MaxPreviewLines {
			return last
		}
	}
	return last
}

func fitPage(o PreviewOptions, start, max int64) int64 {
	if max <= start {
		return start
	}
	if o.PageEnd != nil {
		if end := o.PageEnd(start, max); end > start && end <= max {
			max = min(max, start+int64(o.PreviewLimit))
			if end > max {
				end = max
			}
			return end
		}
	}
	return min(max, start+int64(o.PreviewLimit))
}

// omittedLines is the full line range that neither end shows. A partial head
// line omits its remainder, and a partial tail line omits its prefix, so those
// boundary lines are named as omitted too.
func omittedLines(s selection, lines int64) (int64, int64) {
	start, end := int64(1), lines
	if s.headEnd > 0 {
		start = s.headEnd
		if !s.headPartial {
			start++
		}
	}
	if s.tailStart > 0 {
		end = s.tailStart
		if !s.tailPartial {
			end--
		}
	}
	if start < 1 {
		start = 1
	}
	if end > lines {
		end = lines
	}
	if start > end {
		return 0, 0
	}
	return start, end
}

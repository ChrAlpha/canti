package project

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var skillNamePattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

type frontmatter struct {
	Name                   string
	Description            string
	DisableModelInvocation bool
}

// parseFrontmatter reads the small YAML subset a SKILL.md frontmatter needs:
// top-level name, description, and disable-model-invocation values as plain,
// single-quoted, double-quoted, or literal/folded block scalars. Other keys
// are ignored. A value that is not one of those forms is an error, so an
// ambiguous skill is skipped instead of guessed. truncated reports that the
// file exceeded the prefix the caller read; a frontmatter closed before that
// prefix is still usable.
func parseFrontmatter(content []byte, truncated bool) (frontmatter, error) {
	yaml, err := frontmatterYAML(content, truncated)
	if err != nil {
		return frontmatter{}, err
	}
	return parseFrontmatterLines(strings.Split(yaml, "\n"))
}

func frontmatterYAML(content []byte, truncated bool) (string, error) {
	text := string(content)
	text = strings.TrimPrefix(text, "\ufeff")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	if !strings.HasPrefix(text, "---\n") {
		return "", errors.New("missing frontmatter")
	}
	lines := strings.Split(text[4:], "\n")
	for i, line := range lines {
		if strings.TrimRight(line, " \t") == "---" {
			yaml := strings.Join(lines[:i], "\n") + "\n"
			if !utf8.ValidString(yaml) {
				return "", errors.New("frontmatter is not valid UTF-8")
			}
			return yaml, nil
		}
	}
	if truncated {
		return "", errors.New("frontmatter is not closed within the read prefix")
	}
	return "", errors.New("unterminated frontmatter")
}

func parseFrontmatterLines(lines []string) (frontmatter, error) {
	var out frontmatter
	seen := map[string]bool{}
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			return out, fmt.Errorf("unexpected indented line %q", line)
		}
		key, rest, ok := splitKey(line)
		if !ok {
			return out, fmt.Errorf("unexpected frontmatter line %q", line)
		}
		rest = strings.TrimSpace(rest)
		if key != "name" && key != "description" && key != "disable-model-invocation" {
			i += skipValue(rest, lines[i+1:])
			continue
		}
		if seen[key] {
			return out, fmt.Errorf("duplicate %s", key)
		}
		seen[key] = true
		if key == "name" || key == "description" {
			if err := validatePlainValue(rest); err != nil {
				return out, fmt.Errorf("%s: %w", key, err)
			}
		}
		if key == "disable-model-invocation" {
			if err := validateDisableValue(rest); err != nil {
				return out, fmt.Errorf("%s: %w", key, err)
			}
		}
		value, consumed, err := parseValue(rest, lines[i+1:])
		if err != nil {
			return out, fmt.Errorf("%s: %w", key, err)
		}
		i += consumed
		if err := applyFrontmatter(&out, key, value); err != nil {
			return out, err
		}
	}
	return out, nil
}

func validateDisableValue(rest string) error {
	value := stripComment(strings.TrimSpace(rest))
	if value == "" {
		return nil
	}
	if value[0] == '\'' || value[0] == '"' || value[0] == '>' || value[0] == '|' {
		return errors.New("disable-model-invocation must be an unquoted boolean")
	}
	switch strings.ToLower(value) {
	case "true", "false":
		return nil
	}
	return errors.New("disable-model-invocation must be true or false")
}

// validatePlainValue rejects the unquoted YAML forms that do not mean a
// string: null, booleans, numbers, and a nested mapping introduced by ": ".
func validatePlainValue(rest string) error {
	value := stripComment(strings.TrimSpace(rest))
	if value == "" {
		return nil
	}
	switch value[0] {
	case '\'', '"', '>', '|':
		return nil
	}
	if strings.HasPrefix(value, ":") || strings.Contains(value, ": ") {
		return errors.New("plain scalar is not an unambiguous string")
	}
	switch strings.ToLower(value) {
	case "null", "~", "true", "false", "yes", "no", "on", "off":
		return errors.New("value is not a string")
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return errors.New("value is a number, not a string")
	}
	return nil
}

func applyFrontmatter(out *frontmatter, key, value string) error {
	switch key {
	case "name":
		out.Name = strings.TrimSpace(value)
	case "description":
		out.Description = value
	case "disable-model-invocation":
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true":
			out.DisableModelInvocation = true
		case "false":
		default:
			return errors.New("disable-model-invocation must be true or false")
		}
	}
	return nil
}

func splitKey(line string) (key, rest string, ok bool) {
	at := strings.IndexByte(line, ':')
	if at <= 0 {
		return "", "", false
	}
	key = line[:at]
	if !validKey(key) || at+1 < len(line) && line[at+1] != ' ' {
		return "", "", false
	}
	return key, line[at+1:], true
}

func validKey(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return s != ""
}

func skipValue(_ string, following []string) int {
	return skipContinuation(following)
}

func skipContinuation(following []string) int {
	n := 0
	for n < len(following) {
		line := following[n]
		if line == "" || line[0] == ' ' || line[0] == '\t' {
			n++
			continue
		}
		break
	}
	return n
}

// parseValue parses one scalar value and returns the number of following
// lines it consumed.
func parseValue(rest string, following []string) (string, int, error) {
	if rest == "" {
		return "", 0, nil
	}
	switch rest[0] {
	case '>', '|':
		return parseBlock(rest, following)
	case '\'':
		value, err := parseSingleQuoted(rest)
		return value, 0, err
	case '"':
		value, err := parseDoubleQuoted(rest)
		return value, 0, err
	case '[', '{', '&', '*', '!', '%', '@', '`':
		return "", 0, fmt.Errorf("unsupported YAML value %q", rest)
	}
	if strings.HasPrefix(rest, "- ") || rest == "-" || strings.HasPrefix(rest, "? ") {
		return "", 0, fmt.Errorf("unsupported YAML value %q", rest)
	}
	value := stripComment(rest)
	if value == "" {
		return "", 0, errors.New("empty plain scalar")
	}
	consumed := 0
	for consumed < len(following) {
		line := following[consumed]
		if line == "" {
			break
		}
		if line[0] != ' ' && line[0] != '\t' {
			break
		}
		if line[0] == '\t' {
			return "", 0, errors.New("tab indentation is not supported")
		}
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			consumed++
			continue
		}
		return "", 0, errors.New("multiline plain scalars are not supported")
	}
	return value, consumed, nil
}

func parseBlock(rest string, following []string) (string, int, error) {
	kind, chomp, err := blockHeader(rest)
	if err != nil {
		return "", 0, err
	}
	raw, end, err := blockLines(following)
	if err != nil {
		return "", 0, err
	}
	return chompBlock(blockBody(kind, raw), chomp), end, nil
}

func blockHeader(rest string) (kind, chomp byte, err error) {
	kind = rest[0]
	header := strings.TrimSpace(rest[1:])
	if header != "" {
		switch header[0] {
		case '+', '-':
			chomp = header[0]
			header = strings.TrimSpace(header[1:])
		}
	}
	if strings.HasPrefix(header, "#") {
		header = ""
	}
	if header != "" {
		return 0, 0, fmt.Errorf("unsupported block scalar header %q", rest)
	}
	return kind, chomp, nil
}

func blockLines(following []string) ([]string, int, error) {
	end, indent := 0, -1
	for end < len(following) {
		line := following[end]
		if line == "" {
			end++
			continue
		}
		if line[0] == '\t' {
			return nil, 0, errors.New("tab indentation is not supported")
		}
		if line[0] != ' ' {
			break
		}
		spaces := leadingSpaces(line)
		if indent < 0 {
			indent = spaces
		}
		if spaces < indent {
			break
		}
		end++
	}
	if indent < 0 {
		return nil, end, nil
	}
	raw := make([]string, 0, end)
	for i := 0; i < end; i++ {
		line := following[i]
		if line == "" {
			raw = append(raw, "")
			continue
		}
		if len(line) < indent {
			return nil, 0, errors.New("inconsistent block scalar indentation")
		}
		raw = append(raw, line[indent:])
	}
	return raw, end, nil
}

func blockBody(kind byte, raw []string) string {
	if kind == '|' {
		return strings.Join(raw, "\n")
	}
	var b strings.Builder
	for i, line := range raw {
		if line == "" {
			b.WriteByte('\n')
			continue
		}
		if i > 0 && raw[i-1] != "" {
			if strings.HasPrefix(line, " ") || strings.HasPrefix(raw[i-1], " ") {
				b.WriteByte('\n')
			} else {
				b.WriteByte(' ')
			}
		}
		b.WriteString(line)
	}
	return b.String()
}

func chompBlock(value string, chomp byte) string {
	switch chomp {
	case '-':
		return strings.TrimRight(value, "\n")
	case '+':
		if value != "" && !strings.HasSuffix(value, "\n") {
			return value + "\n"
		}
		return value
	default:
		value = strings.TrimRight(value, "\n")
		if value != "" {
			value += "\n"
		}
		return value
	}
}

func leadingSpaces(s string) int {
	n := 0
	for n < len(s) && s[n] == ' ' {
		n++
	}
	return n
}

func parseSingleQuoted(rest string) (string, error) {
	var b strings.Builder
	for i := 1; i < len(rest); i++ {
		if rest[i] != '\'' {
			b.WriteByte(rest[i])
			continue
		}
		if i+1 < len(rest) && rest[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		if err := onlyComment(rest[i+1:]); err != nil {
			return "", err
		}
		return b.String(), nil
	}
	return "", errors.New("unterminated single-quoted scalar")
}

func parseDoubleQuoted(rest string) (string, error) {
	for i := 1; i < len(rest); i++ {
		if rest[i] == '\\' {
			i++
			continue
		}
		if rest[i] != '"' {
			continue
		}
		value, err := unquoteDouble(rest[:i+1])
		if err != nil {
			return "", err
		}
		if err := onlyComment(rest[i+1:]); err != nil {
			return "", err
		}
		return value, nil
	}
	return "", errors.New("unterminated double-quoted scalar")
}

func unquoteDouble(quoted string) (string, error) {
	if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
		return "", errors.New("invalid double-quoted scalar")
	}
	var b strings.Builder
	for i := 1; i < len(quoted)-1; i++ {
		if quoted[i] != '\\' {
			b.WriteByte(quoted[i])
			continue
		}
		i++
		if i >= len(quoted)-1 {
			return "", errors.New("trailing escape in double-quoted scalar")
		}
		switch quoted[i] {
		case '"', '\\', '/':
			b.WriteByte(quoted[i])
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case '0':
			b.WriteByte(0)
		case 'a':
			b.WriteByte(7)
		case 'e':
			b.WriteByte(27)
		case 'v':
			b.WriteByte(11)
		case ' ':
			b.WriteByte(' ')
		case 'N':
			b.WriteRune(0x85)
		case '_':
			b.WriteRune(0xa0)
		case 'L':
			b.WriteRune(0x2028)
		case 'P':
			b.WriteRune(0x2029)
		case 'x', 'u', 'U':
			digits := map[byte]int{'x': 2, 'u': 4, 'U': 8}[quoted[i]]
			if i+digits >= len(quoted) {
				return "", errors.New("short Unicode escape")
			}
			n, err := strconv.ParseUint(quoted[i+1:i+1+digits], 16, 32)
			if err != nil || n > utf8.MaxRune || n >= 0xD800 && n <= 0xDFFF {
				return "", errors.New("invalid Unicode escape")
			}
			b.WriteRune(rune(n))
			i += digits
		default:
			return "", fmt.Errorf("unsupported string escape \\%c", quoted[i])
		}
	}
	return b.String(), nil
}

func onlyComment(rest string) error {
	rest = strings.TrimSpace(rest)
	if rest == "" || strings.HasPrefix(rest, "#") {
		return nil
	}
	return fmt.Errorf("unexpected text after scalar %q", rest)
}

func stripComment(s string) string {
	if strings.HasPrefix(strings.TrimSpace(s), "#") {
		return ""
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '#' && i > 0 && (s[i-1] == ' ' || s[i-1] == '\t') {
			return strings.TrimSpace(s[:i])
		}
	}
	return strings.TrimSpace(s)
}

func validateSkillName(name string) error {
	if name == "" {
		return errors.New("skill name is empty")
	}
	if !utf8.ValidString(name) || len(name) > MaxSkillNameLength {
		return fmt.Errorf("skill name exceeds %d bytes or is not UTF-8", MaxSkillNameLength)
	}
	if !skillNamePattern.MatchString(name) {
		return fmt.Errorf("skill name %q must be lowercase letters, digits, and single hyphens", name)
	}
	return nil
}

func validateSkillDescription(description string) error {
	if strings.TrimSpace(description) == "" {
		return errors.New("skill description is required")
	}
	if !utf8.ValidString(description) {
		return errors.New("skill description is not UTF-8")
	}
	if utf8.RuneCountInString(description) > MaxSkillDescription {
		return fmt.Errorf("skill description exceeds %d characters", MaxSkillDescription)
	}
	return nil
}

package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"unicode/utf8"
)

// expand reads source again from the archive and checks its hash: an oracle
// for the evidence a retrieval returns.
func (r Reader) expand(ctx context.Context, source SourceRange) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if source.Offset < 0 || source.Length < 0 || source.Length > MaxBytes {
		return "", errors.New("invalid source range")
	}
	f, err := r.open(source.Path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("source is not a regular file")
	}
	data := make([]byte, source.Length)
	if _, err := f.ReadAt(data, source.Offset); err != nil {
		return "", fmt.Errorf("source range unavailable: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if digest(data) != source.SHA256 {
		return "", errors.New("source range changed: sha256 mismatch")
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return "", errors.New("source range is not UTF-8 text")
	}
	return string(data), nil
}

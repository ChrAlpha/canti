package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

var errInvalidDecision = errors.New("invalid decision")

// Decode validates and decodes exactly one decision object.
//
// The parser is intentionally strict: it rejects malformed JSON, trailing
// values, duplicate fields, unknown fields, missing fields, and invalid field
// types. It performs no I/O and keeps no state between calls.
func Decode(raw []byte) (Decision, error) {
	if !utf8.Valid(raw) {
		return Decision{}, fmt.Errorf("core: %w: invalid UTF-8", errInvalidDecision)
	}
	kind, payload, err := decodeObject(raw)
	if err != nil {
		return Decision{}, fmt.Errorf("core: %w: %v", errInvalidDecision, err)
	}
	decision := Decision{Kind: Kind(kind), Payload: payload}
	if decision.Kind != Exec && decision.Kind != Return {
		return Decision{}, fmt.Errorf("core: %w: unsupported kind %q", errInvalidDecision, kind)
	}
	return decision, nil
}

func decodeObject(raw []byte) (string, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return "", "", err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return "", "", errors.New("top-level value must be an object")
	}

	var kind, payload string
	var hasKind, hasPayload bool
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return "", "", err
		}
		key, ok := keyToken.(string)
		if !ok {
			return "", "", errors.New("object key must be a string")
		}
		switch key {
		case "kind":
			if hasKind {
				return "", "", fmt.Errorf("duplicate field %q", key)
			}
			hasKind = true
		case "payload":
			if hasPayload {
				return "", "", fmt.Errorf("duplicate field %q", key)
			}
			hasPayload = true
		default:
			return "", "", fmt.Errorf("unknown field %q", key)
		}
		value, err := decoder.Token()
		if err != nil {
			return "", "", err
		}
		str, ok := value.(string)
		if !ok {
			return "", "", fmt.Errorf("%s must be a string", key)
		}
		if key == "kind" {
			kind = str
		} else {
			payload = str
		}
	}

	if _, err := decoder.Token(); err != nil {
		return "", "", err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return "", "", fmt.Errorf("trailing value %v", token)
		}
		return "", "", err
	}
	if !hasKind || !hasPayload {
		return "", "", errors.New("expected kind and payload")
	}
	return kind, payload, nil
}

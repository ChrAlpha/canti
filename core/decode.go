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
	fields, err := decodeObject(raw)
	if err != nil {
		return Decision{}, fmt.Errorf("core: %w: %v", errInvalidDecision, err)
	}

	if len(fields) != 2 {
		return Decision{}, fmt.Errorf("core: %w: expected kind and payload", errInvalidDecision)
	}

	for field := range fields {
		if field != "kind" && field != "payload" {
			return Decision{}, fmt.Errorf("core: %w: unknown field %q", errInvalidDecision, field)
		}
	}

	var kind *string
	if err := json.Unmarshal(fields["kind"], &kind); err != nil || kind == nil {
		return Decision{}, fmt.Errorf("core: %w: kind must be a string", errInvalidDecision)
	}
	var payload *string
	if err := json.Unmarshal(fields["payload"], &payload); err != nil || payload == nil {
		return Decision{}, fmt.Errorf("core: %w: payload must be a string", errInvalidDecision)
	}

	decision := Decision{Kind: Kind(*kind), Payload: *payload}
	if decision.Kind != Exec && decision.Kind != Return {
		return Decision{}, fmt.Errorf("core: %w: unsupported kind %q", errInvalidDecision, *kind)
	}
	return decision, nil
}

func decodeObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, errors.New("top-level value must be an object")
	}

	fields := make(map[string]json.RawMessage, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("object key must be a string")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate field %q", key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing value %v", token)
		}
		return nil, err
	}
	return fields, nil
}

package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

var errInvalidDecision = errors.New("invalid decision")

// Decode validates and decodes exactly one decision object.
//
// The parser is intentionally strict: it rejects malformed JSON, trailing
// values, duplicate fields, unknown fields, missing fields, and invalid field
// types. Kind and payload are required; purpose is optional. An exec payload
// must not be blank. It performs no I/O and keeps no state between calls.
func Decode(raw []byte) (Decision, error) {
	if !utf8.Valid(raw) {
		return Decision{}, fmt.Errorf("core: %w: invalid UTF-8", errInvalidDecision)
	}
	fields, err := decodeObject(raw)
	if err != nil {
		return Decision{}, fmt.Errorf("core: %w: %v", errInvalidDecision, err)
	}
	purpose, hasPurpose := fields["purpose"]
	required := len(fields)
	if hasPurpose {
		required--
	}
	if required != 2 {
		return Decision{}, fmt.Errorf("core: %w: expected kind and payload", errInvalidDecision)
	}
	for field := range fields {
		if field != "kind" && field != "payload" && field != "purpose" {
			return Decision{}, fmt.Errorf("core: %w: unknown field %q", errInvalidDecision, field)
		}
	}
	kind := fields["kind"]
	if kind == nil {
		return Decision{}, fmt.Errorf("core: %w: kind must be a string", errInvalidDecision)
	}
	payload := fields["payload"]
	if payload == nil {
		return Decision{}, fmt.Errorf("core: %w: payload must be a string", errInvalidDecision)
	}
	if hasPurpose && purpose == nil {
		return Decision{}, fmt.Errorf("core: %w: purpose must be a string", errInvalidDecision)
	}
	decision := Decision{Kind: Kind(*kind), Payload: *payload}
	if purpose != nil {
		decision.Purpose = *purpose
	}
	if decision.Kind != Exec && decision.Kind != Return {
		return Decision{}, fmt.Errorf("core: %w: unsupported kind %q", errInvalidDecision, *kind)
	}
	if decision.Kind == Exec && strings.TrimSpace(decision.Payload) == "" {
		return Decision{}, fmt.Errorf("core: %w: exec payload is blank", errInvalidDecision)
	}
	return decision, nil
}

func decodeObject(raw []byte) (map[string]*string, error) {
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

	fields := make(map[string]*string, 2)
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
		var value *string
		if err := decoder.Decode(&value); err != nil {
			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				return nil, err
			}
			value = nil
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

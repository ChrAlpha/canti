package archive

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
)

var errCursorEncoding = errors.New("cursor is not unpadded base64url")

// encodeCursor and decodeCursor are the wire format of a directory search or
// discovery continuation: unpadded base64url of exactly one JSON object.
func encodeCursor(v any) string {
	data, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(cursor string, v any) error {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return errCursorEncoding
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected exactly one cursor object")
	}
	return nil
}

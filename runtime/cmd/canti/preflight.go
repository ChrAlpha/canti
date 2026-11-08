package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	hostserver "github.com/ChrAlpha/canti/runtime/internal/hostrpc"

	"github.com/ChrAlpha/canti/runtime/internal/host"
	"github.com/ChrAlpha/canti/sdk/go/hostrpc"
)

// preflight is a bounded, versioned, side-effect-free executable interface.
// It does not open a writable host or create its state directory.
func preflight(ctx context.Context, in io.Reader, out io.Writer) int {
	req, err := decodePreflight(in)
	resp := hostrpc.Response{Protocol: hostrpc.Protocol}
	if err == nil && req.Protocol != hostrpc.Protocol {
		resp.Error = &hostrpc.Error{Code: hostrpc.CodeProtocol, Message: "runtime and client versions differ; install matching canti and canti-tui executables"}
	} else if err == nil && req.Op != hostrpc.OpValidate {
		err = errors.New("preflight requires validate")
	}
	if err == nil && resp.Error == nil {
		resp.Validated, err = hostserver.Validate(ctx, req.Validate, nil)
	}
	if err != nil {
		code := hostrpc.CodeFailed
		if errors.As(err, new(host.SpecError)) {
			code = hostrpc.CodeSpec
		}
		resp.Error = &hostrpc.Error{Code: code, Message: err.Error()}
	}
	if json.NewEncoder(out).Encode(resp) != nil {
		return exitError
	}
	return exitOK
}

// decodePreflight bounds the complete stream and requires exactly one value.
// Version refusal precedes interpreting fields of another protocol.
func decodePreflight(in io.Reader) (hostrpc.Request, error) {
	const limit = 1 << 20
	var req hostrpc.Request
	body, err := io.ReadAll(io.LimitReader(in, limit+1))
	if err != nil {
		return req, err
	}
	if len(body) > limit {
		return req, errors.New("preflight request exceeds 1 MiB")
	}
	var version struct {
		Protocol int `json:"protocol"`
	}
	if err := json.Unmarshal(body, &version); err != nil {
		return req, err
	}
	if version.Protocol != hostrpc.Protocol {
		req.Protocol = version.Protocol
		return req, nil
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	err = d.Decode(&req)
	return req, err
}

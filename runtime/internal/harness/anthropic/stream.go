package anthropic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/harness/provider"
)

// readMessageStream assembles the final Messages object from the stream's
// events. A stream without message_stop is an incomplete attempt even if it
// carried a whole response in message_start; partial content never becomes a
// decision. A fallback content block is the documented server-side fallback
// boundary: content before it is discarded for the decision, and the serving
// model is the one its to.model names.
func readMessageStream(s *provider.Stream, limit int64) ([]byte, []byte, harness.Usage, bool, error) {
	a := &messageAssembler{responseHeader: s.Header, status: s.Status, limit: limit,
		redact: s.RedactJSON, redactValue: s.RedactValue, ident: s.Ident}
	for {
		event, err := s.Next()
		if err != nil {
			body := a.failureBody()
			if errors.Is(err, io.EOF) {
				return body, nil, a.observedUsage(), a.invalidUTF8, fmt.Errorf("anthropic: stream ended before message_stop: %w", io.ErrUnexpectedEOF)
			}
			return body, nil, a.observedUsage(), a.invalidUTF8, err
		}
		done, err := a.consume(event)
		if err != nil {
			return a.failureBody(), nil, a.observedUsage(), a.invalidUTF8, err
		}
		if !done {
			continue
		}
		body, raw, err := a.complete()
		if err != nil {
			return a.failureBody(), nil, a.observedUsage(), a.invalidUTF8, err
		}
		usage, usageErr := a.currentUsage()
		if usageErr != nil {
			return body, raw, usage, a.invalidUTF8, usageErr
		}
		return body, raw, usage, a.invalidUTF8, nil
	}
}

type streamBlock struct {
	raw     json.RawMessage
	fields  map[string]json.RawMessage
	kind    string
	ident   func(string) string
	mutable bool
	dirty   bool
	closed  bool

	text         strings.Builder
	thinking     strings.Builder
	signature    strings.Builder
	signatureSet bool
	input        []byte
	inputSet     bool
	citations    []json.RawMessage
}

type messageAssembler struct {
	start        json.RawMessage
	startFields  map[string]json.RawMessage
	startExact   bool
	startUsage   json.RawMessage
	blocks       []*streamBlock
	blockBase    int
	nextIndex    int
	fallbackAt   int
	fallbackOpen bool
	seenStart    bool
	seenDelta    bool
	stopped      bool

	contentTouched bool
	stopReason     json.RawMessage
	stopReasonSet  bool
	stopSequence   json.RawMessage
	stopSeqSet     bool
	usage          json.RawMessage
	usageSet       bool
	outputReported bool
	snapshot       harness.Usage
	snapshotSet    bool
	snapshotErr    error
	extra          map[string]json.RawMessage

	size    int64
	limit   int64
	status  int
	rawBody []byte

	responseHeader http.Header
	errorBody      json.RawMessage
	redact         func([]byte) []byte
	redactValue    func(string) string
	ident          func(string) string
	invalidUTF8    bool
}

func (a *messageAssembler) consume(event provider.Event) (bool, error) {
	raw := event.RawData
	if !utf8.Valid(raw) {
		a.invalidUTF8 = true
	}
	if len(raw) == 0 {
		raw = event.Data
	}
	fields, ok := provider.ExactObject(raw)
	if !ok {
		return false, errors.New("anthropic: malformed stream event")
	}
	typ := provider.Discriminator(fields, "type")
	if typ == "" {
		return false, errors.New("anthropic: stream event has no type")
	}
	name := event.RawName
	if name == "" {
		name = event.Name
	}
	if name != "" && name != typ {
		return false, fmt.Errorf("anthropic: stream event %q carries a different type", a.ident(name))
	}
	if !protocolEvent(typ) {
		return false, fmt.Errorf("anthropic: unknown stream event %q", a.ident(typ))
	}
	switch typ {
	case "message_start":
		return false, a.startMessage(fields)
	case "content_block_start":
		return false, a.startBlock(fields)
	case "content_block_delta":
		return false, a.deltaBlock(fields)
	case "content_block_stop":
		return false, a.stopBlock(fields)
	case "message_delta":
		return false, a.deltaMessage(fields, fields)
	case "message_stop":
		if !a.seenStart || !a.seenDelta {
			return false, errors.New("anthropic: message_stop before message_start and message_delta")
		}
		a.stopped = true
		return true, nil
	case "ping":
		return false, nil
	case "error":
		return false, a.streamError(raw, a.redact(raw))
	default:
		return false, fmt.Errorf("anthropic: unknown stream event %q", a.ident(typ))
	}
}

func (a *messageAssembler) startMessage(fields map[string]json.RawMessage) error {
	if a.seenStart {
		return errors.New("anthropic: duplicate message_start")
	}
	raw := provider.ExactField(fields, "message")
	if len(raw) == 0 || raw[0] != '{' || !json.Valid(raw) {
		return errors.New("anthropic: message_start has no valid message object")
	}
	startFields, exact := provider.ExactObject(raw)
	exact = exact && provider.NoCaseAliases(startFields, "id", "type", "role", "model", "content", "stop_reason", "stop_sequence", "usage")
	if exact {
		var err error
		exact, err = a.readStart(startFields)
		if err != nil {
			return err
		}
	}
	if !exact {
		startFields, a.blocks = nil, nil
	}
	a.start, a.startFields, a.startExact = bytes.Clone(raw), startFields, exact
	a.seenStart = true
	return a.grow(len(raw))
}

func (a *messageAssembler) readStart(fields map[string]json.RawMessage) (bool, error) {
	if raw, ok := fields["content"]; ok {
		if len(raw) == 0 || raw[0] != '[' {
			return false, errors.New("anthropic: message_start content is not an array")
		}
		var content []json.RawMessage
		if err := json.Unmarshal(raw, &content); err != nil {
			return false, fmt.Errorf("anthropic: malformed message_start content: %w", err)
		}
		a.blocks = make([]*streamBlock, len(content))
		a.nextIndex = len(content)
		for i, block := range content {
			parsed, err := newStreamBlock(block)
			if err != nil {
				return false, fmt.Errorf("anthropic: message_start block %d: %w", i, err)
			}
			parsed.ident = a.ident
			a.blocks[i] = parsed
		}
	}
	for _, name := range []string{"id", "type", "role", "model"} {
		if raw, ok := fields[name]; ok {
			if _, err := strictString(raw); err != nil {
				return false, fmt.Errorf("anthropic: message_start %s is not a string", name)
			}
		}
	}
	if raw, ok := fields["stop_reason"]; ok && !stringOrNull(raw) {
		return false, errors.New("anthropic: message_start stop_reason is not a string or null")
	}
	if raw, ok := fields["stop_sequence"]; ok && !stringOrNull(raw) {
		return false, errors.New("anthropic: message_start stop_sequence is not a string or null")
	}
	if raw, ok := fields["usage"]; ok && string(raw) != "null" {
		if _, ok := provider.ExactObject(raw); !ok {
			return false, errors.New("anthropic: message_start usage is not an exact object")
		}
		a.startUsage = bytes.Clone(raw)
	}
	return true, nil
}

func newStreamBlock(raw json.RawMessage) (*streamBlock, error) {
	b := &streamBlock{raw: bytes.Clone(raw)}
	fields, ok := provider.ExactObject(raw)
	if !ok {
		return nil, errors.New("content block has duplicate or malformed fields")
	}
	if !provider.NoCaseAliases(fields, "type", "text", "thinking", "signature", "input", "citations", "data") {
		return nil, errors.New("content block has a case-variant field")
	}
	kind, err := streamString(fields, "type")
	if err != nil {
		return nil, err
	}
	b.fields, b.kind = fields, kind
	switch kind {
	case "text":
		err = b.readText()
	case "thinking":
		err = b.readThinking()
	case "tool_use":
		err = b.readToolUse()
	case "redacted_thinking":
		err = b.readData()
	}
	return b, err
}

func (b *streamBlock) readText() error {
	if raw, ok := b.fields["text"]; ok {
		text, err := strictString(raw)
		if err != nil {
			return err
		}
		b.text.WriteString(text)
	}
	if raw, ok := b.fields["citations"]; ok && string(raw) != "null" && (len(raw) == 0 || raw[0] != '[') {
		return errors.New("text citations are not an array")
	}
	b.mutable = true
	return nil
}

func (b *streamBlock) readThinking() error {
	if raw, ok := b.fields["thinking"]; ok {
		thinking, err := strictString(raw)
		if err != nil {
			return err
		}
		b.thinking.WriteString(thinking)
	}
	if raw, ok := b.fields["signature"]; ok {
		signature, err := strictString(raw)
		if err != nil {
			return err
		}
		b.signature.WriteString(signature)
		b.signatureSet = true
	}
	b.mutable = true
	return nil
}

func (b *streamBlock) readToolUse() error {
	if raw, ok := b.fields["input"]; ok && (len(raw) == 0 || raw[0] != '{') {
		return errors.New("tool input is not an object")
	}
	b.mutable = true
	return nil
}

func (b *streamBlock) readData() error {
	if raw, ok := b.fields["data"]; ok {
		if _, err := strictString(raw); err != nil {
			return err
		}
	}
	return nil
}

func (a *messageAssembler) startBlock(fields map[string]json.RawMessage) error {
	if !a.seenStart {
		return errors.New("anthropic: content block before message_start")
	}
	if a.seenDelta {
		return errors.New("anthropic: content block after message_delta")
	}
	index, err := streamIndex(fields)
	if err != nil {
		return err
	}
	raw := provider.ExactField(fields, "content_block")
	if len(raw) == 0 || raw[0] != '{' || !json.Valid(raw) {
		return errors.New("anthropic: content_block_start has no valid block")
	}
	blockFields, ok := provider.ExactObject(raw)
	if !ok {
		return errors.New("anthropic: content block has duplicate or malformed fields")
	}
	if kind := provider.ExactString(blockFields, "type"); kind == "fallback" {
		return a.startFallback(index, blockFields)
	}
	if index != a.nextIndex {
		return fmt.Errorf("anthropic: content block %d follows %d blocks", index, a.nextIndex)
	}
	block, err := newStreamBlock(raw)
	if err != nil {
		return fmt.Errorf("anthropic: content block %d: %w", index, err)
	}
	block.ident = a.ident
	a.blocks = append(a.blocks, block)
	a.nextIndex = index + 1
	a.contentTouched = true
	return a.grow(len(raw))
}

func (a *messageAssembler) startFallback(index int, fields map[string]json.RawMessage) error {
	if index != a.nextIndex {
		return fmt.Errorf("anthropic: fallback block %d follows %d blocks", index, a.nextIndex)
	}
	if !provider.NoCaseAliases(fields, "type", "to") {
		return errors.New("anthropic: fallback block has a case-variant field")
	}
	to, ok := provider.ExactObject(provider.ExactField(fields, "to"))
	if !ok || !provider.NoCaseAliases(to, "model") {
		return errors.New("anthropic: fallback block has no to.model")
	}
	model, err := streamString(to, "model")
	if err != nil || model == "" {
		return errors.New("anthropic: fallback block has no to.model")
	}
	a.blocks = nil
	a.blockBase = index + 1
	a.nextIndex = index + 1
	a.fallbackAt, a.fallbackOpen = index, true
	a.contentTouched = true
	a.size = int64(len(a.start))
	a.extra = nil
	a.usage, a.usageSet, a.outputReported = nil, false, false
	a.snapshot, a.snapshotSet, a.snapshotErr = harness.Usage{}, false, nil
	a.stopReason, a.stopReasonSet, a.stopSequence, a.stopSeqSet = nil, false, nil, false
	return a.addExtra("model", to["model"])
}

func (a *messageAssembler) deltaBlock(fields map[string]json.RawMessage) error {
	if a.seenDelta {
		return errors.New("anthropic: content delta after message_delta")
	}
	index, err := streamIndex(fields)
	if err != nil {
		return err
	}
	block, err := a.block(index)
	if err != nil {
		return err
	}
	if block.closed {
		return fmt.Errorf("anthropic: content block %d is already stopped", index)
	}
	delta, ok := provider.ExactObject(provider.ExactField(fields, "delta"))
	if !ok {
		return fmt.Errorf("anthropic: content block %d has no valid delta", index)
	}
	if !provider.NoCaseAliases(delta, "type", "text", "thinking", "signature", "partial_json", "citation") {
		return fmt.Errorf("anthropic: content block %d delta has a case-variant field", index)
	}
	added, err := block.apply(provider.Discriminator(delta, "type"), delta)
	if err != nil {
		return fmt.Errorf("anthropic: content block %d: %w", index, err)
	}
	if err := a.grow(added); err != nil {
		return err
	}
	a.contentTouched = true
	return nil
}

func (a *messageAssembler) stopBlock(fields map[string]json.RawMessage) error {
	if a.seenDelta {
		return errors.New("anthropic: content stop after message_delta")
	}
	index, err := streamIndex(fields)
	if err != nil {
		return err
	}
	if a.fallbackOpen && index == a.fallbackAt {
		a.fallbackOpen = false
		return nil
	}
	block, err := a.block(index)
	if err != nil {
		return err
	}
	if block.closed {
		return fmt.Errorf("anthropic: content block %d stopped twice", index)
	}
	if block.kind == "tool_use" && block.inputSet && len(block.input) > 0 {
		if !validToolInput(block.input) {
			return fmt.Errorf("anthropic: content block %d tool input is not a JSON object", index)
		}
	}
	block.closed = true
	return nil
}

func (a *messageAssembler) deltaMessage(fields, rawFields map[string]json.RawMessage) error {
	if !a.seenStart {
		return errors.New("anthropic: message_delta before message_start")
	}
	if !provider.NoCaseAliases(fields, "type", "delta", "usage", "model") {
		return errors.New("anthropic: message_delta has a case-variant field")
	}
	delta, ok := provider.ExactObject(provider.ExactField(fields, "delta"))
	if !ok {
		return errors.New("anthropic: message_delta has no valid delta")
	}
	if !provider.NoCaseAliases(delta, "stop_reason", "stop_sequence", "stop_details", "model") {
		return errors.New("anthropic: message_delta delta has a case-variant field")
	}
	if err := a.applyMessageDelta(delta); err != nil {
		return err
	}
	if rawDelta, ok := provider.ExactObject(provider.ExactField(rawFields, "delta")); ok {
		if rawStop := provider.ExactField(rawDelta, "stop_reason"); len(rawStop) > 0 {
			if !stringOrNull(rawStop) {
				return errors.New("anthropic: message_delta stop_reason is not a string or null")
			}
			var value string
			if json.Unmarshal(rawStop, &value) == nil {
				a.stopReason = marshalAlways(canonicalStopReason(value))
			} else {
				a.stopReason = bytes.Clone(rawStop)
			}
			a.stopReasonSet = true
		}
	}
	if err := a.applyMessageEvent(fields); err != nil {
		return err
	}
	if raw, ok := fields["usage"]; ok && string(raw) != "null" {
		if err := a.mergeDeltaUsage(raw); err != nil {
			return err
		}
	}
	a.seenDelta = true
	return nil
}

func (a *messageAssembler) applyMessageDelta(delta map[string]json.RawMessage) error {
	for key, raw := range delta {
		switch key {
		case "stop_reason":
			continue
		case "stop_sequence":
			if !stringOrNull(raw) {
				return errors.New("anthropic: message_delta stop_sequence is not a string or null")
			}
			a.stopSequence, a.stopSeqSet = bytes.Clone(raw), true
		case "model":
			if _, err := strictString(raw); err != nil {
				return err
			}
			if err := a.addExtra(key, raw); err != nil {
				return err
			}
		default:
			if err := a.addExtra(key, raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *messageAssembler) applyMessageEvent(fields map[string]json.RawMessage) error {
	for key, raw := range fields {
		switch key {
		case "type", "delta", "usage":
			continue
		case "model":
			if _, err := strictString(raw); err != nil {
				return err
			}
		}
		if err := a.addExtra(key, raw); err != nil {
			return err
		}
	}
	return nil
}

func (a *messageAssembler) addExtra(key string, raw json.RawMessage) error {
	if a.extra == nil {
		a.extra = map[string]json.RawMessage{}
	}
	old := 0
	if existing, ok := a.extra[key]; ok {
		old = len(existing)
	} else if existing, ok := a.startFields[key]; ok {
		old = len(existing)
	}
	if err := a.adjustSize(int64(len(raw) - old)); err != nil {
		return err
	}
	a.extra[key] = bytes.Clone(raw)
	return nil
}

func (a *messageAssembler) mergeDeltaUsage(raw json.RawMessage) error {
	if _, ok := provider.ExactObject(raw); !ok {
		return errors.New("anthropic: message_delta usage is not an exact object")
	}
	base := a.usage
	if !a.usageSet {
		base = a.startUsage
	}
	merged, err := mergeUsage(base, raw)
	if err != nil {
		return err
	}
	probe := append([]byte(`{"usage":`), merged...)
	probe = append(probe, '}')
	usage, _, usageErr := provider.Usage(probe, true)
	if usageErr != nil {
		return usageErr
	}
	if err := a.adjustSize(int64(len(merged) - len(base))); err != nil {
		return err
	}
	a.usage, a.usageSet = merged, true
	a.snapshot.InputTokens, a.snapshot.InputReported = usage.InputTokens, usage.InputReported
	if delta, ok := provider.ExactObject(raw); ok {
		if _, supplied := delta["output_tokens"]; supplied && usage.OutputReported {
			a.outputReported = true
			a.snapshot.OutputTokens, a.snapshot.OutputReported = usage.OutputTokens, true
		}
	}
	a.snapshotSet, a.snapshotErr = true, nil
	return nil
}

func (a *messageAssembler) block(index int) (*streamBlock, error) {
	if index < a.blockBase || index >= a.blockBase+len(a.blocks) {
		return nil, fmt.Errorf("anthropic: content block %d does not exist", index)
	}
	return a.blocks[index-a.blockBase], nil
}

func (a *messageAssembler) streamError(rawData, body []byte) error {
	a.errorBody = bytes.Clone(body)
	fields, ok := provider.ExactObject(rawData)
	if !ok {
		return errors.New("anthropic: malformed stream error event")
	}
	if !provider.NoCaseAliases(fields, "type", "error", "request_id") {
		return errors.New("anthropic: stream error event has a case-variant field")
	}
	errorFields, nested := provider.ExactObject(provider.ExactField(fields, "error"))
	if !nested || !provider.NoCaseAliases(errorFields, "type", "message", "code") {
		return errors.New("anthropic: malformed stream error event")
	}
	rawType := provider.Discriminator(errorFields, "type")
	typ := canonicalType(rawType)
	message := a.redactValue(provider.ExactString(errorFields, "message"))
	requestID := a.redactValue(provider.ExactString(fields, "request_id"))
	if requestID == "" {
		requestID = a.redactValue(a.responseHeader.Get("request-id"))
	}
	text := fmt.Sprintf("anthropic: status %d", a.status)
	if typ != "" {
		text += " " + typ
	}
	if message != "" {
		text += ": " + message
	}
	if requestID != "" {
		text += " (request_id " + requestID + ")"
	}
	err := errors.New(text)
	switch rawType {
	case "billing_error":
		return &harness.StopError{Reason: core.StopBudgetExhausted, Err: err}
	case "invalid_request_error":
		if strings.Contains(strings.ToLower(provider.ExactString(errorFields, "message")), "credit balance") {
			return &harness.StopError{Reason: core.StopBudgetExhausted, Err: err}
		}
		return provider.Unrecoverable(err)
	case "authentication_error", "permission_error", "not_found_error", "request_too_large":
		return provider.Unrecoverable(err)
	default:
		return err
	}
}

func (a *messageAssembler) failureBody() []byte {
	if len(a.errorBody) > 0 {
		return a.redact(a.errorBody)
	}
	body, _ := a.partial()
	return body
}

func (a *messageAssembler) observedUsage() harness.Usage {
	usage, _ := a.currentUsage()
	return usage
}

func (a *messageAssembler) currentUsage() (harness.Usage, error) {
	if !a.snapshotSet {
		if err := a.captureStartUsage(); err != nil {
			return a.snapshot, err
		}
	}
	return a.snapshot, a.snapshotErr
}

func (a *messageAssembler) captureStartUsage() error {
	if len(a.startUsage) == 0 || string(a.startUsage) == "null" {
		a.snapshot, a.snapshotSet = harness.Usage{}, true
		return nil
	}
	probe := append([]byte(`{"usage":`), a.startUsage...)
	probe = append(probe, '}')
	usage, _, err := provider.Usage(probe, true)
	if err != nil {
		a.snapshot, a.snapshotSet, a.snapshotErr = harness.Usage{}, true, err
		return err
	}
	usage.OutputTokens, usage.OutputReported = 0, false
	a.snapshot, a.snapshotSet = usage, true
	return nil
}

func (a *messageAssembler) partial() ([]byte, error) {
	if !a.seenStart {
		return nil, nil
	}
	if !a.contentTouched && !a.stopReasonSet && !a.usageSet && len(a.extra) == 0 {
		return a.redact(bytes.Clone(a.start)), nil
	}
	return a.build(true)
}

func (a *messageAssembler) complete() ([]byte, []byte, error) {
	if !a.seenStart || !a.seenDelta || !a.stopped {
		return nil, nil, errors.New("anthropic: incomplete message stream")
	}
	if a.fallbackOpen {
		return nil, nil, errors.New("anthropic: fallback block was not stopped")
	}
	if a.contentTouched {
		for _, block := range a.blocks {
			if !block.closed {
				return nil, nil, errors.New("anthropic: content block was not stopped")
			}
		}
	}
	if !a.contentTouched && !a.stopReasonSet && !a.usageSet && len(a.extra) == 0 {
		raw := bytes.Clone(a.start)
		a.rawBody = raw
		return a.redact(raw), raw, nil
	}
	body, err := a.build(false)
	return body, a.rawBody, err
}

func (a *messageAssembler) build(partial bool) ([]byte, error) {
	if !a.startExact || a.startFields == nil {
		return nil, errors.New("anthropic: cannot assemble an ambiguous message_start")
	}
	fields := make(map[string]json.RawMessage, len(a.startFields)+len(a.extra)+4)
	for key, value := range a.startFields {
		fields[key] = value
	}
	for key, value := range a.extra {
		fields[key] = value
	}
	content := make([]json.RawMessage, len(a.blocks))
	for i, block := range a.blocks {
		raw, err := block.render(partial)
		if err != nil {
			return nil, err
		}
		content[i] = raw
	}
	rawContent, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	fields["content"] = rawContent
	if a.stopReasonSet {
		fields["stop_reason"] = a.stopReason
	} else if _, ok := fields["stop_reason"]; !ok {
		fields["stop_reason"] = []byte("null")
	}
	if a.stopSeqSet {
		fields["stop_sequence"] = a.stopSequence
	}
	usage := a.startUsage
	if a.usageSet {
		usage = a.usage
	} else if a.seenDelta {
		usage, err = withoutOutputTokens(a.startUsage)
		if err != nil {
			return nil, err
		}
	}
	if len(usage) > 0 && !a.outputReported {
		usage, err = withoutOutputTokens(usage)
		if err != nil {
			return nil, err
		}
	}
	if len(usage) > 0 {
		fields["usage"] = usage
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	if a.limit > 0 && int64(len(body)) > a.limit {
		return nil, fmt.Errorf("anthropic: response exceeds %d bytes", a.limit)
	}
	a.rawBody = body
	return a.redact(body), nil
}

func (b *streamBlock) apply(typ string, delta map[string]json.RawMessage) (int, error) {
	if !b.mutable || b.fields == nil {
		return 0, errors.New("content block cannot carry a delta")
	}
	if !protocolKind(typ) {
		return 0, fmt.Errorf("anthropic: unknown content delta %q", b.ident(typ))
	}
	switch typ {
	case "text_delta":
		if b.kind != "text" {
			return 0, errors.New("text delta on a non-text block")
		}
		text, err := streamString(delta, "text")
		if err != nil {
			return 0, err
		}
		b.text.WriteString(text)
		b.dirty = true
		return len(text), nil
	case "thinking_delta":
		if b.kind != "thinking" {
			return 0, errors.New("thinking delta on a non-thinking block")
		}
		text, err := streamString(delta, "thinking")
		if err != nil {
			return 0, err
		}
		b.thinking.WriteString(text)
		b.dirty = true
		return len(text), nil
	case "signature_delta":
		if b.kind != "thinking" {
			return 0, errors.New("signature delta on a non-thinking block")
		}
		signature, err := streamString(delta, "signature")
		if err != nil {
			return 0, err
		}
		b.signature.WriteString(signature)
		b.signatureSet, b.dirty = true, true
		return len(signature), nil
	case "input_json_delta":
		if b.kind != "tool_use" {
			return 0, errors.New("tool input delta on a non-tool block")
		}
		partial, err := streamString(delta, "partial_json")
		if err != nil {
			return 0, err
		}
		b.input = append(b.input, partial...)
		b.inputSet, b.dirty = true, true
		return len(partial), nil
	case "citations_delta":
		if b.kind != "text" {
			return 0, errors.New("citation delta on a non-text block")
		}
		citation := provider.ExactField(delta, "citation")
		if len(citation) == 0 || !json.Valid(citation) {
			return 0, errors.New("citation delta has no valid citation")
		}
		b.citations = append(b.citations, bytes.Clone(citation))
		b.dirty = true
		return len(citation), nil
	default:
		return 0, fmt.Errorf("unknown delta type %q", b.ident(typ))
	}
}

func (b *streamBlock) render(partial bool) ([]byte, error) {
	if !b.dirty {
		return b.raw, nil
	}
	fields := make(map[string]json.RawMessage, len(b.fields)+2)
	for key, value := range b.fields {
		fields[key] = value
	}
	switch b.kind {
	case "text":
		fields["text"] = marshalAlways(b.text.String())
		if len(b.citations) > 0 {
			raw, err := json.Marshal(b.citations)
			if err != nil {
				return nil, err
			}
			fields["citations"] = raw
		}
	case "thinking":
		fields["thinking"] = marshalAlways(b.thinking.String())
		if b.signatureSet {
			fields["signature"] = marshalAlways(b.signature.String())
		}
	case "tool_use":
		if b.inputSet {
			input := b.input
			if len(input) == 0 {
				input = []byte("{}")
			}
			if !validToolInput(input) {
				if partial {
					break
				}
				return nil, errors.New("anthropic: streamed tool input is not a JSON object")
			}
			fields["input"] = bytes.Clone(input)
		}
	default:
		return b.raw, nil
	}
	return json.Marshal(fields)
}

func (a *messageAssembler) grow(n int) error {
	if n <= 0 {
		return nil
	}
	return a.adjustSize(int64(n))
}

func (a *messageAssembler) adjustSize(delta int64) error {
	next := a.size + delta
	if next < 0 {
		next = 0
	}
	if a.limit > 0 && next > a.limit {
		return fmt.Errorf("anthropic: response exceeds %d bytes", a.limit)
	}
	a.size = next
	return nil
}

func streamIndex(fields map[string]json.RawMessage) (int, error) {
	raw := provider.ExactField(fields, "index")
	if len(raw) == 0 || string(raw) == "null" || raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return 0, errors.New("anthropic: stream event has no valid content index")
	}
	var index int
	if json.Unmarshal(raw, &index) != nil || index < 0 {
		return 0, errors.New("anthropic: stream event has no valid content index")
	}
	return index, nil
}

func streamString(fields map[string]json.RawMessage, name string) (string, error) {
	return strictString(provider.ExactField(fields, name))
}

func strictString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", errors.New("stream field is not a string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("stream field is not a string: %w", err)
	}
	return value, nil
}

func stringOrNull(raw json.RawMessage) bool {
	return string(raw) == "null" || len(raw) > 0 && raw[0] == '"' && json.Unmarshal(raw, new(string)) == nil
}

func validToolInput(raw []byte) bool {
	return len(raw) > 0 && raw[0] == '{' && json.Valid(raw)
}

func marshalAlways(value string) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

func mergeUsage(start, delta json.RawMessage) (json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if len(start) > 0 && string(start) != "null" {
		var ok bool
		if fields, ok = provider.ExactObject(start); !ok {
			return nil, errors.New("anthropic: message_start usage is not an exact object")
		}
	}
	deltaFields, ok := provider.ExactObject(delta)
	if !ok {
		return nil, errors.New("anthropic: message_delta usage is not an exact object")
	}
	for key, value := range deltaFields {
		fields[key] = value
	}
	return json.Marshal(fields)
}

func withoutOutputTokens(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return raw, nil
	}
	fields, ok := provider.ExactObject(raw)
	if !ok {
		return nil, errors.New("anthropic: message_start usage is not an exact object")
	}
	delete(fields, "output_tokens")
	return json.Marshal(fields)
}

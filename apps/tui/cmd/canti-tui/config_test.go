package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/apps/tui/internal/chat"
)

func TestChatFlagsDefaults(t *testing.T) {
	o, code, err := parse([]string{"-ephemeral"}, &bytes.Buffer{})
	if err != nil || code != chat.OK {
		t.Fatalf("parse = %d, %v", code, err)
	}
	if o.cfg.Spec.MaxSteps != 500 || o.cfg.Spec.WallTime != "4h0m0s" || o.cfg.Spec.ContextTokens != 0 {
		t.Fatalf("defaults = %+v", o.cfg.Spec)
	}
	var usage bytes.Buffer
	if _, _, err := parse([]string{"-h"}, &usage); err == nil || !strings.Contains(usage.String(), "required") || !strings.Contains(usage.String(), "model's input window or lower") {
		t.Fatalf("context-tokens help = %q, %v", usage.String(), err)
	}
}

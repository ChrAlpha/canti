package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
	"github.com/ChrAlpha/canti/internal/harness/sqlite"
)

func TestCompactionInspectionShowsFrozenBytesWithoutChangingExecution(t *testing.T) {
	command := `canti:retrieve {"operation":"list","path":"/canti/runs","reason":"` + strings.Repeat("locate earlier records ", 30) + `"}`
	f := newFixture(t, map[int]core.Decision{1: {Kind: core.Exec, Payload: command}, 2: {Kind: core.Exec, Payload: command}, 3: {Kind: core.Return, Payload: "done"}})
	if r := f.cli(nil, "run", "-goal", "Inspect committed context.", "-compact-steps", "2", "-retain-bytes", "1"); r.code != exitOK {
		t.Fatalf("run: %+v", r)
	}
	before := f.history()
	calls := len(f.model.requests()) + len(f.model.summaryRequests())
	r := f.cli(nil, "compactions", "-limit", "1")
	var page struct {
		RunID   string                    `json:"run_id"`
		Records []sqlite.CompactionRecord `json:"records"`
		Next    int64                     `json:"next_after"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &page); r.code != exitOK || err != nil || len(page.Records) != 1 || page.Next <= 0 {
		t.Fatalf("page %+v %v", r, err)
	}
	c := page.Records[0].Operation
	if !c.Accepted || len(c.Attempts) != 1 || !c.Attempts[0].Done || len(c.Plan.Request.Body) != 0 || len(c.Attempts[0].Response.Body) != 0 {
		t.Fatalf("operation %+v", c)
	}
	raw := f.cli(nil, "compactions", "-id", c.Plan.ID, "-raw")
	if raw.code != exitOK || !bytes.Equal([]byte(raw.stdout), f.model.summaryRequests()[0]) {
		t.Fatal("diagnostics changed frozen request bytes")
	}
	full := f.cli(nil, "compactions", "-id", c.Plan.ID, "-bodies")
	var got harness.Compaction
	if err := json.Unmarshal([]byte(full.stdout), &got); full.code != exitOK || err != nil || !bytes.Equal(got.Plan.Request.Body, []byte(raw.stdout)) || len(got.Attempts[0].Response.Body) == 0 {
		t.Fatalf("full operation: %v %+v", err, full)
	}
	status := f.status()
	if status.Totals.SummaryInputTokens <= 0 || status.Totals.ReservedUnknownTokens != 0 {
		t.Fatalf("summary usage %+v", status.Totals)
	}
	if after := f.history(); !reflect.DeepEqual(before, after) || calls != len(f.model.requests())+len(f.model.summaryRequests()) {
		t.Fatal("inspection mutated the run or sent a request")
	}
	for _, args := range [][]string{{"-raw"}, {"-id", c.Plan.ID, "-raw", "-bodies"}, {"-after", "-1"}, {"-limit", "101"}} {
		if r := f.cli(nil, "compactions", args...); r.code != exitUsage {
			t.Fatalf("accepted invalid flags: %+v", r)
		}
	}
}

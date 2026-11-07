package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/harness"
)

func encodedContextEventBytes(t *testing.T, events []harness.Transition) int64 {
	t.Helper()
	var total int64
	for _, tr := range events {
		for _, v := range []any{tr.Event, tr.From, tr.To, tr.Facts} {
			body, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			total += int64(len(body))
		}
	}
	return total
}

func TestContextRecordsBoundsLargePartialResponses(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", core.Limits{MaxSteps: 2, MaxAttempts: 4}))
	d.freeze()
	for range 2 {
		d.send()
		d.do(core.ResponseIncomplete{Step: 1, Attempt: d.snap.State.Attempt}, harness.Facts{Failure: "partial", Response: &harness.Response{Body: []byte(strings.Repeat("x", 8<<20))}})
	}
	d.send()
	var got []int64
	for after := int64(0); ; {
		p, err := s.ContextRecords(context.Background(), "run-1", 1, 1, after, 100)
		if err != nil {
			t.Fatal(err)
		}
		if size := encodedContextEventBytes(t, p.Events); size > 16<<20 {
			t.Fatalf("retained %d encoded bytes in one event page", size)
		}
		if p.EventBytes != encodedContextEventBytes(t, p.Events) {
			t.Fatal("reported bytes differ from retained event JSON")
		}
		for _, event := range p.Events {
			got = append(got, event.Seq)
		}
		if !p.More {
			break
		}
		if len(p.Events) == 0 || p.Events[len(p.Events)-1].Seq <= after {
			t.Fatal("byte page did not advance")
		}
		after = p.Events[len(p.Events)-1].Seq
	}
	if len(got) != 5 {
		t.Fatalf("lost or repeated events: %v", got)
	}
	for i, seq := range got {
		if seq != int64(i+2) {
			t.Fatalf("event sequence: %v", got)
		}
	}
}

func TestContextRecordsDoesNotDecodeLookahead(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.decide([]byte(`{"kind":"return","payload":"done"}`), harness.Usage{})
	if _, err := s.db.Exec("UPDATE transitions SET facts = '{' WHERE seq = 3"); err != nil {
		t.Fatal(err)
	}
	p, err := s.ContextRecords(context.Background(), "run-1", 1, 1, 0, 1)
	if err != nil || len(p.Events) != 1 || p.Events[0].Seq != 2 || !p.More {
		t.Fatalf("lookahead blocked selected page: %+v %v", p, err)
	}
	if _, err := s.ContextRecords(context.Background(), "run-1", 1, 1, 2, 1); err == nil {
		t.Fatal("selected corrupt event was accepted")
	}
}

func TestContextRecordsOversizedDecisionAdvancesAlone(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	body, _ := json.Marshal(core.Decision{Kind: core.Exec, Payload: "canti:" + strings.Repeat("z", 9<<20)})
	d.decide(body, harness.Usage{})
	d.perform()
	p, err := s.ContextRecords(context.Background(), "run-1", 1, 1, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Events) != 2 || !p.More {
		t.Fatalf("oversized state/event joined first page: %d events, more=%t", len(p.Events), p.More)
	}
	p, err = s.ContextRecords(context.Background(), "run-1", 1, 1, p.Events[1].Seq, 100)
	if err != nil || len(p.Events) != 1 || !p.More || p.Events[0].To.Phase != core.PhaseEffectSaved || encodedContextEventBytes(t, p.Events) <= 16<<20 || p.EventBytes != encodedContextEventBytes(t, p.Events) {
		t.Fatalf("oversized event did not advance alone: events=%d more=%t error=%v", len(p.Events), p.More, err)
	}
	p, err = s.ContextRecords(context.Background(), "run-1", 1, 1, p.Events[0].Seq, 100)
	if err != nil || len(p.Events) != 1 || p.More || p.Events[0].To.Step != 2 {
		t.Fatalf("lost event following oversized singleton: %+v %v", p, err)
	}
}

func TestContextRecordsByteBoundaryUsesPersistedUTF8JSON(t *testing.T) {
	s := open(t, tempPath(t))
	d := newDriver(t, s, createRun(t, s, "run-1", limits))
	d.freeze()
	d.send()
	prefix := strings.Repeat("证据\n\"\\\x00", 32)
	facts := harness.Facts{Failure: prefix}
	d.do(core.ResponseIncomplete{Step: 1, Attempt: 1}, facts)
	d.send()
	const sizes = `octet_length(event)+octet_length(from_state)+octet_length(to_state)+octet_length(facts)`
	var initial int64
	if err := s.db.QueryRow("SELECT sum(" + sizes + ") FROM transitions WHERE seq IN (2,3)").Scan(&initial); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int64{-1, 0, 1} {
		facts.Failure = prefix + strings.Repeat("x", int(ContextEventPageBytes+delta-initial))
		body, err := json.Marshal(facts)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec("UPDATE transitions SET facts=? WHERE seq=3", string(body)); err != nil {
			t.Fatal(err)
		}
		p, err := s.ContextRecords(context.Background(), "run-1", 1, 1, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		want := 2
		if delta > 0 {
			want = 1
		}
		if len(p.Events) != want || !p.More {
			t.Fatalf("limit%+d: %d events, more=%t", delta, len(p.Events), p.More)
		}
		var stored int64
		if err := s.db.QueryRow("SELECT sum("+sizes+") FROM transitions WHERE seq>1 AND seq<=?", p.Events[len(p.Events)-1].Seq).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if p.EventBytes != stored || stored != encodedContextEventBytes(t, p.Events) {
			t.Fatalf("limit%+d: report%d database%d", delta, p.EventBytes, stored)
		}
		if delta <= 0 && stored != ContextEventPageBytes+delta {
			t.Fatalf("wrong boundary fixture: %d", stored)
		}
	}
}

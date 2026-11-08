//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ChrAlpha/canti/runtime/core"
	"github.com/ChrAlpha/canti/runtime/internal/archive"
)

var (
	stressRound      = regexp.MustCompile(`(?m)^SESSION_STRESS_ROUND=(\d+)$`)
	stressRun        = regexp.MustCompile(`Run (run_[a-f0-9]+); anchor sequence`)
	stressNavigation = regexp.MustCompile(`Previous run: /canti/runs/(run_[a-f0-9]+)/`)
	stressOrigin     = regexp.MustCompile(`origin=(/canti/runs/run_[a-f0-9]+/steps/00000001/preview)`)
	stressLatest     = regexp.MustCompile(`Latest completed step (\d+), sequence (\d+);`)
	stressSession    = regexp.MustCompile(`(?m)^\{"session_id":"horizon",[^\n]+\}$`)
)

// The policy learns history only through the native reader. Reset deliberately
// discards its selection; the next read and focus explicitly restore evidence.
func sessionStressDecision(step int, prompt string) (core.Decision, error) {
	bad := func(format string, args ...any) (core.Decision, error) {
		return core.Decision{}, fmt.Errorf(format, args...)
	}
	rm, id := stressRound.FindStringSubmatch(prompt), stressRun.FindStringSubmatch(prompt)
	if len(rm) == 0 || len(id) == 0 {
		return bad("missing authoritative goal or run anchor")
	}
	round, _ := strconv.Atoi(rm[1])
	runID := id[1]
	root := "/canti/runs/" + runID
	previous := ""
	if round > 1 {
		nav := stressNavigation.FindStringSubmatch(prompt)
		if len(nav) == 0 {
			return bad("missing previous session navigation")
		}
		previous = nav[1]
	}
	read := func(path string) (core.Decision, error) {
		q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: path, Reason: "verify original session evidence", Limit: 8192})
		return core.Decision{Kind: core.Exec, Payload: "canti:retrieve " + string(q)}, nil
	}
	origin := root + "/steps/00000001/preview"
	if round > 1 {
		m := stressOrigin.FindStringSubmatch(prompt)
		if len(m) > 0 {
			origin = m[1]
		} else if step >= 3 && step != 4 && step != 8 {
			return bad("previous result lost its original evidence reference at step %d", step)
		}
	}
	switch step {
	case 1, 6, 11:
		stage := 0
		if step == 6 {
			stage = 1
		}
		if step == 11 {
			stage = 2
		}
		label := fmt.Sprintf("run-%02d:%c", round, 'A'+stage)
		command := fmt.Sprintf("set -eu; printf '%s\\n' >> effects.txt; touch ledger.txt; test \"$(wc -l < ledger.txt)\" -eq %d; printf '%s\\n' >> ledger.txt; tail -n 1 ledger.txt", label, (round-1)*3+stage, label)
		return core.Decision{Kind: core.Exec, Payload: command}, nil
	case 2, 4, 8:
		if round == 1 {
			return read(root + "/goal")
		}
		return read("/canti/runs/" + previous + "/result")
	case 3, 5, 9:
		latest := stressLatest.FindStringSubmatch(prompt)
		if len(latest) == 0 || latest[1] != strconv.Itoa(step-1) {
			return bad("missing preceding retrieval at step %d", step)
		}
		if round > 1 && !strings.Contains(prompt, fmt.Sprintf("completed run=%02d ", round-1)) {
			return bad("prior result body missing at step %d", step)
		}
		return core.Decision{Kind: core.Exec, Payload: fmt.Sprintf(`canti:focus {"sources":[{"seq":%s,"source":0}]}`, latest[2])}, nil
	case 7:
		return read(root + "/session")
	case 10:
		return read(origin)
	}
	links := 0
	if round == 10 {
		if step == 12 {
			return read("/canti/runs/" + previous + "/session")
		}
		var meta struct {
			Sequence int    `json:"sequence"`
			Previous string `json:"previous_run_id"`
		}
		if err := json.Unmarshal([]byte(stressSession.FindString(prompt)), &meta); err != nil {
			return bad("missing explicit session navigation at step %d: %v", step, err)
		}
		links = step - 12
		if meta.Sequence != round-links {
			return bad("navigation skipped session: sequence=%d links=%d", meta.Sequence, links)
		}
		if meta.Previous != "" {
			return read("/canti/runs/" + meta.Previous + "/session")
		}
		if links != 9 {
			return bad("session chain ended at %d links", links)
		}
	}
	return core.Decision{Kind: core.Return, Payload: fmt.Sprintf("completed run=%02d origin=%s links=%d", round, origin, links)}, nil
}

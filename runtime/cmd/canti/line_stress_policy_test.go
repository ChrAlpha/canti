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
	stressRound      = regexp.MustCompile(`(?m)^LINE_STRESS_ROUND=(\d+)$`)
	stressRun        = regexp.MustCompile(`Run (run_[a-f0-9]+); anchor sequence`)
	stressNavigation = regexp.MustCompile(`(?m)^- (run_[a-f0-9]+):`)
	stressOrigin     = regexp.MustCompile(`origin=(/canti/runs/run_[a-f0-9]+/steps/00000001/preview)`)
)

// The policy learns history only through the native reader. Each phase closes
// with a handover; a provider rejection keeps the minimal/index head. Later
// rounds read the predecessor's result, then retrieve the resolved origin
// record in its own step and check its evidence before completing the round.
func lineStressDecision(step int, prompt string) (core.Decision, error) {
	bad := func(format string, args ...any) (core.Decision, error) {
		return core.Decision{}, fmt.Errorf(format, args...)
	}
	rm, id := stressRound.FindStringSubmatch(prompt), stressRun.FindStringSubmatch(prompt)
	if len(rm) == 0 || len(id) == 0 {
		return bad("missing authoritative goal or run anchor")
	}
	round, _ := strconv.Atoi(rm[1])
	root := "/canti/runs/" + id[1]
	previous := ""
	if round > 1 {
		nav := stressNavigation.FindStringSubmatch(prompt)
		if len(nav) == 0 {
			return bad("missing previous-run navigation")
		}
		previous = nav[1]
	}
	read := func(path string) core.Decision {
		q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: path, Reason: "verify original line evidence", Limit: 8192})
		return core.Decision{Kind: core.Exec, Payload: "canti:retrieve " + string(q)}
	}
	// The origin is the first run's original evidence reference. It is read
	// back from the previous run's result in the resident epoch; no handover
	// note is rendered, so a step that does not need it proceeds without it.
	origin := ""
	if round == 1 {
		origin = root + "/steps/00000001/preview"
	} else if m := stressOrigin.FindStringSubmatch(prompt); len(m) > 0 {
		origin = m[1]
	}
	switch step {
	case 1, 6, 12:
		stage := 0
		if step == 6 {
			stage = 1
		}
		if step == 12 {
			stage = 2
			if !strings.Contains(prompt, origin) {
				return bad("the origin record path is missing from the resident epoch at step %d", step)
			}
			if !strings.Contains(prompt, "run-01:A") {
				return bad("the origin record's evidence is missing from the resident epoch at step %d", step)
			}
		}
		label := fmt.Sprintf("run-%02d:%c", round, 'A'+stage)
		command := fmt.Sprintf("set -eu; printf '%s\\n' >> effects.txt; touch ledger.txt; test \"$(wc -l < ledger.txt)\" -eq %d; printf '%s\\n' >> ledger.txt; tail -n 1 ledger.txt", label, (round-1)*3+stage, label)
		return core.Decision{Kind: core.Exec, Payload: command}, nil
	case 2, 4, 8:
		if round == 1 {
			return read(root + "/goal"), nil
		}
		return read("/canti/runs/" + previous + "/result"), nil
	case 3:
		if !strings.Contains(prompt, "### Step 2\n") {
			return bad("the preceding retrieval left the resident epoch at step 3")
		}
		return read(root + "/goal"), nil
	case 5, 9:
		if !strings.Contains(prompt, fmt.Sprintf("### Step %d\n", step-1)) {
			return bad("the preceding retrieval left the resident epoch at step %d", step)
		}
		marker := ""
		if round > 1 {
			marker = fmt.Sprintf(" completed run=%02d", round-1)
			if !strings.Contains(prompt, fmt.Sprintf("completed run=%02d", round-1)) {
				return bad("prior result body missing at step %d", step)
			}
		}
		if origin == "" {
			return core.Decision{Kind: core.Exec, Payload: fmt.Sprintf("canti:handover%s stage=%d", marker, step)}, nil
		}
		return core.Decision{Kind: core.Exec, Payload: fmt.Sprintf("canti:handover origin=%s%s stage=%d", origin, marker, step)}, nil
	case 7:
		return read(root + "/previous"), nil
	case 10:
		if round == 1 {
			return read(root + "/goal"), nil
		}
		return read("/canti/runs/" + previous + "/result"), nil
	case 11:
		if origin == "" {
			return bad("the resolved origin reference is missing at step %d", step)
		}
		return read(origin), nil
	}
	if origin == "" {
		return bad("previous result lost its original evidence reference at step %d", step)
	}
	return core.Decision{Kind: core.Return, Payload: fmt.Sprintf("completed run=%02d origin=%s", round, origin)}, nil
}

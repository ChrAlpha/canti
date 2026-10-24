//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ChrAlpha/canti/core"
	"github.com/ChrAlpha/canti/internal/archive"
)

var (
	stressRound      = regexp.MustCompile(`(?m)^SESSION_STRESS_ROUND=(\d+)$`)
	stressRun        = regexp.MustCompile(`Every completed step of this run[^\n]+/canti/runs/(run_[a-f0-9]+)/steps/`)
	stressNavigation = regexp.MustCompile(`Browse older records through /canti/runs/(run_[a-f0-9]+)/session`)
	stressOrigin     = regexp.MustCompile(`origin=(/canti/runs/run_[a-f0-9]+/steps/00000001/preview)`)
	stressWindow     = regexp.MustCompile(`This window begins at step (\d+)\.`)
	stressCheckpoint = regexp.MustCompile(`(?m)^SESSION_STRESS_STATE=(\{[^\n]+\})$`)
	stressSession    = regexp.MustCompile(`(?m)^\{"session_id":"horizon",[^\n]+\}$`)
)

type sessionStressState struct {
	Run      string `json:"run"`
	Phase    int    `json:"phase"`
	Previous string `json:"previous"`
	Origin   string `json:"origin"`
	Cursor   string `json:"cursor"`
	Links    int    `json:"links"`
}

func sessionStressDecision(_ int, prompt string) (core.Decision, error) {
	bad := func(format string, args ...any) (core.Decision, error) {
		return core.Decision{}, fmt.Errorf(format, args...)
	}
	roundMatch, runMatch, windowMatch := stressRound.FindStringSubmatch(prompt), stressRun.FindStringSubmatch(prompt), stressWindow.FindStringSubmatch(prompt)
	if len(roundMatch) == 0 || len(runMatch) == 0 || len(windowMatch) == 0 {
		return bad("missing current goal, run, or window")
	}
	round, _ := strconv.Atoi(roundMatch[1])
	runID := runMatch[1]
	s, err := stressState(prompt, round, runID)
	if err != nil {
		return bad("state: %v", err)
	}
	_, current, _ := strings.Cut(prompt, "## Current window steps\n")
	for _, block := range strings.Split("\n"+current, "\n### Step ")[1:] {
		if err := s.observe(round, runID, block); err != nil {
			return bad("round %d phase %d: %v", round, s.Phase, err)
		}
	}
	read := func(path string) (core.Decision, error) {
		q, _ := json.Marshal(archive.RetrievalQuery{Operation: "read", Path: path, Reason: "verify original session evidence", Limit: 8192})
		return core.Decision{Kind: core.Exec, Payload: "canti:retrieve " + string(q)}, nil
	}
	root := "/canti/runs/" + runID
	switch s.Phase {
	case 0, 4, 8:
		stage := s.Phase / 4
		label := fmt.Sprintf("run-%02d:%c", round, 'A'+stage)
		command := fmt.Sprintf("set -eu; printf '%s\\n' >> effects.txt; touch ledger.txt; test \"$(wc -l < ledger.txt)\" -eq %d; printf '%s\\n' >> ledger.txt; tail -n 1 ledger.txt", label, (round-1)*3+stage, label)
		return core.Decision{Kind: core.Exec, Payload: command}, nil
	case 1:
		if round == 1 {
			return read(root + "/goal")
		}
		return read("/canti/runs/" + s.Previous + "/result")
	case 2:
		if round == 10 {
			return read("/canti/runs/" + s.Cursor + "/session")
		}
		return read(root + "/session")
	case 3, 9:
		return read(s.Origin)
	case 5:
		return read(root + "/goal")
	case 6:
		return core.Decision{Kind: core.Exec, Payload: fmt.Sprintf("test \"$(wc -l < ledger.txt)\" -eq %d && printf 'verified run=%02d\\n'", (round-1)*3+2, round)}, nil
	case 7:
		return read(root + "/steps/00000001/preview")
	case 10:
		return core.Decision{Kind: core.Return, Payload: fmt.Sprintf("completed run=%02d origin=%s links=%d", round, s.Origin, s.Links)}, nil
	}
	return bad("unknown phase %d", s.Phase)
}

func (s *sessionStressState) observe(round int, runID, block string) error {
	require := func(want string) error {
		if !strings.Contains(block, want) {
			return fmt.Errorf("missing %q in current observation", want)
		}
		return nil
	}
	read := func(path string) error {
		if err := require("Retrieved original evidence:\n"); err != nil {
			return err
		}
		return require("\nSource " + path + ":\n")
	}
	root := "/canti/runs/" + runID
	switch s.Phase {
	case 0, 4, 8:
		if err := require(fmt.Sprintf("Preview:\n```\nrun-%02d:%c\n", round, 'A'+s.Phase/4)); err != nil {
			return err
		}
	case 1:
		if round == 1 {
			if err := read(root + "/goal"); err != nil {
				return err
			}
		} else {
			if err := read("/canti/runs/" + s.Previous + "/result"); err != nil {
				return err
			}
			if err := require(fmt.Sprintf("completed run=%02d ", round-1)); err != nil {
				return err
			}
			origin := stressOrigin.FindStringSubmatch(block)
			if len(origin) == 0 {
				return fmt.Errorf("previous result has no original evidence reference")
			}
			s.Origin = origin[1]
		}
	case 2:
		id := runID
		if round == 10 {
			id = s.Cursor
		}
		if err := read("/canti/runs/" + id + "/session"); err != nil {
			return err
		}
		var meta struct {
			Sequence int    `json:"sequence"`
			Previous string `json:"previous_run_id"`
		}
		if err := json.Unmarshal([]byte(stressSession.FindString(block)), &meta); err != nil {
			return err
		}
		if round == 10 {
			if meta.Sequence != round-1-s.Links {
				return fmt.Errorf("navigation skipped a session run: %+v", meta)
			}
			s.Links++
			if meta.Previous != "" {
				s.Cursor = meta.Previous
				return nil
			}
			if meta.Sequence != 1 {
				return fmt.Errorf("navigation ended before sequence 1")
			}
			s.Origin = "/canti/runs/" + id + "/steps/00000001/preview"
		} else if meta.Sequence != round {
			return fmt.Errorf("wrong current session metadata: %+v", meta)
		}
	case 3, 9:
		if err := read(s.Origin); err != nil {
			return err
		}
		if err := require("\nrun-01:A\n"); err != nil {
			return err
		}
	case 5:
		if err := read(root + "/goal"); err != nil {
			return err
		}
		if err := require(fmt.Sprintf("SESSION_STRESS_ROUND=%d\n", round)); err != nil {
			return err
		}
	case 6:
		if err := require(fmt.Sprintf("Preview:\n```\nverified run=%02d\n", round)); err != nil {
			return err
		}
	case 7:
		if err := read(root + "/steps/00000001/preview"); err != nil {
			return err
		}
		if err := require(fmt.Sprintf("\nrun-%02d:A\n", round)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected observation after phase %d", s.Phase)
	}
	s.Phase++
	return nil
}

func stressState(prompt string, round int, runID string) (sessionStressState, error) {
	s := sessionStressState{}
	if h := stressCheckpoint.FindStringSubmatch(prompt); len(h) > 0 {
		if err := json.Unmarshal([]byte(h[1]), &s); err != nil {
			return s, err
		}
	}
	if s.Run != runID {
		s = sessionStressState{Run: runID, Previous: s.Run, Cursor: s.Run, Origin: s.Origin}
	}
	if round == 1 && s.Origin == "" {
		s.Origin = "/canti/runs/" + runID + "/steps/00000001/preview"
	}
	if round > 1 && s.Previous == "" {
		return s, fmt.Errorf("previous run missing from checkpoint")
	}
	return s, nil
}

func sessionStressSummary(body []byte) (string, error) {
	var envelope struct {
		Messages []struct{ Content []struct{ Text string } }
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return "", err
	}
	var text strings.Builder
	for _, m := range envelope.Messages {
		for _, b := range m.Content {
			text.WriteString(b.Text)
		}
	}
	prompt := text.String()
	runMatch := stressRun.FindStringSubmatch(prompt)
	if len(runMatch) == 0 {
		return "", fmt.Errorf("summary missing current run")
	}
	runID := runMatch[1]
	roundMatch := stressRound.FindStringSubmatch(prompt)
	if len(roundMatch) == 0 {
		return "", fmt.Errorf("summary missing current round")
	}
	round, _ := strconv.Atoi(roundMatch[1])
	s, err := stressState(prompt, round, runID)
	if err != nil {
		return "", err
	}
	_, current, _ := strings.Cut(prompt, "## Current window steps\n")
	for _, step := range strings.Split("\n"+current, "\n### Step ")[1:] {
		if err := s.observe(round, runID, step); err != nil {
			return "", err
		}
	}
	data, _ := json.Marshal(s)
	return "SESSION_STRESS_STATE=" + string(data), nil
}

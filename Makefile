.PHONY: build build-smoke check ci-integration ci-long ci-pty ci-unit commits fmt graph integration lint race smoke test test-names vet acceptance native-deps isolation ci-policy

empty :=
space := $(empty) $(empty)
anchored = ^($(subst $(space),|,$(strip $(1))))$$

LONG_TESTS = \
	TestContextCaptureBoundsRejectedRetrievalAcrossReopen \
	TestContextDiagnosticAcrossExecutionRetrievalHandoverAndRestart \
	TestDiscoveryCandidatesSurviveNoiseAndBinaryAcrossWindows \
	TestLoopBoundsRequestsAsHistoryGrows \
	TestNativeDiscoveryOwnRunAcrossRestartsAndWindows \
	TestRetrievalDiscoveryAcrossFreshPages \
	TestLineDialogueSurvivesHandoverRetryAndRestart \
	TestLineMultiRunWindowStress \
	TestLineUnknownResolutionPreservesTheRunningEffect \
	TestUnlimitedStepsAcrossCrashHandoverAndRestart

PTY_TESTS = \
	TestPTYNativeRawRestore \
	TestNativeInitFailureRestoresPTY \
	TestChatKeepsInheritedDescriptorsBlocking \
	TestChatPromptWriteBackpressure \
	TestChatPTYEnterSteersWhileRunning \
	TestChatPTYFollowUpCtrlFQueuesAndContinues \
	TestChatPTYHeldInputCtrlRRestores \
	TestChatPTYCtrlXCancelsTheEffect \
	TestChatPTYPasteSpansCompletionAndRequiresManualSend \
	TestChatPTYBusyScrollDoesNotQueueInput \
	TestChatPTYDisplayFailureWaitsForCancellation \
	TestChatPTYEndsWithUsageWhenNoInputFits \
	TestChatPTYExitPreservesCompleteTranscript \
	TestChatPTYHangupCancelsTheDockerJob \
	TestChatPTYHangupDetachesFromAHostProcess \
	TestChatPTYHangupStopsTheActiveRun \
	TestChatPTYInterruptsDockerJobAndKeepsLine \
	TestChatPTYResolutionGuidanceSurvivesAltScreen \
	TestChatPTYSecondInterruptExitsBlockedCleanupWithFullStderr \
	TestChatPTYSplitBracketedPasteSubmitsOneMultilineGoal \
	TestChatPTYStderrBackpressureCancelsActiveRun \
	TestChatPTYThirtyTwoReturnedRoundsKeepProgressAndDescriptorsBounded \
	TestChatTerminalKeepsReadAheadDraft \
	TestChatTerminalReopensPromptAfterStderrBackpressure

LONG = $(call anchored,$(LONG_TESTS))
PTY = $(call anchored,$(PTY_TESTS))
MODULES = scripts/run-modules.sh
CI_TEST = python3 $(CURDIR)/scripts/ci-shard.py $(CURDIR)/scripts/test-report.sh go test -json -count=1 -race
GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1

check: fmt graph vet test-names test acceptance native-deps ci-policy

ci-policy:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-ci-policy.py

native-deps:
	scripts/check-tui-deps.sh

fmt:
	test -z "$$(find runtime sdk/go apps/tui -name '*.go' -type f -exec gofmt -l {} +)"

graph:
	scripts/check-module-graph.sh

isolation:
	scripts/check-module-isolation.sh

build:
	scripts/build-modules.sh

build-smoke:
	scripts/build-modules.sh --smoke

smoke:
	scripts/smoke-executables.sh

vet:
	$(MODULES) go vet ./...
	$(MODULES) go vet -tags integration,live ./...
	cd apps/tui && GOWORK=off go vet -tags acceptance,integration ./...

lint:
	$(MODULES) $(GOLANGCI_LINT) run ./...
	$(MODULES) $(GOLANGCI_LINT) run --build-tags integration,live ./...
	cd apps/tui && GOWORK=off $(GOLANGCI_LINT) run --build-tags acceptance,integration ./...

commits:
	scripts/check-commits.sh "$(BASE)"

acceptance:
	scripts/run-acceptance.sh

test:
	$(MODULES) go test ./...

race:
	$(MODULES) go test -count=1 -race ./...

integration:
	$(MODULES) go test -count=1 -race -timeout=15m -tags integration ./...
	ACCEPTANCE_TAGS=acceptance,integration scripts/run-acceptance.sh -timeout=15m

ci-unit:
	$(MODULES) $(CI_TEST) -skip '$(PTY)' ./...
	ACCEPTANCE_REPORT=1 scripts/run-acceptance.sh -skip '$(PTY)'

ci-integration:
	$(MODULES) $(CI_TEST) -timeout=15m -tags integration -skip '$(LONG)|$(PTY)' ./...
	ACCEPTANCE_REPORT=1 ACCEPTANCE_TAGS=acceptance,integration scripts/run-acceptance.sh -timeout=15m -skip '$(LONG)|$(PTY)'

ci-long:
	CI_SHARD_BALANCE=long $(MODULES) $(CI_TEST) -timeout=30m -tags integration -run '$(LONG)' ./...
	CI_SHARD_BALANCE=long ACCEPTANCE_REPORT=1 ACCEPTANCE_TAGS=acceptance,integration scripts/run-acceptance.sh -timeout=30m -run '$(LONG)'

ci-pty: export CI_SHARD_COUNT = 1
ci-pty: export CI_SHARD_INDEX = 0
ci-pty:
	cd apps/tui && GOWORK=off $(CI_TEST) -p 1 -timeout=5m -run '$(PTY)' ./internal/terminal
	ACCEPTANCE_REPORT=1 ACCEPTANCE_TAGS=acceptance,integration scripts/run-acceptance.sh -p 1 -timeout=5m -run '$(PTY)'

test-names:
	LONG='$(LONG)' PTY='$(PTY)' scripts/check-test-names.sh

.PHONY: build build-smoke check ci-integration ci-long ci-pty ci-unit commits fmt graph integration lint race smoke test test-names vet

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
	TestSessionDialogueSurvivesHandoverRetryAndRestart \
	TestSessionGrowthRemainsBoundedAndIsolated \
	TestSessionMultiRunWindowStress \
	TestSessionUnknownResolutionPreservesTheRunningEffect \
	TestUnlimitedStepsAcrossCrashHandoverAndRestart

PTY_TESTS = \
	TestChatInputContinuesDroppingPageKeyAcrossRunCompletion \
	TestChatInputContinuesDroppingPasteAcrossRunCompletion \
	TestChatInputDrainedInterruptReachesProgram \
	TestChatInputReadsABlockingTerminalOnlyWhenInputIsReady \
	TestChatInputReadsItsOwnNonblockingDescription \
	TestChatInputTerminalReadNeverWaitsForInput \
	TestChatKeepsInheritedDescriptorsBlocking \
	TestChatPromptWriteBackpressure \
	TestChatPTYBusyInputDoesNotBecomeNextRun \
	TestChatPTYBusyPasteTailAfterReturnIsDiscarded \
	TestChatPTYBusyScrollDoesNotQueueInput \
	TestChatPTYDisplayFailureWaitsForCancellation \
	TestChatPTYEndsWithUsageWhenNoInputFits \
	TestChatPTYExitPreservesCompleteTranscript \
	TestChatPTYHangupCancelsTheDockerJob \
	TestChatPTYHangupDetachesFromAHostProcess \
	TestChatPTYHangupStopsTheActiveRun \
	TestChatPTYInterruptsDockerJobAndKeepsSession \
	TestChatPTYResolutionGuidanceSurvivesAltScreen \
	TestChatPTYSecondInterruptExitsBlockedCleanupWithFullStderr \
	TestChatPTYSplitBracketedPasteSubmitsOneMultilineGoal \
	TestChatPTYStderrBackpressureCancelsActiveRun \
	TestChatPTYThirtyTwoReturnedRoundsKeepProgressAndDescriptorsBounded \
	TestChatTerminalDiscardsBusyInput \
	TestChatTerminalReopensPromptAfterStderrBackpressure \
	TestChatTUIRejectsQueuedPreviousEpoch \
	TestChatTUITeardownCancellation

LONG = $(call anchored,$(LONG_TESTS))
PTY = $(call anchored,$(PTY_TESTS))
MODULES = scripts/run-modules.sh
CI_TEST = python3 $(CURDIR)/scripts/ci-shard.py $(CURDIR)/scripts/test-report.sh go test -json -count=1 -race
GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1

check: fmt graph vet test-names test

fmt:
	test -z "$$(find runtime sdk/go apps/tui -name '*.go' -type f -exec gofmt -l {} +)"

graph:
	scripts/check-module-graph.sh

build:
	scripts/build-modules.sh

build-smoke:
	scripts/build-modules.sh --smoke

smoke:
	scripts/smoke-executables.sh

vet:
	$(MODULES) go vet ./...
	$(MODULES) go vet -tags integration,live ./...

lint:
	$(MODULES) $(GOLANGCI_LINT) run ./...
	$(MODULES) $(GOLANGCI_LINT) run --build-tags integration,live ./...

commits:
	scripts/check-commits.sh "$(BASE)"

test:
	$(MODULES) go test ./...

race:
	$(MODULES) go test -count=1 -race ./...

integration:
	$(MODULES) go test -count=1 -race -timeout=15m -tags integration ./...

ci-unit:
	$(MODULES) $(CI_TEST) -skip '$(PTY)' ./...

ci-integration:
	$(MODULES) $(CI_TEST) -timeout=15m -tags integration -skip '$(LONG)|$(PTY)' ./...

ci-long:
	$(MODULES) $(CI_TEST) -timeout=30m -tags integration -run '$(LONG)' ./...

ci-pty:
	cd runtime && GOWORK=off $(CI_TEST) -p 1 -timeout=5m -tags integration -run '$(PTY)' ./cmd/canti
	cd apps/tui && GOWORK=off $(CI_TEST) -p 1 -timeout=5m -tags integration -run '$(PTY)' ./cmd/canti-tui

test-names:
	LONG='$(LONG)' PTY='$(PTY)' scripts/check-test-names.sh

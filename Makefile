.PHONY: check ci-integration ci-long ci-pty ci-unit fmt integration test test-names vet

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
	TestChatKeepsInheritedDescriptorsBlocking \
	TestChatPromptWriteBackpressure \
	TestChatPTYBusyInputDoesNotBecomeNextRun \
	TestChatPTYBusyPasteTailAfterReturnIsDiscarded \
	TestChatPTYBusyScrollDoesNotQueueInput \
	TestChatPTYDisplayFailureWaitsForCancellation \
	TestChatPTYEndsWithUsageWhenNoInputFits \
	TestChatPTYExitPreservesCompleteTranscript \
	TestChatPTYHangupCancelsTheDockerJob \
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
	TestChatTUITeardownCancellation \
	TestPromptFlushBarrierAndImmediateInput \
	TestPromptOutputBoundaries

LONG = $(call anchored,$(LONG_TESTS))
PTY = $(call anchored,$(PTY_TESTS))
CI_TEST = scripts/test-report.sh go test -json -count=1 -race

check: fmt vet test-names test

fmt:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...
	go vet -tags integration,live,legacygen ./...

test:
	go test ./...

integration:
	go test -count=1 -race -timeout=15m -tags integration ./...

ci-unit:
	$(CI_TEST) -skip '$(PTY)' ./...

ci-integration:
	$(CI_TEST) -timeout=15m -tags integration -skip '$(LONG)|$(PTY)' ./...

ci-long:
	$(CI_TEST) -timeout=30m -tags integration -run '$(LONG)' ./...

ci-pty:
	$(CI_TEST) -p 1 -timeout=5m -tags integration -run '$(PTY)' ./cmd/canti

test-names:
	LONG='$(LONG)' PTY='$(PTY)' scripts/check-test-names.sh

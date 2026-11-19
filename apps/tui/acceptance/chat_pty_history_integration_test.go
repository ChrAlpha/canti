//go:build acceptance && integration && linux

package acceptance

import "testing"

func TestChatPTYReopensEarlierStepsAndAnswer(t *testing.T) {
	f, image := dockerFixture(t, map[int]decision{
		1: {Kind: "exec", Payload: "printf 'history step output\\n'"},
		2: {Kind: "return", Payload: "history answer"},
	})
	p := startPTYChat(t, f, nil, "-image", image)
	ready := p.awaitStatus(t, "ready", 0)
	p.send(t, "history goal\r")
	p.await(t, "│ history step output", ready)
	answer := p.await(t, "history answer", ready)
	p.awaitStatus(t, "ready", answer)
	p.send(t, "\x03")
	p.finish(t, exitOK)

	again := startPTYChat(t, f, nil, "-image", image)
	again.awaitStatus(t, "ready", 0)
	again.await(t, "── earlier history ──", 0)
	again.await(t, "› history goal", 0)
	again.await(t, "$ printf 'history step output\\n'", 0)
	again.await(t, "│ history step output", 0)
	again.await(t, "history answer", 0)
	again.send(t, "\x03")
	again.finish(t, exitOK)
}

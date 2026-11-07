package harness

// DecisionToolName is the tool a model calls with its decision when the
// run's decision format is tool; the tool system prompt names it.
const DecisionToolName = "canti_decision"

// DecisionToolDescription describes DecisionToolName to the model.
const DecisionToolDescription = "Submit the next canti decision as tool input instead of replying with JSON text."

// DecisionSchema is the JSON Schema of a decision object, in the strict
// subset every supported model API accepts: kind, purpose, and payload, all
// required, in that order, and nothing else. core still decodes a decision
// without a purpose.
const DecisionSchema = `{"type":"object","properties":{"kind":{"type":"string","enum":["exec","return"]},` +
	`"purpose":{"type":"string","description":"One short line: what this step is for and what you will do next with its result."},` +
	`"payload":{"type":"string"}},"required":["kind","payload","purpose"],"additionalProperties":false}`

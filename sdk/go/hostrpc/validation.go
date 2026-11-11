package hostrpc

import (
	"context"
	"errors"
)

// Validation is a side-effect-free request to the runtime's `preflight`
// executable mode. Credentials travel over stdin, never command arguments.
// It is also accepted by a running host through OpValidate. A non-fresh
// submission without ExplicitConstraints receives the latest run's constraints
// from the host; a fresh one keeps its own. Validation neither reserves
// admission nor performs input replay lookup.
type Validation struct {
	State      string     `json:"state"`
	Submission Submission `json:"submission"`
	Binding    Binding    `json:"binding"`
}

// Validated contains canonical configuration and the constraints/limits
// required to present input errors. Admission always checks them again.
type Validated struct {
	State            string `json:"state"`
	Spec             Spec   `json:"spec"`
	Constraints      string `json:"constraints"`
	GoalLimit        int    `json:"goal_limit"`
	ConstraintsLimit int    `json:"constraints_limit"`
}

// Validate checks a prospective submission through the runtime without admitting a
// run or creating state. The same envelope is used by `canti preflight` when
// no host serves the state directory yet.
func (c *Client) Validate(ctx context.Context, v Validation) (Validated, error) {
	resp, err := c.Call(ctx, Request{Op: OpValidate, Validate: &v})
	if err != nil {
		return Validated{}, err
	}
	if resp.Validated == nil {
		return Validated{}, errors.New("hostrpc: validation without an answer")
	}
	return *resp.Validated, nil
}

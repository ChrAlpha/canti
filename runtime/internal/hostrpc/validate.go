package hostrpc

import (
	"cmp"
	"context"
	"errors"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

// Validate owns runtime validation for both the socket and preflight process.
// It writes no facts and performs no model request or executor launch.
func Validate(ctx context.Context, v *Validation, h *host.Host) (*Validated, error) {
	if v == nil {
		return nil, errors.New("validation request is required")
	}
	spec, err := SpecFrom(v.Submission.Spec)
	if err != nil {
		return nil, host.SpecError{Err: err}
	}
	if h != nil {
		if err := h.CheckLocation(spec.Config.Workspace, v.State); err != nil {
			return nil, err
		}
	}
	if err := host.ValidateLayout(spec.Config.Workspace, v.State, v.Submission.SessionID); err != nil {
		return nil, err
	}
	if spec.Config.ContextVersion == 0 {
		spec.Config.ContextVersion = host.ContextVersion
	}
	if spec.Config.HistoryImports != "" {
		spec.Config.HistoryImports, err = host.CanonicalImports(spec.Config.HistoryImports)
		if err != nil {
			return nil, host.SpecError{Err: err}
		}
	}
	constraints := v.Submission.Constraints
	if v.Submission.SessionID != "" && !v.Submission.ExplicitConstraints {
		if h != nil {
			constraints, err = h.InheritedConstraints(ctx, v.Submission.SessionID)
		} else {
			constraints, err = readInherited(ctx, v.State, v.Submission.SessionID)
		}
		if err != nil {
			return nil, err
		}
	}
	b := BindingFrom(&v.Binding)
	sub := host.Submission{SessionID: v.Submission.SessionID, Goal: v.Submission.Goal, Constraints: constraints, Spec: spec, Binding: b}
	if err := host.Validate(ctx, v.State, sub); err != nil {
		return nil, err
	}
	if err := validateCredential(ctx, v.State, spec.Config, b, h); err != nil {
		return nil, err
	}
	l := harness.DefaultPromptLimits()
	return &Validated{State: v.State, Spec: SpecOf(spec), Constraints: constraints, GoalLimit: l.Goal, ConstraintsLimit: l.Constraints}, nil
}

func readInherited(ctx context.Context, state, session string) (string, error) {
	path, err := hostrpc.SocketPath(state)
	if err != nil {
		return "", err
	}
	c := &hostrpc.Client{Path: path}
	value, err := c.Inherited(ctx, session)
	if err == nil || !hostrpc.Unavailable(err) {
		return value, err
	}
	return host.ReadInherited(ctx, state, session)
}

func validateCredential(ctx context.Context, state string, c host.RunConfig, b host.Binding, h *host.Host) error {
	provider := cmp.Or(b.Provider, b.Fallback, host.DefaultProvider(c.API))
	if err := host.CheckProvider(provider); err != nil {
		return err
	}
	if b.Keys.For(c.API) == (host.Keys{}) {
		var pairs []Pair
		if h != nil {
			pairs = PairsOf(h.Bindings())
		} else if path, err := hostrpc.SocketPath(state); err == nil {
			hello, err := (&hostrpc.Client{Path: path}).Hello(ctx)
			if err == nil {
				pairs = hello.Bindings
			} else if !hostrpc.Unavailable(err) {
				return err
			}
		}
		for _, p := range pairs {
			if p.API == c.API && p.Provider == provider {
				return host.CheckModel(c)
			}
		}
	}
	_, err := host.NewModel(c, provider, b.Keys)
	return err
}

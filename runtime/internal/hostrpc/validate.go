package hostrpc

import (
	"cmp"
	"context"
	"errors"

	"github.com/ChrAlpha/canti/sdk/go/hostrpc"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
	"github.com/ChrAlpha/canti/runtime/internal/host"
)

// Validate owns the shared admission decision for both the socket and
// preflight process. It writes no facts and performs no model request or
// executor launch.
func Validate(ctx context.Context, v *Validation, h *host.Host) (*Validated, error) {
	state, spec, constraints, b, err := prepareValidation(ctx, v, h)
	if err != nil {
		return nil, err
	}
	sub := host.Submission{InputID: v.Submission.InputID, Goal: v.Submission.Goal, Constraints: constraints, ExplicitConstraints: v.Submission.ExplicitConstraints,
		Delivery: harness.InputDelivery(v.Submission.Delivery), Fresh: v.Submission.Fresh, Spec: spec, Binding: b}
	prepared, opening, err := host.Validate(ctx, state, sub)
	if err != nil {
		return nil, err
	}
	if opening {
		if err := validateCredential(ctx, state, prepared.Config, b, h); err != nil {
			return nil, err
		}
	}
	return validated(state, prepared, constraints)
}

// Resolve canonicalizes a submission's config and state through the serving
// host or preflight transport without making an admission decision.
func Resolve(ctx context.Context, v *Validation, h *host.Host) (*Validated, error) {
	state, spec, constraints, b, err := prepareValidation(ctx, v, h)
	if err != nil {
		return nil, err
	}
	sub := host.Submission{InputID: v.Submission.InputID, Goal: v.Submission.Goal, Constraints: constraints, ExplicitConstraints: v.Submission.ExplicitConstraints,
		Delivery: harness.InputDelivery(v.Submission.Delivery), Fresh: v.Submission.Fresh, Spec: spec, Binding: b}
	prepared, err := host.Resolve(ctx, state, sub)
	if err != nil {
		return nil, err
	}
	return validated(state, prepared, constraints)
}

func prepareValidation(ctx context.Context, v *Validation, h *host.Host) (string, host.Spec, string, host.Binding, error) {
	if v == nil {
		return "", host.Spec{}, "", host.Binding{}, errors.New("validation request is required")
	}
	spec, err := SpecFrom(v.Submission.Spec)
	if err != nil {
		return "", host.Spec{}, "", host.Binding{}, host.SpecError{Err: err}
	}
	if h != nil && spec.Config.Workspace != h.Workspace() {
		return "", host.Spec{}, "", host.Binding{}, h.CheckLocation(spec.Config.Workspace, v.State)
	}
	state, err := host.CanonicalState(spec.Config.Workspace, v.State)
	if err != nil {
		return "", host.Spec{}, "", host.Binding{}, err
	}
	if h != nil {
		if err := h.CheckLocation(spec.Config.Workspace, state); err != nil {
			return "", host.Spec{}, "", host.Binding{}, err
		}
	}
	if err := host.ValidateLayout(spec.Config.Workspace, state); err != nil {
		return "", host.Spec{}, "", host.Binding{}, err
	}
	if spec.Config.ContextVersion == 0 {
		spec.Config.ContextVersion = host.ContextVersion
	}
	if spec.Config.HistoryImports != "" {
		spec.Config.HistoryImports, err = host.CanonicalImports(spec.Config.HistoryImports)
		if err != nil {
			return "", host.Spec{}, "", host.Binding{}, host.SpecError{Err: err}
		}
	}
	constraints := v.Submission.Constraints
	if !v.Submission.ExplicitConstraints && !v.Submission.Fresh {
		if h != nil {
			inherited, err := h.InheritedConstraints(ctx)
			if err != nil {
				return "", host.Spec{}, "", host.Binding{}, err
			}
			constraints = cmp.Or(inherited, constraints)
		} else {
			inherited, err := readInherited(ctx, state)
			if err != nil {
				return "", host.Spec{}, "", host.Binding{}, err
			}
			constraints = cmp.Or(inherited, constraints)
		}
	}
	return state, spec, constraints, BindingFrom(&v.Binding), nil
}

func validated(state string, prepared host.Spec, constraints string) (*Validated, error) {
	projectBinding, err := ProjectOfMeta(prepared.Project.Meta(), prepared.Config.NoProjectContext)
	if err != nil {
		return nil, err
	}
	l := harness.DefaultPromptLimits()
	return &Validated{State: state, Spec: SpecOf(prepared), Constraints: constraints, GoalLimit: l.Goal, ConstraintsLimit: l.Constraints,
		Project: projectBinding}, nil
}

func readInherited(ctx context.Context, state string) (string, error) {
	path, err := hostrpc.SocketPath(state)
	if err != nil {
		return "", err
	}
	c := &hostrpc.Client{Path: path}
	value, err := c.Inherited(ctx)
	if err == nil || !hostrpc.Unavailable(err) {
		return value, err
	}
	return host.ReadInherited(ctx, state)
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

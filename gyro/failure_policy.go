package gyro

import (
	"context"
	"errors"
)

var (
	ErrFailoverNotAllowed  = errors.New("failover is not allowed")
	ErrNoEligibleCandidate = errors.New("no eligible route candidate")
)

// FailurePolicy converts a selector result and health observations into one
// pure routing decision. It never probes, retries, or owns resources.
type FailurePolicy interface {
	Decide(context.Context, RouteRequest, TopologySnapshot, CandidateSet, HealthView) (RouteDecision, error)
}

// PrimaryOnly is the default policy: only the first selector candidate may be
// selected, and it must be observed Healthy.
type PrimaryOnly struct{}

var _ FailurePolicy = PrimaryOnly{}

func (PrimaryOnly) Decide(ctx context.Context, request RouteRequest, snapshot TopologySnapshot, selection CandidateSet, health HealthView) (RouteDecision, error) {
	if err := routeContextErr(ctx); err != nil {
		return RouteDecision{}, err
	}
	members, err := validateSelection(snapshot, selection)
	if err != nil {
		return RouteDecision{}, err
	}
	if len(selection.Candidates) == 0 {
		return RouteDecision{}, ErrNoEligibleCandidate
	}
	if health == nil || health.Status(selection.Candidates[0].MemberID) != Healthy {
		return RouteDecision{}, ErrFailoverNotAllowed
	}
	return decisionForCandidate(snapshot, selection, members, 0, "primary-only", "primary candidate is healthy"), nil
}

// HealthyCandidate selects the first candidate accepted by the health view.
// Unknown observations are eligible only when AllowUnknown is explicitly set.
type HealthyCandidate struct {
	AllowUnknown bool
}

var _ FailurePolicy = HealthyCandidate{}

func (policy HealthyCandidate) Decide(ctx context.Context, request RouteRequest, snapshot TopologySnapshot, selection CandidateSet, health HealthView) (RouteDecision, error) {
	if err := routeContextErr(ctx); err != nil {
		return RouteDecision{}, err
	}
	members, err := validateSelection(snapshot, selection)
	if err != nil {
		return RouteDecision{}, err
	}
	if health == nil {
		return RouteDecision{}, ErrNoEligibleCandidate
	}
	for index, candidate := range selection.Candidates {
		status := health.Status(candidate.MemberID)
		if status == Healthy || (status == Unknown && policy.AllowUnknown) {
			reason := "healthy candidate selected"
			if status == Unknown {
				reason = "candidate selected with unknown health"
			}
			return decisionForCandidate(snapshot, selection, members, index, "healthy-candidate", reason), nil
		}
	}
	return RouteDecision{}, ErrNoEligibleCandidate
}

func routeContextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

func validateSelection(snapshot TopologySnapshot, selection CandidateSet) (map[string]Member, error) {
	if selection.Revision != snapshot.Revision {
		return nil, ErrSelectionMismatch
	}
	members := make(map[string]Member, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if _, exists := members[member.ID]; exists {
			return nil, ErrSelectionMismatch
		}
		members[member.ID] = member
	}
	seen := make(map[string]struct{}, len(selection.Candidates))
	for _, candidate := range selection.Candidates {
		if _, exists := members[candidate.MemberID]; !exists {
			return nil, ErrSelectionMismatch
		}
		if _, exists := seen[candidate.MemberID]; exists {
			return nil, ErrSelectionMismatch
		}
		seen[candidate.MemberID] = struct{}{}
	}
	return members, nil
}

func decisionForCandidate(snapshot TopologySnapshot, selection CandidateSet, members map[string]Member, primaryIndex int, policy, reason string) RouteDecision {
	primaryID := selection.Candidates[primaryIndex].MemberID
	candidates := make([]Member, 0, len(selection.Candidates)-1)
	for index, candidate := range selection.Candidates {
		if index != primaryIndex {
			candidates = append(candidates, cloneMember(members[candidate.MemberID]))
		}
	}
	return RouteDecision{
		Primary:    cloneMember(members[primaryID]),
		Candidates: candidates,
		Revision:   snapshot.Revision,
		Policy:     policy,
		Reason:     reason,
	}
}

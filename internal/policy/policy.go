package policy

import (
	"context"

	"gyro/gyro"
)

// FailurePolicy converts a selector result and health observations into one
// pure routing decision. It never probes, retries, or owns resources.
var _ gyro.FailurePolicy = PrimaryOnly{}

// PrimaryOnly is the default policy: only the first selector candidate may be
// selected, and it must be observed Healthy.
type PrimaryOnly struct{}

func (PrimaryOnly) Decide(ctx context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot, selection gyro.CandidateSet, health gyro.HealthView) (gyro.RouteDecision, error) {
	if err := routeContextErr(ctx); err != nil {
		return gyro.RouteDecision{}, err
	}
	members, err := validateSelection(snapshot, selection)
	if err != nil {
		return gyro.RouteDecision{}, err
	}
	if len(selection.Candidates) == 0 {
		return gyro.RouteDecision{}, gyro.ErrNoEligibleCandidate
	}
	if health == nil || health.Status(selection.Candidates[0].MemberID) != gyro.Healthy {
		return gyro.RouteDecision{}, gyro.ErrFailoverNotAllowed
	}
	return decisionForCandidate(snapshot, selection, members, 0, "primary-only", "primary candidate is healthy"), nil
}

// HealthyCandidate selects the first candidate accepted by the health view.
// Unknown observations are eligible only when AllowUnknown is explicitly set.
type HealthyCandidate struct {
	AllowUnknown bool
}

var _ gyro.FailurePolicy = HealthyCandidate{}

func (policy HealthyCandidate) Decide(ctx context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot, selection gyro.CandidateSet, health gyro.HealthView) (gyro.RouteDecision, error) {
	if err := routeContextErr(ctx); err != nil {
		return gyro.RouteDecision{}, err
	}
	members, err := validateSelection(snapshot, selection)
	if err != nil {
		return gyro.RouteDecision{}, err
	}
	if health == nil {
		return gyro.RouteDecision{}, gyro.ErrNoEligibleCandidate
	}
	for index, candidate := range selection.Candidates {
		status := health.Status(candidate.MemberID)
		if status == gyro.Healthy || (status == gyro.Unknown && policy.AllowUnknown) {
			reason := "healthy candidate selected"
			if status == gyro.Unknown {
				reason = "candidate selected with unknown health"
			}
			return decisionForCandidate(snapshot, selection, members, index, "healthy-candidate", reason), nil
		}
	}
	return gyro.RouteDecision{}, gyro.ErrNoEligibleCandidate
}

func routeContextErr(ctx context.Context) error {
	if ctx == nil {
		return gyro.ErrNilContext
	}
	return ctx.Err()
}

func validateSelection(snapshot gyro.TopologySnapshot, selection gyro.CandidateSet) (map[string]gyro.Member, error) {
	if selection.Revision != snapshot.Revision {
		return nil, gyro.ErrSelectionMismatch
	}
	members := make(map[string]gyro.Member, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if _, exists := members[member.ID]; exists {
			return nil, gyro.ErrSelectionMismatch
		}
		members[member.ID] = member
	}
	seen := make(map[string]struct{}, len(selection.Candidates))
	for _, candidate := range selection.Candidates {
		if _, exists := members[candidate.MemberID]; !exists {
			return nil, gyro.ErrSelectionMismatch
		}
		if _, exists := seen[candidate.MemberID]; exists {
			return nil, gyro.ErrSelectionMismatch
		}
		seen[candidate.MemberID] = struct{}{}
	}
	return members, nil
}

func decisionForCandidate(snapshot gyro.TopologySnapshot, selection gyro.CandidateSet, members map[string]gyro.Member, primaryIndex int, policy, reason string) gyro.RouteDecision {
	primaryID := selection.Candidates[primaryIndex].MemberID
	candidates := make([]gyro.Member, 0, len(selection.Candidates)-1)
	for index, candidate := range selection.Candidates {
		if index != primaryIndex {
			candidates = append(candidates, cloneMember(members[candidate.MemberID]))
		}
	}
	return gyro.RouteDecision{
		Primary:    cloneMember(members[primaryID]),
		Candidates: candidates,
		Revision:   snapshot.Revision,
		Policy:     policy,
		Reason:     reason,
	}
}

func cloneMember(member gyro.Member) gyro.Member {
	result := member
	result.Endpoints = append([]gyro.Endpoint(nil), member.Endpoints...)
	if member.Attributes != nil {
		result.Attributes = make(map[string]string, len(member.Attributes))
		for key, value := range member.Attributes {
			result.Attributes[key] = value
		}
	}
	return result
}

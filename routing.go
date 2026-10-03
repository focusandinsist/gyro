package gyro

import (
	"context"
	"errors"
)

var (
	// ErrInvalidRequest indicates that a route request is missing data required
	// by the selected routing policy.
	ErrInvalidRequest = errors.New("invalid route request")
	// ErrNoMembers indicates that a route was requested for an empty topology.
	ErrNoMembers = errors.New("no topology members")
	// ErrSelectionMismatch indicates that candidates do not belong to the
	// topology snapshot supplied for the same routing operation.
	ErrSelectionMismatch = errors.New("candidate selection does not match topology snapshot")
)

// RouteRequest is the immutable, protocol-neutral input to selection.
// Attributes are request semantics, not topology or resource configuration.
type RouteRequest struct {
	Key        string
	Attributes map[string]string
}

// Candidate identifies a member in the snapshot used by a Selector. It does
// not imply replica ownership, health, or an established resource.
type Candidate struct {
	MemberID string
}

// CandidateSet is an ordered selection bound to the revision that produced
// it. The first candidate is the selector's preferred member.
type CandidateSet struct {
	Revision   Revision
	Candidates []Candidate
}

// RouteDecision is a pure routing result. Candidates contains the ordered
// alternatives after Primary and never contains protocol resources.
type RouteDecision struct {
	Primary    Member
	Candidates []Member
	Revision   Revision
	Policy     string
	Reason     string
}

// Selector is a pure, protocol-neutral candidate ordering policy.
type Selector interface {
	Select(ctx context.Context, request RouteRequest, snapshot TopologySnapshot) (CandidateSet, error)
}

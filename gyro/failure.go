package gyro

import (
	"context"
	"errors"
)

var (
	ErrFailoverNotAllowed  = errors.New("failover is not allowed")
	ErrNoEligibleCandidate = errors.New("no eligible route candidate")
)

// FailurePolicy converts a selector result and health observations into a
// protocol-neutral routing decision. It never probes, retries, or owns
// resources.
type FailurePolicy interface {
	Decide(context.Context, RouteRequest, TopologySnapshot, CandidateSet, HealthView) (RouteDecision, error)
}

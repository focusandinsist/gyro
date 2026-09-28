package gyro

import (
	"context"
	"fmt"
	"sort"
)

// ConsistentHashSelector orders topology members with the same consistent-go
// partitioning rules used by the legacy locator. It owns no nodes or resources
// and is safe to call concurrently because each selection builds an immutable
// ring for its input snapshot.
type ConsistentHashSelector struct {
	config LocatorConfig
}

var _ Selector = (*ConsistentHashSelector)(nil)

// NewConsistentHashSelector creates a pure selector using the configured hash
// function, partition count, replication factor, and load.
func NewConsistentHashSelector(config LocatorConfig) (*ConsistentHashSelector, error) {
	if _, err := newHashRing(config); err != nil {
		return nil, err
	}
	return &ConsistentHashSelector{config: config}, nil
}

// Select returns every member in deterministic ring order, with the first
// member as the preferred candidate. The returned candidates are IDs only;
// resources and health are handled by later layers.
func (s *ConsistentHashSelector) Select(ctx context.Context, request RouteRequest, snapshot TopologySnapshot) (CandidateSet, error) {
	if ctx == nil {
		return CandidateSet{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return CandidateSet{}, err
	}
	if request.Key == "" {
		return CandidateSet{}, ErrInvalidRequest
	}

	normalized, err := normalizeTopologySnapshot(snapshot)
	if err != nil {
		return CandidateSet{}, err
	}
	if len(normalized.Members) == 0 {
		return CandidateSet{}, ErrNoMembers
	}

	ring, err := newHashRing(s.config)
	if err != nil {
		return CandidateSet{}, err
	}
	memberIDs := make([]string, len(normalized.Members))
	for i, member := range normalized.Members {
		memberIDs[i] = member.ID
	}
	sort.Strings(memberIDs)
	for _, memberID := range memberIDs {
		if err := ring.Add(ctx, memberID); err != nil {
			return CandidateSet{}, fmt.Errorf("failed to add member %s to selector ring: %w", memberID, err)
		}
	}

	ids, err := ring.LocateReplicas(ctx, []byte(request.Key), len(memberIDs))
	if err != nil {
		return CandidateSet{}, fmt.Errorf("failed to select candidates: %w", err)
	}
	if len(ids) == 0 {
		return CandidateSet{}, ErrNoMembers
	}
	candidates := make([]Candidate, len(ids))
	for i, id := range ids {
		candidates[i] = Candidate{MemberID: id}
	}
	return CandidateSet{Revision: normalized.Revision, Candidates: candidates}, nil
}

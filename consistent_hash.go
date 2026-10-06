package gyro

import (
	"context"
	"fmt"
	"sort"

	"github.com/focusandinsist/consistent-go/consistent"
)

// ConsistentHashSelector orders members by the default consistent-hash rules.
// It owns no resources and can be called concurrently.
type ConsistentHashSelector struct {
	config LocatorConfig
}

var _ Selector = (*ConsistentHashSelector)(nil)

func NewConsistentHashSelector(config LocatorConfig) (*ConsistentHashSelector, error) {
	if _, err := newHashRing(config); err != nil {
		return nil, err
	}
	return &ConsistentHashSelector{config: config}, nil
}

func (s *ConsistentHashSelector) Select(ctx context.Context, request RouteRequest, snapshot TopologySnapshot) (CandidateSet, error) {
	if err := routeContextErr(ctx); err != nil {
		return CandidateSet{}, err
	}
	if request.Key == "" {
		return CandidateSet{}, ErrInvalidRequest
	}
	normalized, err := normalizeSelectorSnapshot(snapshot)
	if err != nil {
		return CandidateSet{}, err
	}
	if len(normalized.Members) == 0 {
		return CandidateSet{}, ErrNoMembers
	}
	ids := make([]string, len(normalized.Members))
	for i, member := range normalized.Members {
		ids[i] = member.ID
	}
	ring, err := buildHashRing(ctx, s.config, ids)
	if err != nil {
		return CandidateSet{}, err
	}
	selected, err := ring.LocateReplicas(ctx, []byte(request.Key), len(ids))
	if err != nil {
		return CandidateSet{}, fmt.Errorf("failed to select candidates: %w", err)
	}
	if len(selected) == 0 {
		return CandidateSet{}, ErrNoMembers
	}
	candidates := make([]Candidate, len(selected))
	for i, id := range selected {
		candidates[i] = Candidate{MemberID: id}
	}
	return CandidateSet{Revision: normalized.Revision, Candidates: candidates}, nil
}

func routeContextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

func newHashRing(config LocatorConfig) (*consistent.Consistent, error) {
	var hasher consistent.Hasher
	switch config.HashFunction {
	case "", "xxhash":
		hasher = consistent.NewXXHasher()
	case "murmur3":
		hasher = consistent.NewMurmurHash3Hasher()
	default:
		return nil, fmt.Errorf("unknown hash function: %s", config.HashFunction)
	}
	return consistent.New(consistent.Config{
		Hasher: hasher, PartitionCount: config.PartitionCount,
		ReplicationFactor: config.ReplicationFactor, Load: config.Load,
	})
}

func buildHashRing(ctx context.Context, config LocatorConfig, ids []string) (*consistent.Consistent, error) {
	ring, err := newHashRing(config)
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ring.Add(ctx, id); err != nil {
			return nil, fmt.Errorf("failed to add member %s to selector ring: %w", id, err)
		}
	}
	return ring, nil
}

func normalizeSelectorSnapshot(snapshot TopologySnapshot) (TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return TopologySnapshot{}, ErrInvalidSnapshot
	}
	result := snapshot
	result.Members = append([]Member(nil), snapshot.Members...)
	seen := make(map[string]struct{}, len(result.Members))
	for _, member := range result.Members {
		if member.ID == "" {
			return TopologySnapshot{}, ErrInvalidSnapshot
		}
		if _, ok := seen[member.ID]; ok {
			return TopologySnapshot{}, ErrInvalidSnapshot
		}
		seen[member.ID] = struct{}{}
	}
	sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].ID < result.Members[j].ID })
	return result, nil
}

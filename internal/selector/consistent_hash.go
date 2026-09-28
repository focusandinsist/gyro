package selector

import (
	"context"
	"fmt"
	"sort"

	"github.com/focusandinsist/consistent-go/consistent"
	"github.com/focusandinsist/gyro/gyro"
)

// ConsistentHashSelector orders topology members with the same consistent-go
// partitioning rules used by the legacy locator. It owns no nodes or resources
// and is safe to call concurrently because each selection builds an immutable
// ring for its input snapshot.
type ConsistentHashSelector struct {
	config gyro.LocatorConfig
}

var _ gyro.Selector = (*ConsistentHashSelector)(nil)

// NewConsistentHashSelector creates a pure selector using the configured hash
// function, partition count, replication factor, and load.
func NewConsistentHashSelector(config gyro.LocatorConfig) (*ConsistentHashSelector, error) {
	if _, err := newHashRing(config); err != nil {
		return nil, err
	}
	return &ConsistentHashSelector{config: config}, nil
}

// Select returns every member in deterministic ring order, with the first
// member as the preferred candidate. The returned candidates are IDs only;
// resources and health are handled by later layers.
func (s *ConsistentHashSelector) Select(ctx context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot) (gyro.CandidateSet, error) {
	if ctx == nil {
		return gyro.CandidateSet{}, gyro.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return gyro.CandidateSet{}, err
	}
	if request.Key == "" {
		return gyro.CandidateSet{}, gyro.ErrInvalidRequest
	}

	normalized, err := normalizeSnapshot(snapshot)
	if err != nil {
		return gyro.CandidateSet{}, err
	}
	if len(normalized.Members) == 0 {
		return gyro.CandidateSet{}, gyro.ErrNoMembers
	}

	ring, err := newHashRing(s.config)
	if err != nil {
		return gyro.CandidateSet{}, err
	}
	memberIDs := make([]string, len(normalized.Members))
	for i, member := range normalized.Members {
		memberIDs[i] = member.ID
	}
	sort.Strings(memberIDs)
	for _, memberID := range memberIDs {
		if err := ring.Add(ctx, memberID); err != nil {
			return gyro.CandidateSet{}, fmt.Errorf("failed to add member %s to selector ring: %w", memberID, err)
		}
	}

	ids, err := ring.LocateReplicas(ctx, []byte(request.Key), len(memberIDs))
	if err != nil {
		return gyro.CandidateSet{}, fmt.Errorf("failed to select candidates: %w", err)
	}
	if len(ids) == 0 {
		return gyro.CandidateSet{}, gyro.ErrNoMembers
	}
	candidates := make([]gyro.Candidate, len(ids))
	for i, id := range ids {
		candidates[i] = gyro.Candidate{MemberID: id}
	}
	return gyro.CandidateSet{Revision: normalized.Revision, Candidates: candidates}, nil
}

type hashRing interface {
	LocateReplicas(context.Context, []byte, int) ([]string, error)
	Add(context.Context, string) error
}

func newHashRing(config gyro.LocatorConfig) (hashRing, error) {
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

func normalizeSnapshot(snapshot gyro.TopologySnapshot) (gyro.TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	result := snapshot
	result.Members = append([]gyro.Member(nil), snapshot.Members...)
	seen := make(map[string]struct{}, len(result.Members))
	for i := range result.Members {
		member := &result.Members[i]
		if member.ID == "" {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		if _, ok := seen[member.ID]; ok {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		seen[member.ID] = struct{}{}
	}
	sort.Slice(result.Members, func(i, j int) bool { return result.Members[i].ID < result.Members[j].ID })
	return result, nil
}

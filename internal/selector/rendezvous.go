package selector

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
	"gyro"
)

// RendezvousSelector ranks every member independently by a deterministic
// rendezvous score. It has no resource or health dependencies.
type RendezvousSelector struct {
	seed string
}

var _ gyro.Selector = (*RendezvousSelector)(nil)

// NewRendezvousSelector creates a selector. Seed is part of the routing
// configuration and must be kept equal by clients that need identical results.
func NewRendezvousSelector(seed string) *RendezvousSelector {
	return &RendezvousSelector{seed: seed}
}

func (s *RendezvousSelector) Select(ctx context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot) (gyro.CandidateSet, error) {
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

	type scoredMember struct {
		member gyro.Member
		score  uint64
	}
	scored := make([]scoredMember, 0, len(normalized.Members))
	for _, member := range normalized.Members {
		if err := ctx.Err(); err != nil {
			return gyro.CandidateSet{}, err
		}
		scored = append(scored, scoredMember{
			member: member,
			score:  rendezvousScore(s.seed, request.Key, member.ID),
		})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].member.ID < scored[j].member.ID
	})
	candidates := make([]gyro.Candidate, len(scored))
	for i, item := range scored {
		candidates[i] = gyro.Candidate{MemberID: item.member.ID}
	}
	return gyro.CandidateSet{Revision: normalized.Revision, Candidates: candidates}, nil
}

func rendezvousScore(seed, key, memberID string) uint64 {
	hasher := xxhash.New()
	_, _ = hasher.WriteString(seed)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.WriteString(key)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.WriteString(memberID)
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], hasher.Sum64())
	return binary.LittleEndian.Uint64(encoded[:])
}

func normalizeSnapshot(snapshot gyro.TopologySnapshot) (gyro.TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	result := snapshot
	result.Members = append([]gyro.Member(nil), snapshot.Members...)
	seen := make(map[string]struct{}, len(result.Members))
	for _, member := range result.Members {
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

package gyro

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// RendezvousSelector ranks every member independently by a deterministic
// rendezvous score. It has no resource or health dependencies.
type RendezvousSelector struct {
	seed string
}

var _ Selector = (*RendezvousSelector)(nil)

// NewRendezvousSelector creates a selector. Seed is part of the routing
// configuration and must be kept equal by clients that need identical results.
func NewRendezvousSelector(seed string) *RendezvousSelector {
	return &RendezvousSelector{seed: seed}
}

func (s *RendezvousSelector) Select(ctx context.Context, request RouteRequest, snapshot TopologySnapshot) (CandidateSet, error) {
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

	type scoredMember struct {
		member Member
		score  uint64
	}
	scored := make([]scoredMember, 0, len(normalized.Members))
	for _, member := range normalized.Members {
		if err := ctx.Err(); err != nil {
			return CandidateSet{}, err
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
	candidates := make([]Candidate, len(scored))
	for i, item := range scored {
		candidates[i] = Candidate{MemberID: item.member.ID}
	}
	return CandidateSet{Revision: normalized.Revision, Candidates: candidates}, nil
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

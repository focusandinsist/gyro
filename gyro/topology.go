package gyro

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

var (
	ErrNilContext = errors.New("context cannot be nil")
	// ErrInvalidSnapshot indicates that a complete topology snapshot failed
	// structural validation and was not accepted by a store.
	ErrInvalidSnapshot = errors.New("invalid topology snapshot")
	// ErrStaleRevision indicates that a snapshot is older than the active
	// revision from the same source.
	ErrStaleRevision = errors.New("stale topology revision")
	// ErrRevisionConflict indicates that one source used different tokens for
	// the same generation.
	ErrRevisionConflict = errors.New("topology revision conflict")
	// ErrIncomparableRevision indicates that a publish attempted to cross the
	// active source boundary without an explicit reset.
	ErrIncomparableRevision = errors.New("topology revisions are incomparable")
)

// Endpoint is a protocol adapter connection target for a Member.
type Endpoint struct {
	Address    string
	Attributes map[string]string
}

// Member is a stable logical topology identity and its connection targets.
type Member struct {
	ID         string
	Endpoints  []Endpoint
	Attributes map[string]string
}

// Revision identifies a complete snapshot from one topology source.
// Token is opaque: stores only compare it for equality and never order it.
type Revision struct {
	Source     string
	Generation uint64
	Token      string
}

// TopologySnapshot is a complete, immutable snapshot once accepted by a
// TopologyStore. Members are canonically sorted by ID.
type TopologySnapshot struct {
	Revision Revision
	Members  []Member
}

// TopologyStore owns one accepted immutable snapshot in a process.
type TopologyStore interface {
	Snapshot() (TopologySnapshot, bool)
	Publish(ctx context.Context, snapshot TopologySnapshot) error
	ResetSource(ctx context.Context, snapshot TopologySnapshot) error
}

// InMemoryTopologyStore is a thread-safe TopologyStore for complete snapshots.
type InMemoryTopologyStore struct {
	mu       sync.RWMutex
	snapshot TopologySnapshot
	valid    bool
}

var _ TopologyStore = (*InMemoryTopologyStore)(nil)

// NewTopologyStore creates an empty in-memory topology store.
func NewTopologyStore() *InMemoryTopologyStore {
	return &InMemoryTopologyStore{}
}

// Snapshot returns a deep copy of the current snapshot. The boolean is false
// until the first valid snapshot has been published.
func (s *InMemoryTopologyStore) Snapshot() (TopologySnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.valid {
		return TopologySnapshot{}, false
	}
	return cloneTopologySnapshot(s.snapshot), true
}

// Publish accepts a newer snapshot from the active source. The first valid
// publish establishes the active source; source changes require ResetSource.
func (s *InMemoryTopologyStore) Publish(ctx context.Context, snapshot TopologySnapshot) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	normalized, err := normalizeTopologySnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := contextErr(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextErr(ctx); err != nil {
		return err
	}
	if !s.valid {
		s.snapshot = normalized
		s.valid = true
		return nil
	}

	current := s.snapshot.Revision
	incoming := normalized.Revision
	if incoming.Source != current.Source {
		return ErrIncomparableRevision
	}
	switch {
	case incoming.Generation < current.Generation:
		return ErrStaleRevision
	case incoming.Generation == current.Generation:
		if incoming.Token == current.Token {
			return nil
		}
		return ErrRevisionConflict
	default:
		s.snapshot = normalized
		return nil
	}
}

// ResetSource atomically replaces the active source with a complete snapshot.
// It is the only operation that can invalidate events from the previous
// source, and it also works when the store is empty.
func (s *InMemoryTopologyStore) ResetSource(ctx context.Context, snapshot TopologySnapshot) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	normalized, err := normalizeTopologySnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := contextErr(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextErr(ctx); err != nil {
		return err
	}
	if s.valid && normalized.Revision.Source == s.snapshot.Revision.Source {
		current := s.snapshot.Revision
		incoming := normalized.Revision
		switch {
		case incoming.Generation < current.Generation:
			return ErrStaleRevision
		case incoming.Generation == current.Generation:
			if incoming.Token == current.Token {
				return nil
			}
			return ErrRevisionConflict
		}
	}
	s.snapshot = normalized
	s.valid = true
	return nil
}

func normalizeTopologySnapshot(snapshot TopologySnapshot) (TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return TopologySnapshot{}, ErrInvalidSnapshot
	}

	result := TopologySnapshot{
		Revision: snapshot.Revision,
		Members:  make([]Member, len(snapshot.Members)),
	}
	seenMembers := make(map[string]struct{}, len(snapshot.Members))
	for i, member := range snapshot.Members {
		if member.ID == "" {
			return TopologySnapshot{}, ErrInvalidSnapshot
		}
		if _, exists := seenMembers[member.ID]; exists {
			return TopologySnapshot{}, ErrInvalidSnapshot
		}
		seenMembers[member.ID] = struct{}{}

		result.Members[i] = Member{
			ID:         member.ID,
			Endpoints:  make([]Endpoint, len(member.Endpoints)),
			Attributes: cloneStringMap(member.Attributes),
		}
		seenEndpoints := make([]Endpoint, 0, len(member.Endpoints))
		for j, endpoint := range member.Endpoints {
			if endpoint.Address == "" {
				return TopologySnapshot{}, ErrInvalidSnapshot
			}
			for _, seen := range seenEndpoints {
				if seen.Address == endpoint.Address && equalStringMaps(seen.Attributes, endpoint.Attributes) {
					return TopologySnapshot{}, ErrInvalidSnapshot
				}
			}
			copyEndpoint := Endpoint{
				Address:    endpoint.Address,
				Attributes: cloneStringMap(endpoint.Attributes),
			}
			seenEndpoints = append(seenEndpoints, copyEndpoint)
			result.Members[i].Endpoints[j] = copyEndpoint
		}
		sort.Slice(result.Members[i].Endpoints, func(left, right int) bool {
			leftEndpoint := result.Members[i].Endpoints[left]
			rightEndpoint := result.Members[i].Endpoints[right]
			if leftEndpoint.Address != rightEndpoint.Address {
				return leftEndpoint.Address < rightEndpoint.Address
			}
			return endpointAttributesKey(leftEndpoint) < endpointAttributesKey(rightEndpoint)
		})
	}
	sort.Slice(result.Members, func(left, right int) bool {
		return result.Members[left].ID < result.Members[right].ID
	})
	return result, nil
}

func cloneTopologySnapshot(snapshot TopologySnapshot) TopologySnapshot {
	result := TopologySnapshot{
		Revision: snapshot.Revision,
		Members:  make([]Member, len(snapshot.Members)),
	}
	for i, member := range snapshot.Members {
		result.Members[i] = Member{
			ID:         member.ID,
			Endpoints:  make([]Endpoint, len(member.Endpoints)),
			Attributes: cloneStringMap(member.Attributes),
		}
		for j, endpoint := range member.Endpoints {
			result.Members[i].Endpoints[j] = Endpoint{
				Address:    endpoint.Address,
				Attributes: cloneStringMap(endpoint.Attributes),
			}
		}
	}
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func equalStringMaps(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, exists := right[key]
		if !exists || rightValue != leftValue {
			return false
		}
	}
	return true
}

func endpointAttributesKey(endpoint Endpoint) string {
	encoded, _ := json.Marshal(endpoint.Attributes)
	return string(encoded)
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

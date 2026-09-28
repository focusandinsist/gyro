package topology

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/focusandinsist/gyro/gyro"
)

// Store is the internal implementation of gyro.TopologyStore. It owns one
// accepted immutable snapshot and rejects stale or incomparable revisions.
type Store struct {
	mu       sync.RWMutex
	snapshot gyro.TopologySnapshot
	valid    bool
}

var _ gyro.TopologyStore = (*Store)(nil)

func NewStore() *Store { return &Store{} }

func (s *Store) Snapshot() (gyro.TopologySnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.valid {
		return gyro.TopologySnapshot{}, false
	}
	return cloneSnapshot(s.snapshot), true
}

func (s *Store) Publish(ctx context.Context, snapshot gyro.TopologySnapshot) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	normalized, err := normalize(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := contextErr(ctx); err != nil {
		return err
	}
	if !s.valid {
		s.snapshot, s.valid = normalized, true
		return nil
	}
	current := s.snapshot.Revision
	incoming := normalized.Revision
	if incoming.Source != current.Source {
		return gyro.ErrIncomparableRevision
	}
	switch {
	case incoming.Generation < current.Generation:
		return gyro.ErrStaleRevision
	case incoming.Generation == current.Generation && incoming.Token != current.Token:
		return gyro.ErrRevisionConflict
	case incoming.Generation == current.Generation:
		return nil
	default:
		s.snapshot = normalized
		return nil
	}
}

func (s *Store) ResetSource(ctx context.Context, snapshot gyro.TopologySnapshot) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	normalized, err := normalize(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.valid && normalized.Revision.Source == s.snapshot.Revision.Source {
		current := s.snapshot.Revision
		incoming := normalized.Revision
		if incoming.Generation < current.Generation {
			return gyro.ErrStaleRevision
		}
		if incoming.Generation == current.Generation && incoming.Token != current.Token {
			return gyro.ErrRevisionConflict
		}
		if incoming.Generation == current.Generation {
			return nil
		}
	}
	s.snapshot, s.valid = normalized, true
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return gyro.ErrNilContext
	}
	return ctx.Err()
}

func normalize(snapshot gyro.TopologySnapshot) (gyro.TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	result := cloneSnapshot(snapshot)
	seenMembers := make(map[string]struct{}, len(result.Members))
	for i := range result.Members {
		member := &result.Members[i]
		if member.ID == "" {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		if _, exists := seenMembers[member.ID]; exists {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		seenMembers[member.ID] = struct{}{}
		seenEndpoints := make(map[string]struct{}, len(member.Endpoints))
		for _, endpoint := range member.Endpoints {
			if endpoint.Address == "" {
				return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
			}
			key := endpointKey(endpoint)
			if _, exists := seenEndpoints[key]; exists {
				return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
			}
			seenEndpoints[key] = struct{}{}
		}
		sort.Slice(member.Endpoints, func(left, right int) bool {
			return endpointKey(member.Endpoints[left]) < endpointKey(member.Endpoints[right])
		})
	}
	sort.Slice(result.Members, func(left, right int) bool { return result.Members[left].ID < result.Members[right].ID })
	return result, nil
}

func cloneSnapshot(snapshot gyro.TopologySnapshot) gyro.TopologySnapshot {
	result := gyro.TopologySnapshot{Revision: snapshot.Revision, Members: make([]gyro.Member, len(snapshot.Members))}
	for i, member := range snapshot.Members {
		result.Members[i] = gyro.Member{ID: member.ID, Endpoints: make([]gyro.Endpoint, len(member.Endpoints)), Attributes: cloneMap(member.Attributes)}
		for j, endpoint := range member.Endpoints {
			result.Members[i].Endpoints[j] = gyro.Endpoint{Address: endpoint.Address, Attributes: cloneMap(endpoint.Attributes)}
		}
	}
	return result
}

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func endpointKey(endpoint gyro.Endpoint) string {
	encoded, _ := json.Marshal(endpoint.Attributes)
	return endpoint.Address + "\x00" + string(encoded)
}

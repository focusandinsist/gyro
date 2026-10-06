package gyro

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/focusandinsist/consistent-go/consistent"
)

// Router selects members for keys without health checks or protocol resources.
// ReplaceMembers publishes a complete new membership snapshot.
type Router struct {
	config    LocatorConfig
	replaceMu sync.Mutex
	state     atomic.Pointer[routerState]
}

type routerState struct {
	members map[string]Member
	ring    *consistent.Consistent
}

// NewRouter constructs a router for a non-empty set of stable member IDs.
func NewRouter(members []Member, config LocatorConfig) (*Router, error) {
	if len(members) == 0 {
		return nil, ErrNoMembers
	}
	state, err := buildRouterState(context.Background(), members, config)
	if err != nil {
		return nil, err
	}
	router := &Router{config: config}
	router.state.Store(state)
	return router, nil
}

// Route returns the consistent-hash primary for key.
func (r *Router) Route(ctx context.Context, key string) (Member, error) {
	candidates, err := r.Candidates(ctx, key, 1)
	if err != nil {
		return Member{}, err
	}
	return candidates[0], nil
}

// Candidates returns up to count members in selector order, primary first.
func (r *Router) Candidates(ctx context.Context, key string, count int) ([]Member, error) {
	if err := routeContextErr(ctx); err != nil {
		return nil, err
	}
	if key == "" || count <= 0 {
		return nil, ErrInvalidRequest
	}
	if r == nil {
		return nil, ErrNoMembers
	}
	state := r.state.Load()
	if state == nil || len(state.members) == 0 {
		return nil, ErrNoMembers
	}
	if count > len(state.members) {
		count = len(state.members)
	}
	ids, err := state.ring.LocateReplicas(ctx, []byte(key), count)
	if err != nil {
		return nil, err
	}
	members := make([]Member, len(ids))
	for i, id := range ids {
		members[i] = cloneRouterMember(state.members[id])
	}
	return members, nil
}

// ReplaceMembers atomically replaces the complete member set. An empty set is
// valid and causes subsequent routes to return ErrNoMembers.
func (r *Router) ReplaceMembers(ctx context.Context, members []Member) error {
	if err := routeContextErr(ctx); err != nil {
		return err
	}
	r.replaceMu.Lock()
	defer r.replaceMu.Unlock()
	state, err := buildRouterState(ctx, members, r.config)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.state.Store(state)
	return nil
}

func buildRouterState(ctx context.Context, members []Member, config LocatorConfig) (*routerState, error) {
	state := &routerState{members: make(map[string]Member, len(members))}
	ids := make([]string, 0, len(members))
	for _, member := range members {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if member.ID == "" {
			return nil, ErrInvalidSnapshot
		}
		if _, exists := state.members[member.ID]; exists {
			return nil, ErrInvalidSnapshot
		}
		state.members[member.ID] = cloneRouterMember(member)
		ids = append(ids, member.ID)
	}
	if len(ids) == 0 {
		return state, nil
	}
	ring, err := buildHashRing(ctx, config, ids)
	if err != nil {
		return nil, err
	}
	state.ring = ring
	return state, nil
}

func cloneRouterMember(member Member) Member {
	result := member
	result.Attributes = cloneRouterAttributes(member.Attributes)
	if member.Endpoints != nil {
		result.Endpoints = make([]Endpoint, len(member.Endpoints))
		for i, endpoint := range member.Endpoints {
			result.Endpoints[i] = endpoint
			result.Endpoints[i].Attributes = cloneRouterAttributes(endpoint.Attributes)
		}
	}
	return result
}

func cloneRouterAttributes(attributes map[string]string) map[string]string {
	if attributes == nil {
		return nil
	}
	result := make(map[string]string, len(attributes))
	for key, value := range attributes {
		result[key] = value
	}
	return result
}

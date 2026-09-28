package gyro

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrResourceUnavailable = errors.New("route resource unavailable")
	ErrResourcePoolClosed  = errors.New("resource pool is closed")
)

// Resource is an adapter-owned resource managed by ResourcePool.
type Resource interface {
	MemberID() string
	Close() error
}

// ResourceFactory creates a resource for one topology member.
type ResourceFactory interface {
	Create(context.Context, Member) (Resource, error)
}

// ResourceHandle keeps a resource alive until Release is called.
type ResourceHandle interface {
	Resource() Resource
	Release() error
}

type resourceEntry struct {
	resource    Resource
	fingerprint string
	refs        int
	pending     bool
	closed      bool
}

type resourceHandle struct {
	pool  *ResourcePool
	entry *resourceEntry
	once  sync.Once
	err   error
}

// ResourcePool owns resources and delays replacement closure until all leases
// against the old entry have been released.
type ResourcePool struct {
	mu      sync.Mutex
	factory ResourceFactory
	active  map[string]*resourceEntry
	closed  bool
}

func NewResourcePool(factory ResourceFactory) (*ResourcePool, error) {
	if factory == nil {
		return nil, fmt.Errorf("resource factory cannot be nil")
	}
	return &ResourcePool{factory: factory, active: make(map[string]*resourceEntry)}, nil
}

// Replace prepares all changed resources before publishing the new member
// set. Existing entries with the same member fingerprint are reused.
func (p *ResourcePool) Replace(ctx context.Context, members []Member) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeTopologySnapshot(TopologySnapshot{Revision: Revision{Source: "resource", Generation: 1, Token: "replace"}, Members: members})
	if err != nil {
		return err
	}
	prepared := make(map[string]*resourceEntry, len(normalized.Members))
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrResourcePoolClosed
	}
	for _, member := range normalized.Members {
		fingerprint := memberFingerprint(member)
		if existing := p.active[member.ID]; existing != nil && existing.fingerprint == fingerprint && !existing.pending {
			prepared[member.ID] = existing
		}
	}
	p.mu.Unlock()

	created := make([]*resourceEntry, 0)
	for _, member := range normalized.Members {
		if _, reused := prepared[member.ID]; reused {
			continue
		}
		resource, createErr := p.factory.Create(ctx, member)
		if createErr != nil {
			for _, entry := range created {
				_ = entry.resource.Close()
			}
			return fmt.Errorf("%w: create member %s: %v", ErrResourceUnavailable, member.ID, createErr)
		}
		if resource == nil || resource.MemberID() != member.ID {
			if resource != nil {
				_ = resource.Close()
			}
			for _, entry := range created {
				_ = entry.resource.Close()
			}
			return fmt.Errorf("%w: factory returned invalid resource for member %s", ErrResourceUnavailable, member.ID)
		}
		entry := &resourceEntry{resource: resource, fingerprint: memberFingerprint(member)}
		prepared[member.ID] = entry
		created = append(created, entry)
	}
	if err := ctx.Err(); err != nil {
		for _, entry := range created {
			_ = entry.resource.Close()
		}
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		for _, entry := range created {
			_ = entry.resource.Close()
		}
		return ErrResourcePoolClosed
	}
	old := p.active
	p.active = prepared
	var closeNow []Resource
	for memberID, entry := range old {
		if prepared[memberID] != entry {
			entry.pending = true
			if entry.refs == 0 {
				entry.closed = true
				closeNow = append(closeNow, entry.resource)
			}
		}
	}
	p.mu.Unlock()
	for _, resource := range closeNow {
		_ = resource.Close()
	}
	return nil
}

func (p *ResourcePool) Acquire(ctx context.Context, memberID string) (ResourceHandle, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrResourcePoolClosed
	}
	entry := p.active[memberID]
	if entry == nil || entry.pending || entry.closed {
		return nil, ErrResourceUnavailable
	}
	entry.refs++
	return &resourceHandle{pool: p, entry: entry}, nil
}

func (h *resourceHandle) Resource() Resource { return h.entry.resource }

func (h *resourceHandle) Release() error {
	var resource Resource
	h.once.Do(func() {
		h.pool.mu.Lock()
		if h.entry.refs > 0 {
			h.entry.refs--
		}
		if h.entry.pending && h.entry.refs == 0 && !h.entry.closed {
			h.entry.closed = true
			resource = h.entry.resource
		}
		h.pool.mu.Unlock()
		if resource != nil {
			h.err = resource.Close()
		}
	})
	return h.err
}

func (p *ResourcePool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	old := p.active
	p.active = make(map[string]*resourceEntry)
	var closeNow []Resource
	for _, entry := range old {
		entry.pending = true
		if entry.refs == 0 && !entry.closed {
			entry.closed = true
			closeNow = append(closeNow, entry.resource)
		}
	}
	p.mu.Unlock()
	var closeErr error
	for _, resource := range closeNow {
		closeErr = errors.Join(closeErr, resource.Close())
	}
	return closeErr
}

func memberFingerprint(member Member) string {
	return fmt.Sprintf("%#v", cloneMember(member))
}

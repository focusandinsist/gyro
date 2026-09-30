package resource

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gyro/gyro"
)

var ErrPoolClosed = errors.New("resource pool is closed")

type resourceEntry struct {
	resource    gyro.Resource
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

type ResourcePool struct {
	mu      sync.Mutex
	factory gyro.ResourceFactory
	active  map[string]*resourceEntry
	closed  bool
}

func NewResourcePool(factory gyro.ResourceFactory) (*ResourcePool, error) {
	if factory == nil {
		return nil, fmt.Errorf("resource factory cannot be nil")
	}
	return &ResourcePool{factory: factory, active: make(map[string]*resourceEntry)}, nil
}

// NewOwnedPool accepts resources created by a protocol adapter. Ownership
// transfers on a successful Add and remains here until removal or Close.
func NewOwnedPool() *ResourcePool {
	return &ResourcePool{active: make(map[string]*resourceEntry)}
}

func (p *ResourcePool) Add(resource gyro.Resource) error {
	if resource == nil || resource.MemberID() == "" {
		return gyro.ErrResourceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	if _, exists := p.active[resource.MemberID()]; exists {
		return fmt.Errorf("resource %s already exists", resource.MemberID())
	}
	p.active[resource.MemberID()] = &resourceEntry{resource: resource}
	return nil
}

func (p *ResourcePool) Remove(memberID string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	entry := p.active[memberID]
	if entry == nil {
		p.mu.Unlock()
		return gyro.ErrResourceUnavailable
	}
	delete(p.active, memberID)
	entry.pending = true
	if entry.refs != 0 || entry.closed {
		p.mu.Unlock()
		return nil
	}
	entry.closed = true
	resource := entry.resource
	p.mu.Unlock()
	return resource.Close()
}

func (p *ResourcePool) Replace(ctx context.Context, members []gyro.Member) error {
	if p.factory == nil {
		return fmt.Errorf("owned resource pool does not create resources")
	}
	if ctx == nil {
		return gyro.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeSnapshot(gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "resource", Generation: 1, Token: "replace"},
		Members:  members,
	})
	if err != nil {
		return err
	}
	prepared := make(map[string]*resourceEntry, len(normalized.Members))
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
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
			closeCreated(created)
			return fmt.Errorf("%w: create member %s: %v", gyro.ErrResourceUnavailable, member.ID, createErr)
		}
		if resource == nil || resource.MemberID() != member.ID {
			if resource != nil {
				_ = resource.Close()
			}
			closeCreated(created)
			return fmt.Errorf("%w: factory returned invalid resource for member %s", gyro.ErrResourceUnavailable, member.ID)
		}
		entry := &resourceEntry{resource: resource, fingerprint: memberFingerprint(member)}
		prepared[member.ID] = entry
		created = append(created, entry)
	}
	if err := ctx.Err(); err != nil {
		closeCreated(created)
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		closeCreated(created)
		return ErrPoolClosed
	}
	old := p.active
	p.active = prepared
	closeNow := make([]gyro.Resource, 0)
	for memberID, entry := range old {
		if prepared[memberID] == entry {
			continue
		}
		entry.pending = true
		if entry.refs == 0 && !entry.closed {
			entry.closed = true
			closeNow = append(closeNow, entry.resource)
		}
	}
	p.mu.Unlock()
	for _, resource := range closeNow {
		_ = resource.Close()
	}
	return nil
}

func (p *ResourcePool) Acquire(ctx context.Context, memberID string) (gyro.ResourceHandle, error) {
	if ctx == nil {
		return nil, gyro.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPoolClosed
	}
	entry := p.active[memberID]
	if entry == nil || entry.pending || entry.closed {
		return nil, gyro.ErrResourceUnavailable
	}
	entry.refs++
	return &resourceHandle{pool: p, entry: entry}, nil
}

func (h *resourceHandle) Resource() gyro.Resource { return h.entry.resource }

func (h *resourceHandle) Release() error {
	var resource gyro.Resource
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
	closeNow := make([]gyro.Resource, 0)
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

func closeCreated(entries []*resourceEntry) {
	for _, entry := range entries {
		_ = entry.resource.Close()
	}
}

func memberFingerprint(member gyro.Member) string { return fmt.Sprintf("%#v", member) }

func normalizeSnapshot(snapshot gyro.TopologySnapshot) (gyro.TopologySnapshot, error) {
	if snapshot.Revision.Source == "" {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	seen := make(map[string]struct{}, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.ID == "" {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		if _, exists := seen[member.ID]; exists {
			return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
		}
		seen[member.ID] = struct{}{}
	}
	return snapshot, nil
}

package routing

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"gyro"
	"gyro/internal/health"
	"gyro/internal/policy"
)

// HealthAwarePool coordinates selection, policy and health observations. It
// never closes nodes directly; each locator delegates that to ResourcePool.
type HealthAwarePool struct {
	monitorMu sync.Mutex
	mu        sync.RWMutex
	locator   *Locator
	observer  *health.Observer
	policy    gyro.FailurePolicy
	closed    bool
	closeErr  error
	logger    atomic.Pointer[slog.Logger]
}

var _ gyro.Locator = (*HealthAwarePool)(nil)
var _ gyro.HealthView = (*HealthAwarePool)(nil)

var poolDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func NewHealthAwarePool(locator *Locator, config gyro.HealthCheckerConfig) *HealthAwarePool {
	return NewHealthAwarePoolWithChecker(locator, health.NewDefaultHealthChecker(config))
}

func NewHealthAwarePoolWithChecker(locator *Locator, checker gyro.HealthChecker) *HealthAwarePool {
	return NewHealthAwarePoolWithCheckerAndPolicy(locator, checker, policy.PrimaryOnly{})
}

func NewHealthAwarePoolWithCheckerAndPolicy(locator *Locator, checker gyro.HealthChecker, failurePolicy gyro.FailurePolicy) *HealthAwarePool {
	if failurePolicy == nil {
		failurePolicy = policy.PrimaryOnly{}
	}
	p := &HealthAwarePool{locator: locator, observer: health.NewObserver(checker, locator.GetAllNodes()), policy: failurePolicy}
	p.logger.Store(poolDiscardLogger)
	return p
}

func (p *HealthAwarePool) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = poolDiscardLogger
	}
	p.logger.Store(logger)
	p.mu.RLock()
	locator := p.locator
	p.mu.RUnlock()
	if locator != nil {
		locator.SetLogger(logger)
	}
}

func (p *HealthAwarePool) currentLocator() *Locator {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.locator
}

// NodeLease keeps the selected node alive across topology replacement or Close.
type NodeLease struct {
	node   gyro.Node
	handle gyro.ResourceHandle
}

func (l *NodeLease) Node() gyro.Node { return l.node }
func (l *NodeLease) Release() error  { return l.handle.Release() }

func (p *HealthAwarePool) BorrowNodeForKey(ctx context.Context, key string) (*NodeLease, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	locator := p.locator
	if locator == nil {
		return nil, gyro.ErrLocatorClosed
	}
	locator.mu.RLock()
	defer locator.mu.RUnlock()
	snapshot, selection, ordered, err := locator.selectRouteLocked(ctx, key)
	if err != nil {
		return nil, err
	}
	nodeByID := make(map[string]gyro.Node, len(ordered))
	for _, node := range ordered {
		nodeByID[node.ID()] = node
	}
	decision, err := p.policy.Decide(ctx, gyro.RouteRequest{Key: key}, snapshot, selection, p.observer)
	if err != nil {
		return nil, err
	}
	node := nodeByID[decision.Primary.ID]
	if node == nil {
		return nil, gyro.ErrSelectionMismatch
	}
	handle, err := locator.resources.Acquire(ctx, node.ID())
	if err != nil {
		return nil, err
	}
	return &NodeLease{node: node, handle: handle}, nil
}

func (p *HealthAwarePool) Get(ctx context.Context, key string) (gyro.Node, error) {
	lease, err := p.BorrowNodeForKey(ctx, key)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	return lease.Node(), nil
}

// GetReplicas returns selector candidates, independent of health policy.
func (p *HealthAwarePool) GetReplicas(ctx context.Context, key string, count int) ([]gyro.Node, error) {
	locator := p.currentLocator()
	if locator == nil {
		return nil, gyro.ErrLocatorClosed
	}
	return locator.GetReplicas(ctx, key, count)
}

func (p *HealthAwarePool) GetAllNodes() []gyro.Node {
	if locator := p.currentLocator(); locator != nil {
		return locator.GetAllNodes()
	}
	return nil
}

func (p *HealthAwarePool) AddNodeContext(ctx context.Context, node gyro.Node) error {
	p.monitorMu.Lock()
	defer p.monitorMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	locator := p.locator
	if locator == nil {
		return gyro.ErrLocatorClosed
	}
	if err := locator.AddNodeContext(ctx, node); err != nil {
		return err
	}
	p.observer.AddNode(node)
	return nil
}

func (p *HealthAwarePool) RemoveNodeContext(ctx context.Context, id string) error {
	p.monitorMu.Lock()
	defer p.monitorMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	locator := p.locator
	if locator == nil {
		return gyro.ErrLocatorClosed
	}
	err := locator.RemoveNodeContext(ctx, id)
	if err == nil {
		p.observer.RemoveNode(id)
	}
	return err
}

func (p *HealthAwarePool) StartHealthMonitoring(ctx context.Context) {
	p.monitorMu.Lock()
	defer p.monitorMu.Unlock()
	if p.currentLocator() != nil {
		p.observer.Start(ctx)
	}
}

func (p *HealthAwarePool) StopHealthMonitoring() { p.observer.Stop() }

func (p *HealthAwarePool) UpdateHealthCheckerConfig(config gyro.HealthCheckerConfig) error {
	return p.observer.UpdateConfig(config)
}

func (p *HealthAwarePool) GetHealthCheckerConfig() gyro.HealthCheckerConfig {
	return p.observer.Config()
}
func (p *HealthAwarePool) GetHealthyNodeCount() int               { return p.observer.Stats().HealthyNodes }
func (p *HealthAwarePool) GetUnhealthyNodeCount() int             { return p.observer.Stats().UnhealthyNodes }
func (p *HealthAwarePool) GetHealthStatus() map[string]bool       { return p.observer.HealthStatus() }
func (p *HealthAwarePool) IsNodeHealthy(id string) bool           { return p.observer.IsNodeHealthy(id) }
func (p *HealthAwarePool) Status(id string) gyro.HealthStatus     { return p.observer.Status(id) }
func (p *HealthAwarePool) Snapshot() map[string]gyro.HealthStatus { return p.observer.Snapshot() }
func (p *HealthAwarePool) GetStats() gyro.HealthAwarePoolStats    { return p.observer.Stats() }

func (p *HealthAwarePool) ReplaceLocator(next *Locator) error {
	if next == nil {
		return fmt.Errorf("new locator cannot be nil")
	}
	p.monitorMu.Lock()
	defer p.monitorMu.Unlock()
	p.mu.Lock()
	if p.closed || p.locator == nil {
		p.mu.Unlock()
		return gyro.ErrLocatorClosed
	}
	if p.locator == next {
		p.mu.Unlock()
		return fmt.Errorf("new locator is already active")
	}
	old := p.locator
	p.observer.Replace(next.GetAllNodes())
	p.locator = next
	p.mu.Unlock()
	next.SetLogger(p.logger.Load())
	if err := old.Close(); err != nil {
		p.logger.Load().Error("failed to close replaced locator", "error", err)
	}
	return nil
}

func (p *HealthAwarePool) Close() error {
	p.monitorMu.Lock()
	defer p.monitorMu.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.closeErr
	}
	p.closed = true
	locator := p.locator
	p.locator = nil
	p.mu.Unlock()
	p.observer.Close()
	if locator != nil {
		p.closeErr = locator.Close()
	}
	return p.closeErr
}

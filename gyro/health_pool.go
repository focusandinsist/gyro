package gyro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/focusandinsist/consistent-go/consistent"
)

// HealthAwarePool wraps a Locator with health checking capabilities.
type HealthAwarePool struct {
	monitorMu      sync.Mutex
	healthChecker  HealthChecker
	snapshot       atomic.Pointer[poolSnapshot]
	mu             sync.RWMutex
	closed         bool
	monitorStarted bool
	closeOnce      sync.Once
	closeErr       error
	logger         atomic.Pointer[slog.Logger]
}

type poolSnapshot struct {
	locator      Locator
	healthyNodes map[string]bool
}

// NewHealthAwarePool creates a new health-aware locator with a default health checker.
func NewHealthAwarePool(locator Locator, config HealthCheckerConfig) *HealthAwarePool {
	healthChecker := NewDefaultHealthChecker(config)
	return NewHealthAwarePoolWithChecker(locator, healthChecker)
}

// NewHealthAwarePoolWithChecker creates a new health-aware locator with an injected health checker.
func NewHealthAwarePoolWithChecker(locator Locator, healthChecker HealthChecker) *HealthAwarePool {
	hap := &HealthAwarePool{
		healthChecker: healthChecker,
	}
	hap.logger.Store(discardLogger)
	healthyNodes := make(map[string]bool)

	for _, node := range locator.GetAllNodes() {
		healthyNodes[node.ID()] = true
		if healthChecker != nil {
			healthChecker.AddNode(node)
		}
	}
	hap.snapshot.Store(&poolSnapshot{locator: locator, healthyNodes: healthyNodes})

	// The health listener is registered in StartHealthMonitoring, not here,
	// to avoid double-registering it if StartHealthMonitoring runs later.
	return hap
}

// SetLogger overrides the logger used for internal diagnostics. Passing nil
// restores the default no-op logger.
func (hap *HealthAwarePool) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardLogger
	}
	hap.logger.Store(logger)
	if locator := hap.currentLocator(); locator != nil {
		if setter, ok := locator.(interface{ SetLogger(*slog.Logger) }); ok {
			setter.SetLogger(logger)
		}
	}
}

func (hap *HealthAwarePool) log() *slog.Logger {
	return hap.logger.Load()
}

func (hap *HealthAwarePool) currentLocator() Locator {
	if snapshot := hap.snapshot.Load(); snapshot != nil {
		return snapshot.locator
	}
	return nil
}

func (hap *HealthAwarePool) currentSnapshot() *poolSnapshot {
	return hap.snapshot.Load()
}

func cloneHealthNodes(nodes map[string]bool) map[string]bool {
	clone := make(map[string]bool, len(nodes))
	for id, healthy := range nodes {
		clone[id] = healthy
	}
	return clone
}

// GetReplicas returns replicas from the currently active locator.
func (hap *HealthAwarePool) GetReplicas(ctx context.Context, key string, count int) ([]Node, error) {
	locator := hap.currentLocator()
	if locator == nil {
		return nil, ErrLocatorClosed
	}
	return locator.GetReplicas(ctx, key, count)
}

// GetAllNodes returns nodes from the currently active locator.
func (hap *HealthAwarePool) GetAllNodes() []Node {
	locator := hap.currentLocator()
	if locator == nil {
		return nil
	}
	return locator.GetAllNodes()
}

// Get retrieves a healthy node for the given key.
func (hap *HealthAwarePool) Get(ctx context.Context, key string) (Node, error) {
	snapshot := hap.currentSnapshot()
	locator := snapshot.locator
	if locator == nil {
		return nil, ErrLocatorClosed
	}
	node, err := locator.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Cheap check against cached health state; no network I/O here.
	isHealthy, exists := snapshot.healthyNodes[node.ID()]

	if !exists || isHealthy {
		return node, nil
	}

	replicaCount := len(locator.GetAllNodes())
	if replicaCount > 3 {
		replicaCount = 3
	}
	var replicas []Node
	for replicaCount > 0 {
		replicas, err = locator.GetReplicas(ctx, key, replicaCount)
		if err == nil {
			break
		}
		if !errors.Is(err, consistent.ErrInsufficientMemberCount) {
			return nil, err
		}
		replicaCount--
	}
	if err != nil {
		return nil, err
	}

	for _, replica := range replicas {
		if replica.ID() == node.ID() {
			continue // already known unhealthy
		}
		if healthy, exists := snapshot.healthyNodes[replica.ID()]; !exists || healthy {
			return replica, nil
		}
	}

	// Every replica is unhealthy too; return the primary and let the caller decide.
	return node, nil
}

// StartHealthMonitoring starts health monitoring for all nodes in the locator.
func (hap *HealthAwarePool) StartHealthMonitoring(ctx context.Context) {
	if ctx == nil {
		return
	}
	hap.monitorMu.Lock()
	defer hap.monitorMu.Unlock()
	hap.mu.Lock()
	if hap.closed || hap.monitorStarted {
		hap.mu.Unlock()
		return
	}
	hap.monitorStarted = true
	hap.mu.Unlock()

	for _, node := range hap.GetAllNodes() {
		hap.healthChecker.AddNode(node)
	}

	hap.healthChecker.AddHealthListener(func(nodeID string, healthy bool) {
		hap.mu.Lock()
		if hap.closed {
			hap.mu.Unlock()
			return
		}
		snapshot := hap.currentSnapshot()
		nodes := cloneHealthNodes(snapshot.healthyNodes)
		nodes[nodeID] = healthy
		hap.snapshot.Store(&poolSnapshot{locator: snapshot.locator, healthyNodes: nodes})
		hap.mu.Unlock()

		hap.log().Info("node health changed", "node_id", nodeID, "healthy", healthy)
	})

	hap.healthChecker.StartMonitoring(ctx)
}

// AddNodeContext adds a node to both the locator and health monitoring.
func (hap *HealthAwarePool) AddNodeContext(ctx context.Context, node Node) error {
	hap.monitorMu.Lock()
	defer hap.monitorMu.Unlock()
	locator := hap.currentLocator()
	if locator == nil {
		return ErrLocatorClosed
	}
	if err := locator.AddNodeContext(ctx, node); err != nil {
		return err
	}

	hap.healthChecker.AddNode(node)

	hap.updateNodeHealth(node.ID(), true)

	return nil
}

// RemoveNodeContext removes a node from both the locator and health monitoring.
func (hap *HealthAwarePool) RemoveNodeContext(ctx context.Context, nodeID string) error {
	hap.monitorMu.Lock()
	defer hap.monitorMu.Unlock()
	locator := hap.currentLocator()
	if locator == nil {
		return ErrLocatorClosed
	}
	if err := locator.RemoveNodeContext(ctx, nodeID); err != nil {
		return err
	}

	hap.healthChecker.RemoveNode(nodeID)

	hap.updateNodeHealth(nodeID, false)

	return nil
}

func (hap *HealthAwarePool) updateNodeHealth(nodeID string, healthy bool) {
	hap.mu.Lock()
	defer hap.mu.Unlock()
	snapshot := hap.currentSnapshot()
	nodes := cloneHealthNodes(snapshot.healthyNodes)
	if healthy {
		nodes[nodeID] = true
	} else {
		delete(nodes, nodeID)
	}
	hap.snapshot.Store(&poolSnapshot{locator: snapshot.locator, healthyNodes: nodes})
}

// StopHealthMonitoring stops health monitoring.
func (hap *HealthAwarePool) StopHealthMonitoring() {
	hap.healthChecker.StopMonitoring()
}

// UpdateHealthCheckerConfig updates the health checker configuration dynamically
func (hap *HealthAwarePool) UpdateHealthCheckerConfig(newConfig HealthCheckerConfig) error {
	checker, ok := hap.healthChecker.(ConfigurableHealthChecker)
	if !ok {
		return fmt.Errorf("health checker does not support runtime configuration")
	}
	return checker.UpdateConfig(newConfig)
}

// GetHealthCheckerConfig returns the current health checker configuration
func (hap *HealthAwarePool) GetHealthCheckerConfig() HealthCheckerConfig {
	checker, ok := hap.healthChecker.(ConfigurableHealthChecker)
	if !ok {
		return HealthCheckerConfig{}
	}
	return checker.GetConfig()
}

// GetHealthyNodeCount returns the number of currently healthy nodes
func (hap *HealthAwarePool) GetHealthyNodeCount() int {
	snapshot := hap.currentSnapshot()

	count := 0
	for _, healthy := range snapshot.healthyNodes {
		if healthy {
			count++
		}
	}
	return count
}

// GetUnhealthyNodeCount returns the number of currently unhealthy nodes
func (hap *HealthAwarePool) GetUnhealthyNodeCount() int {
	snapshot := hap.currentSnapshot()

	count := 0
	for _, healthy := range snapshot.healthyNodes {
		if !healthy {
			count++
		}
	}
	return count
}

// GetHealthStatus returns the health status of all nodes
func (hap *HealthAwarePool) GetHealthStatus() map[string]bool {
	snapshot := hap.currentSnapshot()
	status := make(map[string]bool, len(snapshot.healthyNodes))
	for nodeID, healthy := range snapshot.healthyNodes {
		status[nodeID] = healthy
	}
	return status
}

// IsNodeHealthy returns whether a specific node is healthy
func (hap *HealthAwarePool) IsNodeHealthy(nodeID string) bool {
	snapshot := hap.currentSnapshot()
	healthy, exists := snapshot.healthyNodes[nodeID]
	return !exists || healthy // unknown nodes are assumed healthy
}

// GetStats returns statistics about the health-aware pool including health status.
func (hap *HealthAwarePool) GetStats() HealthAwarePoolStats {
	snapshot := hap.currentSnapshot()
	stats := HealthAwarePoolStats{TotalNodes: len(snapshot.healthyNodes)}
	for _, healthy := range snapshot.healthyNodes {
		if healthy {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
	}

	return stats
}

// ReplaceLocator atomically switches the pool to a fully built locator. The
// existing health checker remains attached to the pool, so
// health-based failover continues across the replacement. Nodes removed from
// the topology are detached from the checker before the old locator closes.
func (hap *HealthAwarePool) ReplaceLocator(newLocator Locator) error {
	if newLocator == nil {
		return fmt.Errorf("new locator cannot be nil")
	}

	// Close and replacement are one lifecycle transaction. Close waits for a
	// replacement already in progress, while a replacement that starts after
	// Close observes the closed flag and is rejected before publication.
	hap.monitorMu.Lock()
	defer hap.monitorMu.Unlock()

	hap.mu.RLock()
	closed := hap.closed
	hap.mu.RUnlock()
	if closed {
		return fmt.Errorf("health-aware pool is closed")
	}

	oldLocator := hap.currentLocator()
	if oldLocator == newLocator {
		return fmt.Errorf("new locator is already active")
	}
	oldNodes := oldLocator.GetAllNodes()
	newNodes := newLocator.GetAllNodes()

	newHealthyNodes := make(map[string]bool, len(newNodes))
	newNodeIDs := make(map[string]struct{}, len(newNodes))
	for _, node := range newNodes {
		newHealthyNodes[node.ID()] = true
		newNodeIDs[node.ID()] = struct{}{}
	}

	hap.mu.Lock()
	hap.snapshot.Store(&poolSnapshot{locator: newLocator, healthyNodes: newHealthyNodes})
	hap.mu.Unlock()
	if setter, ok := newLocator.(interface{ SetLogger(*slog.Logger) }); ok {
		setter.SetLogger(hap.log())
	}

	for _, node := range oldNodes {
		if _, stillPresent := newNodeIDs[node.ID()]; !stillPresent {
			hap.healthChecker.RemoveNode(node.ID())
		}
	}
	for _, node := range newNodes {
		hap.healthChecker.AddNode(node)
	}

	if err := oldLocator.Close(); err != nil {
		// The replacement has already been committed. Reporting an error here
		// would make ConfigManager roll back its snapshot while the new locator
		// is active, creating a configuration/runtime mismatch.
		hap.log().Error("failed to close replaced locator", "error", err)
	}
	return nil
}

// Close closes the locator and stops health monitoring.
func (hap *HealthAwarePool) Close() error {
	hap.closeOnce.Do(func() {
		hap.monitorMu.Lock()
		defer hap.monitorMu.Unlock()
		oldLocator := hap.currentLocator()
		hap.mu.Lock()
		hap.closed = true
		hap.mu.Unlock()

		// The health checker invokes listeners asynchronously. Mark the pool
		// closed before stopping the checker so late callbacks are ignored.
		hap.StopHealthMonitoring()
		hap.snapshot.Store(&poolSnapshot{locator: nil, healthyNodes: map[string]bool{}})
		if oldLocator != nil {
			hap.closeErr = oldLocator.Close()
		}
	})

	return hap.closeErr
}

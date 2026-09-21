package gyro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/focusandinsist/consistent-go/consistent"
)

// HealthChecker provides health checking capabilities for nodes.
type HealthChecker interface {
	Check(ctx context.Context, node Node) error
	AddNode(node Node)
	RemoveNode(nodeID string)
	StartMonitoring(ctx context.Context)
	StopMonitoring()
	IsNodeHealthy(nodeID string) bool
	AddHealthListener(listener HealthListener)
	UpdateConfig(newConfig HealthCheckerConfig) error
	GetConfig() HealthCheckerConfig
	IsEnabled() bool
}

type HealthCheckerConfig struct {
	Enabled           bool          `json:"enabled"`
	Interval          time.Duration `json:"interval"`
	Timeout           time.Duration `json:"timeout"`
	FailureThreshold  int           `json:"failure_threshold"`
	RecoveryThreshold int           `json:"recovery_threshold"`
}

// ValidateHealthCheckerConfig validates values that are required by the
// monitoring runtime before it starts background workers.
func ValidateHealthCheckerConfig(config HealthCheckerConfig) error {
	if config.Interval <= 0 {
		return fmt.Errorf("health checker interval must be positive")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("health checker timeout must be positive")
	}
	if config.FailureThreshold <= 0 {
		return fmt.Errorf("health checker failure threshold must be positive")
	}
	if config.RecoveryThreshold <= 0 {
		return fmt.Errorf("health checker recovery threshold must be positive")
	}
	return nil
}

func DefaultHealthCheckerConfig() HealthCheckerConfig {
	return HealthCheckerConfig{
		Enabled:           true,
		Interval:          30 * time.Second,
		Timeout:           5 * time.Second,
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

type DefaultHealthChecker struct {
	lifecycleMu      sync.Mutex
	mu               sync.RWMutex
	config           HealthCheckerConfig
	nodes            map[string]Node
	nodeGenerations  map[string]uint64
	nodeStats        map[string]*NodeHealthStats
	healthListeners  []HealthListener
	notificationTail chan struct{}
	parentCtx        context.Context
	run              *healthCheckRun
	maxWorkers       int
}

type healthCheckRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	queue  chan Node
}

type NodeHealthStats struct {
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastCheckTime        time.Time
	IsHealthy            bool
	TotalChecks          int64
	TotalFailures        int64
}

type HealthListener func(nodeID string, healthy bool)

func NewDefaultHealthChecker(config HealthCheckerConfig) *DefaultHealthChecker {
	return &DefaultHealthChecker{
		config:          config,
		nodes:           make(map[string]Node),
		nodeGenerations: make(map[string]uint64),
		nodeStats:       make(map[string]*NodeHealthStats),
		healthListeners: make([]HealthListener, 0),
		maxWorkers:      10,
	}
}

// Check performs a health check on the given node.
func (hc *DefaultHealthChecker) Check(ctx context.Context, node Node) error {
	nodeID := node.ID()
	hc.mu.Lock()
	generation := hc.nodeGenerations[nodeID]
	config := hc.config
	stats, exists := hc.nodeStats[nodeID]
	if !exists {
		stats = &NodeHealthStats{IsHealthy: true} // optimistic until proven otherwise
		hc.nodeStats[nodeID] = stats
	}

	stats.TotalChecks++
	stats.LastCheckTime = time.Now()
	hc.mu.Unlock()

	checkCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	healthy := node.IsHealthy(checkCtx)
	hc.mu.Lock()
	if hc.nodeGenerations[nodeID] != generation {
		hc.mu.Unlock()
		return nil
	}
	stats = hc.nodeStats[nodeID]
	if stats == nil {
		hc.mu.Unlock()
		return nil
	}
	var listeners []HealthListener
	var previousNotification <-chan struct{}
	var notificationDone chan struct{}
	notify := false

	// Require FailureThreshold/RecoveryThreshold consecutive results before
	// flipping status, so a single flaky check doesn't cause flapping.
	if healthy {
		stats.ConsecutiveFailures = 0
		stats.ConsecutiveSuccesses++
		if !stats.IsHealthy && stats.ConsecutiveSuccesses >= hc.config.RecoveryThreshold {
			stats.IsHealthy = true
			notify = true
		}
	} else {
		stats.TotalFailures++
		stats.ConsecutiveSuccesses = 0
		stats.ConsecutiveFailures++
		if stats.IsHealthy && stats.ConsecutiveFailures >= hc.config.FailureThreshold {
			stats.IsHealthy = false
			notify = true
		}
	}
	if notify {
		listeners = append(listeners, hc.healthListeners...)
		previousNotification = hc.notificationTail
		notificationDone = make(chan struct{})
		hc.notificationTail = notificationDone
	}
	hc.mu.Unlock()

	if notify {
		go func() {
			defer close(notificationDone)
			if previousNotification != nil {
				<-previousNotification
			}
			for _, listener := range listeners {
				listener(nodeID, healthy)
			}
		}()
	}

	return nil
}

// LastCheckTime returns the timestamp of the most recent health probe. A zero
// value means no probe has run yet.
func (hc *DefaultHealthChecker) LastCheckTime() time.Time {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	var latest time.Time
	for _, stats := range hc.nodeStats {
		if stats.LastCheckTime.After(latest) {
			latest = stats.LastCheckTime
		}
	}
	return latest
}

// AddNode adds a node to be monitored.
func (hc *DefaultHealthChecker) AddNode(node Node) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	nodeID := node.ID()
	hc.nodes[nodeID] = node
	hc.nodeGenerations[nodeID]++

	if _, exists := hc.nodeStats[nodeID]; !exists {
		hc.nodeStats[nodeID] = &NodeHealthStats{
			IsHealthy: true,
		}
	}
}

// RemoveNode removes a node from monitoring.
func (hc *DefaultHealthChecker) RemoveNode(nodeID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	delete(hc.nodes, nodeID)
	delete(hc.nodeStats, nodeID)
	hc.nodeGenerations[nodeID]++
}

// StartMonitoring starts continuous health monitoring.
func (hc *DefaultHealthChecker) StartMonitoring(ctx context.Context) {
	if ctx == nil || ctx.Err() != nil {
		return
	}
	hc.lifecycleMu.Lock()
	defer hc.lifecycleMu.Unlock()
	if hc.run != nil || hc.parentCtx != nil {
		return
	}

	hc.parentCtx = ctx
	hc.mu.RLock()
	config := hc.config
	hc.mu.RUnlock()
	if config.Enabled {
		hc.startRun(config.Interval)
	}
}

// StopMonitoring stops health monitoring.
func (hc *DefaultHealthChecker) StopMonitoring() {
	hc.lifecycleMu.Lock()
	defer hc.lifecycleMu.Unlock()
	hc.parentCtx = nil
	hc.stopRun()
}

// UpdateConfig updates the health checker configuration dynamically
func (hc *DefaultHealthChecker) UpdateConfig(newConfig HealthCheckerConfig) error {
	if err := ValidateHealthCheckerConfig(newConfig); err != nil {
		return err
	}

	hc.lifecycleMu.Lock()
	defer hc.lifecycleMu.Unlock()
	hc.mu.Lock()
	oldConfig := hc.config
	hc.mu.Unlock()
	if hc.run != nil && (oldConfig.Interval != newConfig.Interval || !newConfig.Enabled) {
		hc.stopRun()
	}
	hc.mu.Lock()
	hc.config = newConfig
	hc.mu.Unlock()
	if hc.run == nil && newConfig.Enabled && hc.parentCtx != nil && hc.parentCtx.Err() == nil {
		hc.startRun(newConfig.Interval)
	}

	return nil
}

// GetConfig returns the current configuration
func (hc *DefaultHealthChecker) GetConfig() HealthCheckerConfig {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.config
}

// IsEnabled returns whether health checking is enabled
func (hc *DefaultHealthChecker) IsEnabled() bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.config.Enabled
}

// AddHealthListener adds a health status change listener.
func (hc *DefaultHealthChecker) AddHealthListener(listener HealthListener) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.healthListeners = append(hc.healthListeners, listener)
}

// IsNodeHealthy returns the current health status of a node.
func (hc *DefaultHealthChecker) IsNodeHealthy(nodeID string) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	stats, exists := hc.nodeStats[nodeID]
	if !exists {
		return true // no data yet, assume healthy
	}

	return stats.IsHealthy
}

// GetNodeStats returns health statistics for a node.
func (hc *DefaultHealthChecker) GetNodeStats(nodeID string) *NodeHealthStats {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	stats, exists := hc.nodeStats[nodeID]
	if !exists {
		return nil
	}

	return &NodeHealthStats{
		ConsecutiveFailures:  stats.ConsecutiveFailures,
		ConsecutiveSuccesses: stats.ConsecutiveSuccesses,
		LastCheckTime:        stats.LastCheckTime,
		IsHealthy:            stats.IsHealthy,
		TotalChecks:          stats.TotalChecks,
		TotalFailures:        stats.TotalFailures,
	}
}

// startRun and stopRun are serialized by lifecycleMu. Each worker captures
// only its own run, so a restart cannot redirect an old worker to a new queue.
func (hc *DefaultHealthChecker) startRun(interval time.Duration) {
	ctx, cancel := context.WithCancel(hc.parentCtx)
	run := &healthCheckRun{
		ctx: ctx, cancel: cancel, done: make(chan struct{}), queue: make(chan Node, 100),
	}
	hc.run = run
	var workers sync.WaitGroup
	workers.Add(hc.maxWorkers + 1)
	for i := 0; i < hc.maxWorkers; i++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-run.ctx.Done():
					return
				case node := <-run.queue:
					if run.ctx.Err() != nil {
						return
					}
					if node != nil {
						hc.Check(run.ctx, node)
					}
				}
			}
		}()
	}
	go func() {
		defer workers.Done()
		hc.monitoringLoop(run, interval)
	}()
	go func() {
		workers.Wait()
		close(run.done)
	}()
}

func (hc *DefaultHealthChecker) stopRun() {
	if hc.run == nil {
		return
	}
	run := hc.run
	run.cancel()
	<-run.done
	hc.run = nil
}

// monitoringLoop runs the continuous health monitoring.
func (hc *DefaultHealthChecker) monitoringLoop(run *healthCheckRun, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-run.ctx.Done():
			return
		case <-ticker.C:
			hc.mu.RLock()
			nodes := make([]Node, 0, len(hc.nodes))
			for _, node := range hc.nodes {
				nodes = append(nodes, node)
			}
			hc.mu.RUnlock()

			for _, node := range nodes {
				select {
				case <-run.ctx.Done():
					return
				case run.queue <- node:
				default:
					// Worker pool is saturated; drop this node's check rather
					// than block the ticker loop until the next cycle.
				}
			}
		}
	}
}

// HealthAwarePoolStats contains statistics about a health-aware pool
type HealthAwarePoolStats struct {
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	UnhealthyNodes int `json:"unhealthy_nodes"`
}

// HealthAwarePool wraps a Locator with health checking capabilities.
type HealthAwarePool struct {
	Locator
	locatorMu      sync.RWMutex
	monitorMu      sync.Mutex
	healthChecker  HealthChecker
	healthyNodes   map[string]bool
	mu             sync.RWMutex
	closed         bool
	monitorStarted bool
	closeOnce      sync.Once
	closeErr       error
	logger         atomic.Pointer[slog.Logger]
}

// NewHealthAwarePool creates a new health-aware locator with a default health checker.
func NewHealthAwarePool(locator Locator, config HealthCheckerConfig) *HealthAwarePool {
	healthChecker := NewDefaultHealthChecker(config)
	return NewHealthAwarePoolWithChecker(locator, healthChecker)
}

// NewHealthAwarePoolWithChecker creates a new health-aware locator with an injected health checker.
func NewHealthAwarePoolWithChecker(locator Locator, healthChecker HealthChecker) *HealthAwarePool {
	hap := &HealthAwarePool{
		Locator:       locator,
		healthChecker: healthChecker,
		healthyNodes:  make(map[string]bool),
	}
	hap.logger.Store(discardLogger)

	for _, node := range locator.GetAllNodes() {
		hap.healthyNodes[node.ID()] = true
		if healthChecker != nil {
			healthChecker.AddNode(node)
		}
	}

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
	hap.locatorMu.RLock()
	defer hap.locatorMu.RUnlock()
	return hap.Locator
}

// GetReplicas returns replicas from the currently active locator.
func (hap *HealthAwarePool) GetReplicas(ctx context.Context, key string, count int) ([]Node, error) {
	return hap.currentLocator().GetReplicas(ctx, key, count)
}

// GetAllNodes returns nodes from the currently active locator.
func (hap *HealthAwarePool) GetAllNodes() []Node {
	return hap.currentLocator().GetAllNodes()
}

// Get retrieves a healthy node for the given key.
func (hap *HealthAwarePool) Get(ctx context.Context, key string) (Node, error) {
	locator := hap.currentLocator()
	node, err := locator.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Cheap check against cached health state; no network I/O here.
	hap.mu.RLock()
	isHealthy, exists := hap.healthyNodes[node.ID()]
	hap.mu.RUnlock()

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

	hap.mu.RLock()
	for _, replica := range replicas {
		if replica.ID() == node.ID() {
			continue // already known unhealthy
		}
		if healthy, exists := hap.healthyNodes[replica.ID()]; !exists || healthy {
			hap.mu.RUnlock()
			return replica, nil
		}
	}
	hap.mu.RUnlock()

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
		hap.healthyNodes[nodeID] = healthy
		hap.mu.Unlock()

		hap.log().Info("node health changed", "node_id", nodeID, "healthy", healthy)
	})

	hap.healthChecker.StartMonitoring(ctx)
}

// AddNodeContext adds a node to both the locator and health monitoring.
func (hap *HealthAwarePool) AddNodeContext(ctx context.Context, node Node) error {
	if err := hap.currentLocator().AddNodeContext(ctx, node); err != nil {
		return err
	}

	hap.healthChecker.AddNode(node)

	hap.mu.Lock()
	hap.healthyNodes[node.ID()] = true
	hap.mu.Unlock()

	return nil
}

// RemoveNodeContext removes a node from both the locator and health monitoring.
func (hap *HealthAwarePool) RemoveNodeContext(ctx context.Context, nodeID string) error {
	if err := hap.currentLocator().RemoveNodeContext(ctx, nodeID); err != nil {
		return err
	}

	hap.healthChecker.RemoveNode(nodeID)

	hap.mu.Lock()
	delete(hap.healthyNodes, nodeID)
	hap.mu.Unlock()

	return nil
}

// StopHealthMonitoring stops health monitoring.
func (hap *HealthAwarePool) StopHealthMonitoring() {
	hap.healthChecker.StopMonitoring()
}

// UpdateHealthCheckerConfig updates the health checker configuration dynamically
func (hap *HealthAwarePool) UpdateHealthCheckerConfig(newConfig HealthCheckerConfig) error {
	return hap.healthChecker.UpdateConfig(newConfig)
}

// GetHealthCheckerConfig returns the current health checker configuration
func (hap *HealthAwarePool) GetHealthCheckerConfig() HealthCheckerConfig {
	return hap.healthChecker.GetConfig()
}

// GetHealthyNodeCount returns the number of currently healthy nodes
func (hap *HealthAwarePool) GetHealthyNodeCount() int {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	count := 0
	for _, healthy := range hap.healthyNodes {
		if healthy {
			count++
		}
	}
	return count
}

// GetUnhealthyNodeCount returns the number of currently unhealthy nodes
func (hap *HealthAwarePool) GetUnhealthyNodeCount() int {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	count := 0
	for _, healthy := range hap.healthyNodes {
		if !healthy {
			count++
		}
	}
	return count
}

// GetHealthStatus returns the health status of all nodes
func (hap *HealthAwarePool) GetHealthStatus() map[string]bool {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	status := make(map[string]bool, len(hap.healthyNodes))
	for nodeID, healthy := range hap.healthyNodes {
		status[nodeID] = healthy
	}
	return status
}

// IsNodeHealthy returns whether a specific node is healthy
func (hap *HealthAwarePool) IsNodeHealthy(nodeID string) bool {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	healthy, exists := hap.healthyNodes[nodeID]
	return !exists || healthy // unknown nodes are assumed healthy
}

// GetStats returns statistics about the health-aware pool including health status.
func (hap *HealthAwarePool) GetStats() HealthAwarePoolStats {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	stats := HealthAwarePoolStats{TotalNodes: len(hap.healthyNodes)}
	for _, healthy := range hap.healthyNodes {
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

	hap.locatorMu.Lock()
	hap.Locator = newLocator
	hap.locatorMu.Unlock()
	if setter, ok := newLocator.(interface{ SetLogger(*slog.Logger) }); ok {
		setter.SetLogger(hap.log())
	}

	hap.mu.Lock()
	hap.healthyNodes = newHealthyNodes
	hap.mu.Unlock()

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
		hap.mu.Lock()
		hap.closed = true
		hap.mu.Unlock()

		// The health checker invokes listeners asynchronously. Mark the pool
		// closed before stopping the checker so late callbacks are ignored.
		hap.StopHealthMonitoring()
		hap.closeErr = hap.currentLocator().Close()
	})

	return hap.closeErr
}

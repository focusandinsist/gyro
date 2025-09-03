package gyro

import (
	"context"
	"sync"
	"time"
)

// HealthChecker provides health checking capabilities for nodes.
type HealthChecker interface {
	// Check performs a health check on the given node.
	Check(ctx context.Context, node Node) error

	// StartMonitoring starts continuous health monitoring.
	StartMonitoring(ctx context.Context, interval time.Duration)

	// StopMonitoring stops health monitoring.
	StopMonitoring()
}

type HealthCheckerConfig struct {
	Enabled           bool          `json:"enabled"`
	Interval          time.Duration `json:"interval"`
	Timeout           time.Duration `json:"timeout"`
	FailureThreshold  int           `json:"failure_threshold"`
	RecoveryThreshold int           `json:"recovery_threshold"`
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
	mu              sync.RWMutex
	config          HealthCheckerConfig
	nodeStats       map[string]*NodeHealthStats
	stopCh          chan struct{}
	running         bool
	healthListeners []HealthListener
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
		config:    config,
		nodeStats: make(map[string]*NodeHealthStats),
		stopCh:    make(chan struct{}),
	}
}

// Check performs a health check on the given node.
func (hc *DefaultHealthChecker) Check(ctx context.Context, node Node) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	nodeID := node.ID()
	stats, exists := hc.nodeStats[nodeID]
	if !exists {
		stats = &NodeHealthStats{
			IsHealthy: true, // Assume healthy initially
		}
		hc.nodeStats[nodeID] = stats
	}

	stats.TotalChecks++
	stats.LastCheckTime = time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, hc.config.Timeout)
	defer cancel()

	// Perform the actual health check
	healthy := node.IsHealthy(checkCtx)

	if healthy {
		stats.ConsecutiveFailures = 0
		stats.ConsecutiveSuccesses++

		// Mark as healthy if it wasn't before and meets recovery threshold
		if !stats.IsHealthy && stats.ConsecutiveSuccesses >= hc.config.RecoveryThreshold {
			stats.IsHealthy = true
			hc.notifyHealthChange(nodeID, true)
		}
	} else {
		stats.TotalFailures++
		stats.ConsecutiveSuccesses = 0
		stats.ConsecutiveFailures++

		// Mark as unhealthy if it was healthy before and meets failure threshold
		if stats.IsHealthy && stats.ConsecutiveFailures >= hc.config.FailureThreshold {
			stats.IsHealthy = false
			hc.notifyHealthChange(nodeID, false)
		}
	}

	return nil
}

// StartMonitoring starts continuous health monitoring.
func (hc *DefaultHealthChecker) StartMonitoring(ctx context.Context, interval time.Duration) {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = true
	hc.mu.Unlock()

	go hc.monitoringLoop(ctx, interval)
}

// StopMonitoring stops health monitoring.
func (hc *DefaultHealthChecker) StopMonitoring() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if !hc.running {
		return
	}

	hc.running = false
	close(hc.stopCh)
	hc.stopCh = make(chan struct{})
}

// UpdateConfig updates the health checker configuration dynamically
func (hc *DefaultHealthChecker) UpdateConfig(newConfig HealthCheckerConfig) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	oldConfig := hc.config
	hc.config = newConfig

	// If monitoring is running and interval changed, it need to restart
	if hc.running && oldConfig.Interval != newConfig.Interval {
		// Stop current monitoring
		hc.running = false
		close(hc.stopCh)
		hc.stopCh = make(chan struct{})

		// Restart with new interval if still enabled
		if newConfig.Enabled {
			hc.running = true
			go hc.monitoringLoop(context.Background(), newConfig.Interval)
		}
	} else if !oldConfig.Enabled && newConfig.Enabled && !hc.running {
		// Health checking was disabled, now enabled
		hc.running = true
		go hc.monitoringLoop(context.Background(), newConfig.Interval)
	} else if oldConfig.Enabled && !newConfig.Enabled && hc.running {
		// Health checking was enabled, now disabled
		hc.running = false
		close(hc.stopCh)
		hc.stopCh = make(chan struct{})
	}

	return nil
}

// GetConfig returns the current configuration
func (hc *DefaultHealthChecker) GetConfig() HealthCheckerConfig {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.config
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
		return true // Assume healthy if no stats available
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

// RemoveNode removes health tracking for a node.
func (hc *DefaultHealthChecker) RemoveNode(nodeID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	delete(hc.nodeStats, nodeID)
}

// monitoringLoop runs the continuous health monitoring.
func (hc *DefaultHealthChecker) monitoringLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.stopCh:
			return
		case <-ticker.C:
			// TODO:Need to be implemented with access to the actual nodes
			// Need to pass the pool or nodes to monitor
			// For now, this is a placeholder for the monitoring logic
		}
	}
}

// notifyHealthChange notifies all listeners about a health status change.
func (hc *DefaultHealthChecker) notifyHealthChange(nodeID string, healthy bool) {
	for _, listener := range hc.healthListeners {
		go listener(nodeID, healthy)
	}
}

// HealthEvent represents a health status change event
type HealthEvent struct {
	NodeID    string
	Healthy   bool
	Timestamp time.Time
}

// HealthAwarePool wraps a Pool with health checking capabilities.
type HealthAwarePool struct {
	Pool
	healthChecker   *DefaultHealthChecker
	healthyNodes    map[string]bool
	healthEventChan chan HealthEvent
	mu              sync.RWMutex
}

// NewHealthAwarePool creates a new health-aware pool.
func NewHealthAwarePool(pool Pool, config HealthCheckerConfig) *HealthAwarePool {
	healthChecker := NewDefaultHealthChecker(config)

	hap := &HealthAwarePool{
		Pool:            pool,
		healthChecker:   healthChecker,
		healthyNodes:    make(map[string]bool),
		healthEventChan: make(chan HealthEvent, 100), // Buffered channel for health events
	}

	// Initialize all nodes as healthy
	for _, node := range pool.GetAllNodes() {
		hap.healthyNodes[node.ID()] = true
	}

	// Add health listener to publish health events
	healthChecker.AddHealthListener(func(nodeID string, healthy bool) {
		event := HealthEvent{
			NodeID:    nodeID,
			Healthy:   healthy,
			Timestamp: time.Now(),
		}

		// Non-blocking send to avoid deadlocks
		select {
		case hap.healthEventChan <- event:
		default:
			// Channel is full, drop the event
			// TODO: add some log
		}
	})

	return hap
}

// Get retrieves a healthy node for the given key.
func (hap *HealthAwarePool) Get(ctx context.Context, key string) (Node, error) {
	// Fast path: get the primary node
	node, err := hap.Pool.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Quick health check using cached state
	hap.mu.RLock()
	isHealthy, exists := hap.healthyNodes[node.ID()]
	hap.mu.RUnlock()

	if !exists || isHealthy {
		return node, nil
	}

	// Node is known to be unhealthy, try replicas
	replicas, err := hap.Pool.GetReplicas(ctx, key, 3)
	if err != nil {
		return nil, err
	}

	// Find the first healthy replica
	hap.mu.RLock()
	for _, replica := range replicas {
		if replica.ID() == node.ID() {
			continue // Skip the primary node already checked
		}
		if healthy, exists := hap.healthyNodes[replica.ID()]; !exists || healthy {
			hap.mu.RUnlock()
			return replica, nil
		}
	}
	hap.mu.RUnlock()

	// No healthy replicas found, return the original node
	// The caller can decide how to handle this
	return node, nil
}

// StartHealthMonitoring starts health monitoring for all nodes in the pool.
func (hap *HealthAwarePool) StartHealthMonitoring(ctx context.Context) {
	if !hap.healthChecker.config.Enabled {
		return
	}

	// Start the monitoring loop with worker pool
	hap.healthChecker.StartMonitoring(ctx, hap.healthChecker.config.Interval)

	// Start event-driven health management
	go hap.startHealthEventProcessor(ctx)

	// Start a goroutine to periodically check all nodes using worker pool
	go hap.startHealthCheckWorkerPool(ctx)
}

// startHealthEventProcessor processes health events and updates internal state
func (hap *HealthAwarePool) startHealthEventProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-hap.healthEventChan:
			hap.processHealthEvent(event)
		}
	}
}

// processHealthEvent processes a single health event
func (hap *HealthAwarePool) processHealthEvent(event HealthEvent) {
	hap.mu.Lock()
	defer hap.mu.Unlock()

	oldStatus, existed := hap.healthyNodes[event.NodeID]
	hap.healthyNodes[event.NodeID] = event.Healthy

	// Log significant health changes (in production, use proper logging)
	if !existed {
		// New node discovered
		if event.Healthy {
			// fmt.Printf("New healthy node discovered: %s\n", event.NodeID)
		} else {
			// fmt.Printf("New unhealthy node discovered: %s\n", event.NodeID)
		}
	} else if oldStatus != event.Healthy {
		// Health status changed
		if event.Healthy {
			// fmt.Printf("Node recovered: %s\n", event.NodeID)
		} else {
			// fmt.Printf("Node became unhealthy: %s\n", event.NodeID)
		}
	}

	// TODO
	// 1. Temporarily adjust the node's weight in the consistent hash ring
	// 2. Trigger rebalancing if too many nodes are unhealthy
	// 3. Send alerts or notifications
	// 4. Update metrics and monitoring systems
}

// startHealthCheckWorkerPool starts a worker pool for health checks to control concurrency
func (hap *HealthAwarePool) startHealthCheckWorkerPool(ctx context.Context) {
	// Create a worker pool with limited concurrency
	const maxWorkers = 10
	healthCheckChan := make(chan Node, 100)

	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case node := <-healthCheckChan:
					if node != nil {
						hap.healthChecker.Check(ctx, node)
					}
				}
			}
		}()
	}

	// Periodically queue health checks
	ticker := time.NewTicker(hap.healthChecker.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nodes := hap.Pool.GetAllNodes()
			for _, node := range nodes {
				select {
				case healthCheckChan <- node:
					// Successfully queued
				default:
					// Channel is full, skip this check cycle for this node
					// This prevents blocking and resource exhaustion
				}
			}
		}
	}
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
	return !exists || healthy // Default to healthy if unknown
}

// Close closes the pool and stops health monitoring.
func (hap *HealthAwarePool) Close() error {
	hap.StopHealthMonitoring()
	close(hap.healthEventChan)
	return hap.Pool.Close()
}

// LoadBalancer provides load balancing strategies for node selection.
// Unused for now.
type LoadBalancer interface {
	// Select selects the best node from the given candidates.
	Select(ctx context.Context, nodes []Node, key string) (Node, error)

	// UpdateStats updates the load statistics for a node.
	UpdateStats(nodeID string, latency time.Duration, success bool)
}

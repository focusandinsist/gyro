package gyro

import (
	"context"
	"sync"
	"time"
)

type DefaultHealthChecker struct {
	checkerState
	checkerLifecycle
	checkerBroadcaster
	maxWorkers int
}

type checkerState struct {
	mu              sync.RWMutex
	config          HealthCheckerConfig
	nodes           map[string]Node
	nodeGenerations map[string]uint64
	nodeStats       map[string]*NodeHealthStats
}

type checkerBroadcaster struct {
	healthListeners  []HealthListener
	notificationTail chan struct{}
}

type checkerLifecycle struct {
	lifecycleMu sync.Mutex
	parentCtx   context.Context
	run         *healthCheckRun
}

type healthCheckRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	queue  chan Node
}

func NewDefaultHealthChecker(config HealthCheckerConfig) *DefaultHealthChecker {
	return &DefaultHealthChecker{
		checkerState: checkerState{
			config: config, nodes: make(map[string]Node),
			nodeGenerations: make(map[string]uint64), nodeStats: make(map[string]*NodeHealthStats),
		},
		checkerBroadcaster: checkerBroadcaster{healthListeners: make([]HealthListener, 0)},
		maxWorkers:         10,
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
	stats.LastCheckTime = time.Now()
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

package health

import (
	"context"
	"fmt"
	"sync"

	"gyro/gyro"
)

// Observer owns health membership and observations, never node resources.
type Observer struct {
	startMu            sync.Mutex
	mu                 sync.RWMutex
	checker            gyro.HealthChecker
	nodes              map[string]bool
	started            bool
	listenerRegistered bool
	closed             bool
}

var _ gyro.HealthView = (*Observer)(nil)

func NewObserver(checker gyro.HealthChecker, nodes []gyro.Node) *Observer {
	o := &Observer{checker: checker, nodes: make(map[string]bool)}
	for _, node := range nodes {
		o.AddNode(node)
	}
	return o
}

func (o *Observer) AddNode(node gyro.Node) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.nodes[node.ID()] = true
	if o.checker != nil {
		o.checker.AddNode(node)
	}
}

func (o *Observer) RemoveNode(id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.nodes, id)
	if o.checker != nil {
		o.checker.RemoveNode(id)
	}
}

func (o *Observer) Replace(nodes []gyro.Node) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	next := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		next[node.ID()] = true
	}
	if o.checker != nil {
		for id := range o.nodes {
			if _, exists := next[id]; !exists {
				o.checker.RemoveNode(id)
			}
		}
		for _, node := range nodes {
			o.checker.AddNode(node)
		}
	}
	o.nodes = next
}

func (o *Observer) Start(ctx context.Context) {
	if ctx == nil || o.checker == nil {
		return
	}
	o.startMu.Lock()
	defer o.startMu.Unlock()
	o.mu.Lock()
	if o.closed || o.started {
		o.mu.Unlock()
		return
	}
	o.started = true
	if !o.listenerRegistered {
		o.checker.AddHealthListener(o.onHealthChange)
		o.listenerRegistered = true
	}
	o.mu.Unlock()
	o.checker.StartMonitoring(ctx)
}

func (o *Observer) onHealthChange(id string, healthy bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	if _, exists := o.nodes[id]; !exists {
		return
	}
	if view, ok := o.checker.(gyro.HealthView); ok {
		status := view.Status(id)
		if status == gyro.Unknown || (healthy && status != gyro.Healthy) || (!healthy && status != gyro.Unhealthy) {
			return
		}
	}
	o.nodes[id] = healthy
}

func (o *Observer) Stop() {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	o.mu.Lock()
	o.started = false
	o.mu.Unlock()
	if o.checker != nil {
		o.checker.StopMonitoring()
	}
}

func (o *Observer) Close() {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	o.mu.Lock()
	o.closed = true
	o.started = false
	o.nodes = make(map[string]bool)
	o.mu.Unlock()
	if o.checker != nil {
		o.checker.StopMonitoring()
	}
}

func (o *Observer) Status(id string) gyro.HealthStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, exists := o.nodes[id]
	if !exists {
		return gyro.Unknown
	}
	if view, ok := o.checker.(gyro.HealthView); ok {
		return view.Status(id)
	}
	healthy := o.nodes[id]
	if healthy {
		return gyro.Healthy
	}
	return gyro.Unhealthy
}

func (o *Observer) Snapshot() map[string]gyro.HealthStatus {
	o.mu.RLock()
	defer o.mu.RUnlock()
	result := make(map[string]gyro.HealthStatus, len(o.nodes))
	if view, ok := o.checker.(gyro.HealthView); ok {
		for id := range o.nodes {
			result[id] = view.Status(id)
		}
		return result
	}
	for id, healthy := range o.nodes {
		if healthy {
			result[id] = gyro.Healthy
		} else {
			result[id] = gyro.Unhealthy
		}
	}
	return result
}

func (o *Observer) IsNodeHealthy(id string) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	healthy, exists := o.nodes[id]
	return !exists || healthy
}

func (o *Observer) HealthStatus() map[string]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	result := make(map[string]bool, len(o.nodes))
	for id, healthy := range o.nodes {
		result[id] = healthy
	}
	return result
}

func (o *Observer) Stats() gyro.HealthAwarePoolStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	stats := gyro.HealthAwarePoolStats{TotalNodes: len(o.nodes)}
	for _, healthy := range o.nodes {
		if healthy {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
	}
	return stats
}

func (o *Observer) UpdateConfig(config gyro.HealthCheckerConfig) error {
	checker, ok := o.checker.(gyro.ConfigurableHealthChecker)
	if !ok {
		return fmt.Errorf("health checker does not support runtime configuration")
	}
	return checker.UpdateConfig(config)
}

func (o *Observer) Config() gyro.HealthCheckerConfig {
	if checker, ok := o.checker.(gyro.ConfigurableHealthChecker); ok {
		return checker.GetConfig()
	}
	return gyro.HealthCheckerConfig{}
}

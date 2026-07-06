package gyro

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gyro/gyro/health/gossip"
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

	// Gossip is optional; nil/disabled means active polling is used instead.
	Gossip *GossipConfig `json:"gossip,omitempty"`
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
	nodes           map[string]Node
	nodeStats       map[string]*NodeHealthStats
	stopCh          chan struct{}
	running         bool
	healthListeners []HealthListener

	healthCheckChan chan Node
	maxWorkers      int
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

// GossipConfig configures the Gossip health checker
type GossipConfig struct {
	Enabled        bool                `json:"enabled"`
	ProbingRatio   float64             `json:"probing_ratio"` // fraction of nodes each client probes, e.g. 0.1 = 10%
	GossipInterval time.Duration       `json:"gossip_interval"`
	PeerDiscovery  PeerDiscoveryConfig `json:"peer_discovery"`
	Transport      TransportConfig     `json:"transport"`
	MaxPeers       int                 `json:"max_peers"`
	MessageTTL     time.Duration       `json:"message_ttl"`
}

// PeerDiscoveryConfig configures peer discovery
type PeerDiscoveryConfig struct {
	Type   string                 `json:"type"` // "static", "consul", "kubernetes"
	Config map[string]interface{} `json:"config"`
}

// TransportConfig configures the transport layer
type TransportConfig struct {
	Type     string                 `json:"type"` // "udp", "http"
	BindAddr string                 `json:"bind_addr"`
	Config   map[string]interface{} `json:"config"`
}

// DefaultGossipConfig returns the default Gossip configuration
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		Enabled:        false,
		ProbingRatio:   0.1,
		GossipInterval: 5 * time.Second,
		MaxPeers:       10,
		MessageTTL:     30 * time.Second,
		PeerDiscovery: PeerDiscoveryConfig{
			Type: "static",
		},
		Transport: TransportConfig{
			Type:     "udp",
			BindAddr: ":0", // let the OS assign a port
		},
	}
}

func NewDefaultHealthChecker(config HealthCheckerConfig) *DefaultHealthChecker {
	return &DefaultHealthChecker{
		config:          config,
		nodes:           make(map[string]Node),
		nodeStats:       make(map[string]*NodeHealthStats),
		stopCh:          make(chan struct{}),
		healthListeners: make([]HealthListener, 0),
		healthCheckChan: make(chan Node, 100),
		maxWorkers:      10,
	}
}

// GossipHealthCheckerAdapter adapts gossip.GossipHealthChecker to implement HealthChecker
type GossipHealthCheckerAdapter struct {
	*gossip.GossipHealthChecker
	config HealthCheckerConfig // kept separately since GossipHealthChecker uses its own config type
}

func (g *GossipHealthCheckerAdapter) UpdateConfig(newConfig HealthCheckerConfig) error {
	g.config = newConfig
	// TODO: push this down into the underlying GossipHealthChecker.
	return nil
}

func (g *GossipHealthCheckerAdapter) GetConfig() HealthCheckerConfig {
	return g.config
}

func (g *GossipHealthCheckerAdapter) IsEnabled() bool {
	return g.config.Enabled
}

// AddHealthListener bridges the main package's HealthListener type across
// the package boundary into the gossip package's own type.
func (ghca *GossipHealthCheckerAdapter) AddHealthListener(listener HealthListener) {
	ghca.GossipHealthChecker.AddHealthListener(gossip.HealthListener(listener))
}

// AddNode wraps a gyro.Node so it satisfies gossip.Node.
func (ghca *GossipHealthCheckerAdapter) AddNode(node Node) {
	ghca.GossipHealthChecker.AddNode(&nodeAdapter{node: node})
}

// nodeAdapter bridges gyro.Node to gossip.Node.
type nodeAdapter struct {
	node Node
}

func (na *nodeAdapter) ID() string {
	return na.node.ID()
}

func (na *nodeAdapter) IsHealthy(ctx context.Context) bool {
	return na.node.IsHealthy(ctx)
}

func (ghca *GossipHealthCheckerAdapter) Check(ctx context.Context, node Node) error {
	return ghca.GossipHealthChecker.Check(ctx, &nodeAdapter{node: node})
}

// NewHealthChecker creates a HealthChecker: a GossipHealthCheckerAdapter if
// config.Gossip is enabled, otherwise a DefaultHealthChecker.
func NewHealthChecker(config HealthCheckerConfig) (HealthChecker, error) {
	if config.Gossip != nil && config.Gossip.Enabled {
		gossipConfig := gossip.GossipConfig{
			Enabled:        config.Gossip.Enabled,
			ProbingRatio:   config.Gossip.ProbingRatio,
			GossipInterval: config.Gossip.GossipInterval,
			MaxPeers:       config.Gossip.MaxPeers,
			MessageTTL:     config.Gossip.MessageTTL,
			PeerDiscovery: gossip.PeerDiscoveryConfig{
				Type:   config.Gossip.PeerDiscovery.Type,
				Config: config.Gossip.PeerDiscovery.Config,
			},
			Transport: gossip.TransportConfig{
				Type:     config.Gossip.Transport.Type,
				BindAddr: config.Gossip.Transport.BindAddr,
				Config:   config.Gossip.Transport.Config,
			},
		}

		gossipChecker, err := gossip.NewGossipHealthChecker(gossipConfig)
		if err != nil {
			return nil, err
		}

		return &GossipHealthCheckerAdapter{
			GossipHealthChecker: gossipChecker,
			config:              config,
		}, nil
	}

	return NewDefaultHealthChecker(config), nil
}

// Check performs a health check on the given node.
func (hc *DefaultHealthChecker) Check(ctx context.Context, node Node) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	nodeID := node.ID()
	stats, exists := hc.nodeStats[nodeID]
	if !exists {
		stats = &NodeHealthStats{IsHealthy: true} // optimistic until proven otherwise
		hc.nodeStats[nodeID] = stats
	}

	stats.TotalChecks++
	stats.LastCheckTime = time.Now()

	checkCtx, cancel := context.WithTimeout(ctx, hc.config.Timeout)
	defer cancel()

	healthy := node.IsHealthy(checkCtx)

	// Require FailureThreshold/RecoveryThreshold consecutive results before
	// flipping status, so a single flaky check doesn't cause flapping.
	if healthy {
		stats.ConsecutiveFailures = 0
		stats.ConsecutiveSuccesses++
		if !stats.IsHealthy && stats.ConsecutiveSuccesses >= hc.config.RecoveryThreshold {
			stats.IsHealthy = true
			hc.notifyHealthChange(nodeID, true)
		}
	} else {
		stats.TotalFailures++
		stats.ConsecutiveSuccesses = 0
		stats.ConsecutiveFailures++
		if stats.IsHealthy && stats.ConsecutiveFailures >= hc.config.FailureThreshold {
			stats.IsHealthy = false
			hc.notifyHealthChange(nodeID, false)
		}
	}

	return nil
}

// AddNode adds a node to be monitored.
func (hc *DefaultHealthChecker) AddNode(node Node) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	nodeID := node.ID()
	hc.nodes[nodeID] = node

	if _, exists := hc.nodeStats[nodeID]; !exists {
		hc.nodeStats[nodeID] = &NodeHealthStats{
			IsHealthy:     true,
			LastCheckTime: time.Now(),
		}
	}
}

// RemoveNode removes a node from monitoring.
func (hc *DefaultHealthChecker) RemoveNode(nodeID string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	delete(hc.nodes, nodeID)
	delete(hc.nodeStats, nodeID)
}

// StartMonitoring starts continuous health monitoring.
func (hc *DefaultHealthChecker) StartMonitoring(ctx context.Context) {
	hc.mu.Lock()
	if hc.running {
		hc.mu.Unlock()
		return
	}
	hc.running = true
	hc.mu.Unlock()

	hc.startWorkerPool(ctx)
	go hc.monitoringLoop(ctx)
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

	// The ticker interval is fixed at monitoringLoop startup, so a change
	// requires stopping and restarting the loop rather than adjusting it live.
	if hc.running && oldConfig.Interval != newConfig.Interval {
		hc.running = false
		close(hc.stopCh)
		hc.stopCh = make(chan struct{})

		if newConfig.Enabled {
			hc.running = true
			hc.startWorkerPool(context.Background())
			go hc.monitoringLoop(context.Background())
		}
	} else if !oldConfig.Enabled && newConfig.Enabled && !hc.running {
		hc.running = true
		hc.startWorkerPool(context.Background())
		go hc.monitoringLoop(context.Background())
	} else if oldConfig.Enabled && !newConfig.Enabled && hc.running {
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

// startWorkerPool starts the worker pool for concurrent health checks.
func (hc *DefaultHealthChecker) startWorkerPool(ctx context.Context) {
	for i := 0; i < hc.maxWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-hc.stopCh:
					return
				case node := <-hc.healthCheckChan:
					if node != nil {
						hc.Check(ctx, node)
					}
				}
			}
		}()
	}
}

// monitoringLoop runs the continuous health monitoring.
func (hc *DefaultHealthChecker) monitoringLoop(ctx context.Context) {
	ticker := time.NewTicker(hc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.stopCh:
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
				case hc.healthCheckChan <- node:
				default:
					// Worker pool is saturated; drop this node's check rather
					// than block the ticker loop until the next cycle.
				}
			}
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

// HealthAwarePoolStats contains statistics about a health-aware pool
type HealthAwarePoolStats struct {
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	UnhealthyNodes int `json:"unhealthy_nodes"`
}

// HealthAwarePool wraps a Locator with health checking capabilities.
type HealthAwarePool struct {
	Locator
	healthChecker   HealthChecker
	healthyNodes    map[string]bool
	healthEventChan chan HealthEvent
	mu              sync.RWMutex
	logger          atomic.Pointer[slog.Logger]
}

// NewHealthAwarePool creates a new health-aware locator with a default health checker.
func NewHealthAwarePool(locator Locator, config HealthCheckerConfig) *HealthAwarePool {
	healthChecker := NewDefaultHealthChecker(config)
	return NewHealthAwarePoolWithChecker(locator, healthChecker)
}

// NewHealthAwarePoolWithChecker creates a new health-aware locator with an injected health checker.
func NewHealthAwarePoolWithChecker(locator Locator, healthChecker HealthChecker) *HealthAwarePool {
	hap := &HealthAwarePool{
		Locator:         locator,
		healthChecker:   healthChecker,
		healthyNodes:    make(map[string]bool),
		healthEventChan: make(chan HealthEvent, 100),
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
}

func (hap *HealthAwarePool) log() *slog.Logger {
	return hap.logger.Load()
}

// Get retrieves a healthy node for the given key.
func (hap *HealthAwarePool) Get(ctx context.Context, key string) (Node, error) {
	node, err := hap.Locator.Get(ctx, key)
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

	replicas, err := hap.Locator.GetReplicas(ctx, key, 3)
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
	if !hap.healthChecker.IsEnabled() {
		return
	}

	for _, node := range hap.Locator.GetAllNodes() {
		hap.healthChecker.AddNode(node)
	}

	hap.healthChecker.AddHealthListener(func(nodeID string, healthy bool) {
		hap.mu.Lock()
		hap.healthyNodes[nodeID] = healthy
		hap.mu.Unlock()

		select {
		case hap.healthEventChan <- HealthEvent{NodeID: nodeID, Healthy: healthy, Timestamp: time.Now()}:
		default:
			hap.log().Warn("health event dropped, event channel full", "node_id", nodeID, "healthy", healthy)
		}
	})

	hap.healthChecker.StartMonitoring(ctx)
	go hap.startHealthEventProcessor(ctx)
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

// processHealthEvent handles a single node health transition.
func (hap *HealthAwarePool) processHealthEvent(event HealthEvent) {
	hap.log().Info("node health changed", "node_id", event.NodeID, "healthy", event.Healthy)

	// TODO: weighted rebalancing, alerting, and circuit-breaking on this event.
}

// AddNode adds a node to both the locator and health monitoring.
func (hap *HealthAwarePool) AddNode(node Node) error {
	if err := hap.Locator.AddNode(node); err != nil {
		return err
	}

	hap.healthChecker.AddNode(node)

	hap.mu.Lock()
	hap.healthyNodes[node.ID()] = true
	hap.mu.Unlock()

	return nil
}

// RemoveNode removes a node from both the locator and health monitoring.
func (hap *HealthAwarePool) RemoveNode(nodeID string) error {
	if err := hap.Locator.RemoveNode(nodeID); err != nil {
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

// Close closes the locator and stops health monitoring.
func (hap *HealthAwarePool) Close() error {
	hap.StopHealthMonitoring()
	close(hap.healthEventChan)
	return hap.Locator.Close()
}

// LoadBalancer provides load balancing strategies for node selection.
// Unused for now.
type LoadBalancer interface {
	Select(ctx context.Context, nodes []Node, key string) (Node, error)
	UpdateStats(nodeID string, latency time.Duration, success bool)
}

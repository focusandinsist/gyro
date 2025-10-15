package gyro

import (
	"context"
	"sync"
	"time"

	"gyro/gyro/health/gossip"
)

// HealthChecker provides health checking capabilities for nodes.
type HealthChecker interface {
	// Check performs a health check on the given node.
	Check(ctx context.Context, node Node) error

	// AddNode adds a node to be monitored.
	AddNode(node Node)

	// RemoveNode removes a node from monitoring.
	RemoveNode(nodeID string)

	// StartMonitoring starts continuous health monitoring.
	StartMonitoring(ctx context.Context)

	// StopMonitoring stops health monitoring.
	StopMonitoring()

	// IsNodeHealthy returns the current health status of a node.
	IsNodeHealthy(nodeID string) bool

	// AddHealthListener adds a health status change listener.
	AddHealthListener(listener HealthListener)

	// UpdateConfig updates the health checker configuration dynamically.
	UpdateConfig(newConfig HealthCheckerConfig) error

	// GetConfig returns the current health checker configuration.
	GetConfig() HealthCheckerConfig

	// IsEnabled returns whether health checking is enabled.
	IsEnabled() bool
}

type HealthCheckerConfig struct {
	Enabled           bool          `json:"enabled"`
	Interval          time.Duration `json:"interval"`
	Timeout           time.Duration `json:"timeout"`
	FailureThreshold  int           `json:"failure_threshold"`
	RecoveryThreshold int           `json:"recovery_threshold"`

	// Gossip advanced configuration (optional)
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

	// Worker pool for concurrent health checks
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
	ProbingRatio   float64             `json:"probing_ratio"`   // Ratio of nodes each client probes (0.1 = 10%)
	GossipInterval time.Duration       `json:"gossip_interval"` // Gossip communication interval
	PeerDiscovery  PeerDiscoveryConfig `json:"peer_discovery"`  // Peer discovery configuration
	Transport      TransportConfig     `json:"transport"`       // Transport layer configuration
	MaxPeers       int                 `json:"max_peers"`       // Maximum number of peers
	MessageTTL     time.Duration       `json:"message_ttl"`     // Message time-to-live
}

// PeerDiscoveryConfig configures peer discovery
type PeerDiscoveryConfig struct {
	Type   string                 `json:"type"`   // "static", "consul", "kubernetes"
	Config map[string]interface{} `json:"config"` // Specific configuration
}

// TransportConfig configures the transport layer
type TransportConfig struct {
	Type     string                 `json:"type"`      // "udp", "http"
	BindAddr string                 `json:"bind_addr"` // Listening address
	Config   map[string]interface{} `json:"config"`    // Specific configuration
}

// DefaultGossipConfig returns the default Gossip configuration
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		Enabled:        false, // Disabled by default
		ProbingRatio:   0.1,   // Probe 10% of nodes
		GossipInterval: 5 * time.Second,
		MaxPeers:       10,
		MessageTTL:     30 * time.Second,
		PeerDiscovery: PeerDiscoveryConfig{
			Type: "static",
		},
		Transport: TransportConfig{
			Type:     "udp",
			BindAddr: ":0", // Random port
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
		healthCheckChan: make(chan Node, 100), // Buffered channel for health check tasks
		maxWorkers:      10,                   // Default worker pool size
	}
}

// GossipHealthCheckerAdapter adapts gossip.GossipHealthChecker to implement HealthChecker
type GossipHealthCheckerAdapter struct {
	*gossip.GossipHealthChecker
	config HealthCheckerConfig // Store the original config for interface compliance
}

// UpdateConfig updates the health checker configuration dynamically
func (g *GossipHealthCheckerAdapter) UpdateConfig(newConfig HealthCheckerConfig) error {
	g.config = newConfig
	// TODO: Update the underlying gossip health checker configuration
	return nil
}

// GetConfig returns the current health checker configuration
func (g *GossipHealthCheckerAdapter) GetConfig() HealthCheckerConfig {
	return g.config
}

// IsEnabled returns whether health checking is enabled
func (g *GossipHealthCheckerAdapter) IsEnabled() bool {
	return g.config.Enabled
}

// AddHealthListener adapts the gossip health listener to the main package interface
func (ghca *GossipHealthCheckerAdapter) AddHealthListener(listener HealthListener) {
	// Convert main package HealthListener to gossip package HealthListener
	gossipListener := gossip.HealthListener(listener)
	ghca.GossipHealthChecker.AddHealthListener(gossipListener)
}

// AddNode adapts the main package Node to gossip package Node
func (ghca *GossipHealthCheckerAdapter) AddNode(node Node) {
	// Create an adapter that implements gossip.Node interface
	gossipNode := &nodeAdapter{node: node}
	ghca.GossipHealthChecker.AddNode(gossipNode)
}

// nodeAdapter adapts main package Node to gossip package Node
type nodeAdapter struct {
	node Node
}

func (na *nodeAdapter) ID() string {
	return na.node.ID()
}

func (na *nodeAdapter) IsHealthy(ctx context.Context) bool {
	return na.node.IsHealthy(ctx)
}

// Check adapts the Check method
func (ghca *GossipHealthCheckerAdapter) Check(ctx context.Context, node Node) error {
	gossipNode := &nodeAdapter{node: node}
	return ghca.GossipHealthChecker.Check(ctx, gossipNode)
}

// NewHealthChecker creates an appropriate health checker based on configuration
func NewHealthChecker(config HealthCheckerConfig) (HealthChecker, error) {
	// If Gossip is enabled, create GossipHealthChecker
	if config.Gossip != nil && config.Gossip.Enabled {
		// Convert main package config to gossip package config
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

	// Otherwise create default health checker
	return NewDefaultHealthChecker(config), nil
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

// AddNode adds a node to be monitored.
func (hc *DefaultHealthChecker) AddNode(node Node) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	nodeID := node.ID()
	hc.nodes[nodeID] = node

	// Initialize health stats for the new node
	if _, exists := hc.nodeStats[nodeID]; !exists {
		hc.nodeStats[nodeID] = &NodeHealthStats{
			IsHealthy:     true, // Assume healthy initially
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

	// Start worker pool
	hc.startWorkerPool(ctx)

	// Start monitoring loop
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

	// If monitoring is running and interval changed, it need to restart
	if hc.running && oldConfig.Interval != newConfig.Interval {
		// Stop current monitoring
		hc.running = false
		close(hc.stopCh)
		hc.stopCh = make(chan struct{})

		// Restart with new interval if still enabled
		if newConfig.Enabled {
			hc.running = true
			hc.startWorkerPool(context.Background())
			go hc.monitoringLoop(context.Background())
		}
	} else if !oldConfig.Enabled && newConfig.Enabled && !hc.running {
		// Health checking was disabled, now enabled
		hc.running = true
		hc.startWorkerPool(context.Background())
		go hc.monitoringLoop(context.Background())
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

// startWorkerPool starts the worker pool for concurrent health checks.
func (hc *DefaultHealthChecker) startWorkerPool(ctx context.Context) {
	// Start worker goroutines
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
			// Get all nodes to check
			hc.mu.RLock()
			nodes := make([]Node, 0, len(hc.nodes))
			for _, node := range hc.nodes {
				nodes = append(nodes, node)
			}
			hc.mu.RUnlock()

			// Queue health checks for all nodes
			for _, node := range nodes {
				select {
				case hc.healthCheckChan <- node:
					// Successfully queued
				default:
					// Channel is full, skip this check cycle for this node
					// This prevents blocking and resource exhaustion
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

// HealthAwarePool wraps a Pool with health checking capabilities.
type HealthAwarePool struct {
	Locator
	healthChecker   HealthChecker // Use interface instead of concrete type
	healthyNodes    map[string]bool
	healthEventChan chan HealthEvent
	mu              sync.RWMutex
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
		healthEventChan: make(chan HealthEvent, 100), // Buffered channel for health events
	}

	// Initialize all nodes as healthy and add them to health checker
	for _, node := range locator.GetAllNodes() {
		hap.healthyNodes[node.ID()] = true
		// Add node to health checker for monitoring
		if healthChecker != nil {
			healthChecker.AddNode(node)
		}
	}

	// Note: Health listener will be added in StartHealthMonitoring to avoid duplication
	return hap
}

// Get retrieves a healthy node for the given key.
func (hap *HealthAwarePool) Get(ctx context.Context, key string) (Node, error) {
	// Fast path: get the primary node
	node, err := hap.Locator.Get(ctx, key)
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
	replicas, err := hap.Locator.GetReplicas(ctx, key, 3)
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

// StartHealthMonitoring starts health monitoring for all nodes in the locator.
func (hap *HealthAwarePool) StartHealthMonitoring(ctx context.Context) {
	if !hap.healthChecker.IsEnabled() {
		return
	}

	// Register all nodes from the locator to the health checker
	nodes := hap.Locator.GetAllNodes()
	for _, node := range nodes {
		hap.healthChecker.AddNode(node)
	}

	// Add single health listener to update internal state and send events
	hap.healthChecker.AddHealthListener(func(nodeID string, healthy bool) {
		// Update internal state immediately
		hap.mu.Lock()
		hap.healthyNodes[nodeID] = healthy
		hap.mu.Unlock()

		// Send event to channel for advanced processing (alerts, metrics, etc.)
		select {
		case hap.healthEventChan <- HealthEvent{
			NodeID:    nodeID,
			Healthy:   healthy,
			Timestamp: time.Now(),
		}:
		default:
			// Channel is full, skip this event
			// TODO: add some log
		}
	})

	// Start the health checker's own monitoring
	hap.healthChecker.StartMonitoring(ctx)

	// Start event-driven health management
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

// processHealthEvent processes a single health event for advanced features
func (hap *HealthAwarePool) processHealthEvent(event HealthEvent) {
	// Log significant health changes (in production, use proper logging)
	if event.Healthy {
		// fmt.Printf("Node healthy: %s\n", event.NodeID)
	} else {
		// fmt.Printf("Node unhealthy: %s\n", event.NodeID)
	}

	// TODO: Advanced health event processing
	// 1. Temporarily adjust the node's weight in the consistent hash ring
	// 2. Trigger rebalancing if too many nodes are unhealthy
	// 3. Send alerts or notifications
	// 4. Update metrics and monitoring systems
	// 5. Implement circuit breaker logic
	// 6. Trigger failover procedures
}

// AddNode adds a node to both the locator and health monitoring.
func (hap *HealthAwarePool) AddNode(node Node) error {
	// Add to underlying locator
	if err := hap.Locator.AddNode(node); err != nil {
		return err
	}

	// Add to health checker for monitoring
	hap.healthChecker.AddNode(node)

	// Initialize as healthy
	hap.mu.Lock()
	hap.healthyNodes[node.ID()] = true
	hap.mu.Unlock()

	return nil
}

// RemoveNode removes a node from both the locator and health monitoring.
func (hap *HealthAwarePool) RemoveNode(nodeID string) error {
	// Remove from underlying locator
	if err := hap.Locator.RemoveNode(nodeID); err != nil {
		return err
	}

	// Remove from health checker
	hap.healthChecker.RemoveNode(nodeID)

	// Remove from health tracking
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
	return !exists || healthy // Default to healthy if unknown
}

// GetStats returns statistics about the health-aware pool including health status.
func (hap *HealthAwarePool) GetStats() HealthAwarePoolStats {
	hap.mu.RLock()
	defer hap.mu.RUnlock()

	stats := HealthAwarePoolStats{
		TotalNodes:     len(hap.healthyNodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
	}

	// Count healthy and unhealthy nodes from cached state (fast, no I/O)
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
	// Select selects the best node from the given candidates.
	Select(ctx context.Context, nodes []Node, key string) (Node, error)

	// UpdateStats updates the load statistics for a node.
	UpdateStats(nodeID string, latency time.Duration, success bool)
}

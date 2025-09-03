package gyro

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// NodeInfo contains information about a service node.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Weight   int               `json:"weight,omitempty"`
}

// ServiceDiscovery provides dynamic service discovery capabilities.
type ServiceDiscovery interface {
	// Discover discovers available service nodes.
	Discover(ctx context.Context, serviceName string) ([]NodeInfo, error)

	// Watch watches for changes in service nodes.
	Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error)

	// Register registers a service node.
	Register(ctx context.Context, serviceName string, node NodeInfo) error

	// Unregister unregisters a service node.
	Unregister(ctx context.Context, serviceName string, nodeID string) error
}

// StaticServiceDiscovery .
type StaticServiceDiscovery struct {
	mu       sync.RWMutex
	services map[string][]NodeInfo
}

// NewStaticServiceDiscovery .
func NewStaticServiceDiscovery() *StaticServiceDiscovery {
	return &StaticServiceDiscovery{
		services: make(map[string][]NodeInfo),
	}
}

// Discover discovers available service nodes.
func (ssd *StaticServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]NodeInfo, error) {
	ssd.mu.RLock()
	defer ssd.mu.RUnlock()

	nodes, exists := ssd.services[serviceName]
	if !exists {
		return []NodeInfo{}, nil
	}

	// Return a copy
	result := make([]NodeInfo, len(nodes))
	copy(result, nodes)
	return result, nil
}

// Watch watches for changes in service nodes.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	// For static discovery, just return the current nodes and close the channel
	ch := make(chan []NodeInfo, 1)

	go func() {
		defer close(ch)

		nodes, err := ssd.Discover(ctx, serviceName)
		if err != nil {
			return
		}

		select {
		case ch <- nodes:
		case <-ctx.Done():
		}
	}()

	return ch, nil
}

// Register registers a service node.
func (ssd *StaticServiceDiscovery) Register(ctx context.Context, serviceName string, node NodeInfo) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	if ssd.services[serviceName] == nil {
		ssd.services[serviceName] = make([]NodeInfo, 0)
	}

	// Check if node already exists
	for i, existing := range ssd.services[serviceName] {
		if existing.ID == node.ID {
			// Update existing node
			ssd.services[serviceName][i] = node
			return nil
		}
	}

	// Add new node
	ssd.services[serviceName] = append(ssd.services[serviceName], node)
	return nil
}

// Unregister unregisters a service node.
func (ssd *StaticServiceDiscovery) Unregister(ctx context.Context, serviceName string, nodeID string) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	nodes, exists := ssd.services[serviceName]
	if !exists {
		return fmt.Errorf("service %s not found", serviceName)
	}

	// Find and remove the node
	for i, node := range nodes {
		if node.ID == nodeID {
			// Remove node by swapping with last element and truncating
			nodes[i] = nodes[len(nodes)-1]
			ssd.services[serviceName] = nodes[:len(nodes)-1]
			return nil
		}
	}

	return fmt.Errorf("node %s not found in service %s", nodeID, serviceName)
}

// SetNodes sets all nodes for a service (replaces existing nodes).
func (ssd *StaticServiceDiscovery) SetNodes(serviceName string, nodes []NodeInfo) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	// Make a copy
	nodesCopy := make([]NodeInfo, len(nodes))
	copy(nodesCopy, nodes)
	ssd.services[serviceName] = nodesCopy
}

// ConfigManager manages dynamic configuration updates.
type ConfigManager struct {
	mu       sync.RWMutex
	config   *ClientConfig
	watchers []ConfigWatcher
}

// ConfigWatcher is called when configuration changes.
type ConfigWatcher func(oldConfig, newConfig *ClientConfig) error

// NewConfigManager creates a new configuration manager.
func NewConfigManager(config *ClientConfig) *ConfigManager {
	if config == nil {
		config = DefaultClientConfig()
	}

	return &ConfigManager{
		config:   config,
		watchers: make([]ConfigWatcher, 0),
	}
}

// GetConfig returns the current configuration.
func (cm *ConfigManager) GetConfig() *ClientConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a deep copy
	configJSON, _ := json.Marshal(cm.config)
	var configCopy ClientConfig
	json.Unmarshal(configJSON, &configCopy)
	return &configCopy
}

// UpdateConfig updates the configuration and notifies watchers.
func (cm *ConfigManager) UpdateConfig(newConfig *ClientConfig) error {
	if newConfig == nil {
		return fmt.Errorf("new config cannot be nil")
	}

	cm.mu.Lock()
	oldConfig := cm.config
	cm.config = newConfig
	watchers := make([]ConfigWatcher, len(cm.watchers))
	copy(watchers, cm.watchers)
	cm.mu.Unlock()

	// Notify all watchers
	for _, watcher := range watchers {
		if err := watcher(oldConfig, newConfig); err != nil {
			// TODO:handle this gracefully??
			return fmt.Errorf("config watcher failed: %w", err)
		}
	}

	return nil
}

// AddConfigWatcher adds a configuration change watcher.
func (cm *ConfigManager) AddConfigWatcher(watcher ConfigWatcher) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.watchers = append(cm.watchers, watcher)
}

// DynamicClient provides dynamic configuration and service discovery.
type DynamicClient struct {
	mu            sync.RWMutex
	pool          Pool
	serviceName   string
	discovery     ServiceDiscovery
	configManager *ConfigManager
	stopCh        chan struct{}
	running       bool
	nodeFactory   NodeFactory
}

// NodeFactory creates nodes from NodeInfo.
type NodeFactory interface {
	CreateNode(info NodeInfo) (Node, error)
}

// NewDynamicClient creates a new dynamic client.
func NewDynamicClient(serviceName string, discovery ServiceDiscovery, configManager *ConfigManager, nodeFactory NodeFactory) (*DynamicClient, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}
	if discovery == nil {
		return nil, fmt.Errorf("service discovery cannot be nil")
	}
	if configManager == nil {
		return nil, fmt.Errorf("config manager cannot be nil")
	}
	if nodeFactory == nil {
		return nil, fmt.Errorf("node factory cannot be nil")
	}

	dc := &DynamicClient{
		serviceName:   serviceName,
		discovery:     discovery,
		configManager: configManager,
		nodeFactory:   nodeFactory,
		stopCh:        make(chan struct{}),
	}

	// Initialize the client
	if err := dc.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize dynamic client: %w", err)
	}

	return dc, nil
}

// initializeUnsafe initializes the client with current service nodes (caller must hold lock).
func (dc *DynamicClient) initializeUnsafe() error {
	ctx := context.Background()

	// Discover initial nodes
	nodeInfos, err := dc.discovery.Discover(ctx, dc.serviceName)
	if err != nil {
		return fmt.Errorf("failed to discover initial nodes: %w", err)
	}

	// Create pool
	config := dc.configManager.GetConfig()
	pool, err := NewConsistentPool(config.Pool)
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}

	// Add nodes to pool
	for _, nodeInfo := range nodeInfos {
		node, err := dc.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := pool.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node %s to pool: %w", nodeInfo.ID, err)
		}
	}

	// Store the pool directly
	dc.pool = pool

	return nil
}

// initialize initializes the client with current service nodes.
func (dc *DynamicClient) initialize() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.initializeUnsafe()
}

// getPool returns the underlying pool
func (dc *DynamicClient) getPool() Pool {
	return dc.pool
}

// getPoolNodes returns all nodes from the pool
func (dc *DynamicClient) getPoolNodes() []Node {
	pool := dc.getPool()
	if pool == nil {
		return nil
	}
	return pool.GetAllNodes()
}

// nodeNeedsUpdate checks if a node needs to be updated based on NodeInfo changes
func (dc *DynamicClient) nodeNeedsUpdate(currentNode Node, newNodeInfo NodeInfo) bool {
	// Check if address changed
	if currentNode.Address() != newNodeInfo.Address {
		return true
	}

	// TODO: check:
	// - Metadata changes
	// - Weight changes
	// - Other configuration changes

	return false
}

// Start starts the dynamic client with service discovery and config watching.
func (dc *DynamicClient) Start(ctx context.Context) error {
	dc.mu.Lock()
	if dc.running {
		dc.mu.Unlock()
		return fmt.Errorf("dynamic client is already running")
	}
	dc.running = true
	dc.mu.Unlock()

	// Start service discovery watching
	go dc.watchServiceNodes(ctx)

	// Add config watcher
	dc.configManager.AddConfigWatcher(dc.handleConfigChange)

	return nil
}

// Stop stops the dynamic client.
func (dc *DynamicClient) Stop() error {
	dc.mu.Lock()
	if !dc.running {
		dc.mu.Unlock()
		return nil
	}
	dc.running = false
	close(dc.stopCh)
	dc.mu.Unlock()

	// Close the underlying pool
	if dc.pool != nil {
		return dc.pool.Close()
	}

	return nil
}

// GetPool returns the underlying pool for direct access.
func (dc *DynamicClient) GetPool() Pool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return dc.pool
}

// Close closes the dynamic client.
func (dc *DynamicClient) Close() error {
	return dc.Stop()
}

// watchServiceNodes watches for service node changes.
func (dc *DynamicClient) watchServiceNodes(ctx context.Context) {
	nodesCh, err := dc.discovery.Watch(ctx, dc.serviceName)
	if err != nil {
		// In a real implementation, you'd use proper logging
		fmt.Printf("Failed to watch service nodes: %v\n", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.stopCh:
			return
		case nodes, ok := <-nodesCh:
			if !ok {
				return
			}
			dc.handleServiceNodesChange(nodes)
		}
	}
}

// handleServiceNodesChange handles changes in service nodes with incremental updates.
func (dc *DynamicClient) handleServiceNodesChange(newNodeInfos []NodeInfo) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dc.pool == nil {
		// Pool not initialized yet, do full initialization
		if err := dc.initializeUnsafe(); err != nil {
			fmt.Printf("Failed to initialize pool after node changes: %v\n", err)
		}
		return
	}

	// Get current nodes from the pool
	currentNodes := dc.getPoolNodes()
	currentNodeMap := make(map[string]Node)
	for _, node := range currentNodes {
		currentNodeMap[node.ID()] = node
	}

	// Create map of new nodes for easy lookup
	newNodeMap := make(map[string]NodeInfo)
	for _, nodeInfo := range newNodeInfos {
		newNodeMap[nodeInfo.ID] = nodeInfo
	}

	// Find nodes to remove (exist in current but not in new)
	var nodesToRemove []string
	for nodeID := range currentNodeMap {
		if _, exists := newNodeMap[nodeID]; !exists {
			nodesToRemove = append(nodesToRemove, nodeID)
		}
	}

	// Find nodes to add (exist in new but not in current)
	var nodesToAdd []NodeInfo
	for nodeID, nodeInfo := range newNodeMap {
		if _, exists := currentNodeMap[nodeID]; !exists {
			nodesToAdd = append(nodesToAdd, nodeInfo)
		}
	}

	// Find nodes to update (exist in both but with different metadata)
	var nodesToUpdate []NodeInfo
	for nodeID, newNodeInfo := range newNodeMap {
		if currentNode, exists := currentNodeMap[nodeID]; exists {
			// Check if node needs update (address changed, metadata changed, etc.)
			if dc.nodeNeedsUpdate(currentNode, newNodeInfo) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}

	// Apply changes incrementally
	pool := dc.getPool()
	if pool == nil {
		fmt.Printf("Pool is nil, cannot apply incremental updates\n")
		return
	}

	// Remove nodes
	for _, nodeID := range nodesToRemove {
		if err := pool.RemoveNode(nodeID); err != nil {
			fmt.Printf("Failed to remove node %s: %v\n", nodeID, err)
		} else {
			fmt.Printf("Removed node: %s\n", nodeID)
		}
	}

	// Add new nodes
	for _, nodeInfo := range nodesToAdd {
		node, err := dc.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			fmt.Printf("Failed to create node %s: %v\n", nodeInfo.ID, err)
			continue
		}

		if err := pool.AddNode(node); err != nil {
			fmt.Printf("Failed to add node %s: %v\n", nodeInfo.ID, err)
		} else {
			fmt.Printf("Added node: %s\n", nodeInfo.ID)
		}
	}

	// Update existing nodes
	for _, nodeInfo := range nodesToUpdate {
		// remove the old node and add the new one
		if err := pool.RemoveNode(nodeInfo.ID); err != nil {
			fmt.Printf("Failed to remove node %s for update: %v\n", nodeInfo.ID, err)
			continue
		}

		node, err := dc.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			fmt.Printf("Failed to create updated node %s: %v\n", nodeInfo.ID, err)
			continue
		}

		if err := pool.AddNode(node); err != nil {
			fmt.Printf("Failed to add updated node %s: %v\n", nodeInfo.ID, err)
		} else {
			fmt.Printf("Updated node: %s\n", nodeInfo.ID)
		}
	}

	fmt.Printf("Incremental update completed: +%d -%d ~%d nodes\n",
		len(nodesToAdd), len(nodesToRemove), len(nodesToUpdate))
}

// handleConfigChange handles configuration changes with incremental updates.
func (dc *DynamicClient) handleConfigChange(oldConfig, newConfig *ClientConfig) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dc.pool == nil {
		// Pool not initialized yet, nothing to update
		return nil
	}

	// Compare configurations and apply incremental updates
	var needsPoolRecreation bool
	var healthCheckerUpdates []func() error

	// Check pool configuration changes
	if !dc.poolConfigEqual(oldConfig.Pool, newConfig.Pool) {
		// Pool configuration changed - this requires recreation
		needsPoolRecreation = true
		fmt.Printf("Pool configuration changed, will recreate pool\n")
	}

	// Check health checker configuration changes
	if !dc.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker) {
		healthCheckerUpdates = append(healthCheckerUpdates, func() error {
			return dc.updateHealthCheckerConfig(newConfig.HealthChecker)
		})
		fmt.Printf("Health checker configuration changed, will update\n")
	}

	// Check connection configuration changes
	if !dc.connectionConfigEqual(oldConfig.Connection, newConfig.Connection) {
		// TODO:Connection config changes typically require node recreation
		// For now, just log it but not implement the complex logic
		fmt.Printf("Connection configuration changed (node recreation may be needed)\n")
	}

	// Apply updates, recreate the entire client
	if needsPoolRecreation {
		fmt.Printf("Recreating client due to pool configuration changes\n")
		return dc.initializeUnsafe()
	}

	// Apply health checker updates
	for _, update := range healthCheckerUpdates {
		if err := update(); err != nil {
			fmt.Printf("Failed to update health checker configuration: %v\n", err)
			return err
		}
	}

	fmt.Printf("Configuration update completed successfully\n")
	return nil
}

// poolConfigEqual compares two pool configurations
func (dc *DynamicClient) poolConfigEqual(old, new PoolConfig) bool {
	return old.PartitionCount == new.PartitionCount &&
		old.ReplicationFactor == new.ReplicationFactor &&
		old.Load == new.Load &&
		old.HashFunction == new.HashFunction
}

// healthCheckerConfigEqual compares two health checker configurations
func (dc *DynamicClient) healthCheckerConfigEqual(old, new HealthCheckerConfig) bool {
	return old.Enabled == new.Enabled &&
		old.Interval == new.Interval &&
		old.Timeout == new.Timeout &&
		old.FailureThreshold == new.FailureThreshold &&
		old.RecoveryThreshold == new.RecoveryThreshold
}

// connectionConfigEqual compares two connection configurations
func (dc *DynamicClient) connectionConfigEqual(old, new ConnectionConfig) bool {
	return old.MaxIdleConns == new.MaxIdleConns &&
		old.MaxActiveConns == new.MaxActiveConns &&
		old.IdleTimeout == new.IdleTimeout &&
		old.ConnectTimeout == new.ConnectTimeout &&
		old.ReadTimeout == new.ReadTimeout &&
		old.WriteTimeout == new.WriteTimeout
}

// updateHealthCheckerConfig updates the health checker configuration
func (dc *DynamicClient) updateHealthCheckerConfig(newConfig HealthCheckerConfig) error {
	// Try to get the health checker from the client
	healthAwarePool := dc.getHealthAwarePool()
	if healthAwarePool != nil {
		// Update the health checker configuration
		if err := healthAwarePool.UpdateHealthCheckerConfig(newConfig); err != nil {
			return fmt.Errorf("failed to update health checker config: %w", err)
		}
		fmt.Printf("Successfully updated health checker config: enabled=%v, interval=%v, timeout=%v\n",
			newConfig.Enabled, newConfig.Interval, newConfig.Timeout)
	} else {
		// No health-aware pool available, just log
		fmt.Printf("No health-aware pool available, config update logged: enabled=%v, interval=%v, timeout=%v\n",
			newConfig.Enabled, newConfig.Interval, newConfig.Timeout)
	}

	return nil
}

// getHealthAwarePool tries to extract a HealthAwarePool from the pool
func (dc *DynamicClient) getHealthAwarePool() *HealthAwarePool {
	if dc.pool == nil {
		return nil
	}

	// Check if the pool is a HealthAwarePool
	if hap, ok := dc.pool.(*HealthAwarePool); ok {
		return hap
	}

	return nil
}

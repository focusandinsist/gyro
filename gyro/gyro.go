package gyro

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NodeInfo contains information about a service node.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Weight   int               `json:"weight,omitempty"`
}

// ServiceDiscovery provides service discovery capabilities.
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

// SetNodes sets all nodes for a service (replaces all existing nodes of a services).
func (ssd *StaticServiceDiscovery) SetNodes(serviceName string, nodes []NodeInfo) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	// Make a copy
	nodesCopy := make([]NodeInfo, len(nodes))
	copy(nodesCopy, nodes)
	ssd.services[serviceName] = nodesCopy
}

// ClientHealth represents the health status of the client
type ClientHealth struct {
	ServiceDiscoveryHealthy   bool      `json:"service_discovery_healthy"`
	LastServiceDiscoveryError string    `json:"last_service_discovery_error,omitempty"`
	ServiceDiscoveryRetries   int       `json:"service_discovery_retries"`
	LastHealthCheck           time.Time `json:"last_health_check"`
}

// Client provides configuration and service discovery.
type Client struct {
	mu            sync.RWMutex
	locator       Locator
	serviceName   string
	discovery     ServiceDiscovery
	configManager *ConfigManager
	stopCh        chan struct{}
	running       bool
	nodeFactory   NodeFactory

	// Health tracking
	healthMu                  sync.RWMutex
	serviceDiscoveryHealthy   bool
	lastServiceDiscoveryError string
	serviceDiscoveryRetries   int
}

// NodeFactory creates nodes from NodeInfo.
type NodeFactory interface {
	CreateNode(info NodeInfo) (Node, error)
}

// NewClient creates a new client.
func NewClient(serviceName string, discovery ServiceDiscovery, configManager *ConfigManager, nodeFactory NodeFactory) (*Client, error) {
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

	client := &Client{
		serviceName:   serviceName,
		discovery:     discovery,
		configManager: configManager,
		nodeFactory:   nodeFactory,
		stopCh:        make(chan struct{}),

		// Initialize health status
		serviceDiscoveryHealthy: false, // Will be set to true when watch is established
	}

	if err := client.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	return client, nil
}

// initializeUnsafe initializes the client with current service nodes (caller must hold lock).
func (c *Client) initializeUnsafe() error {
	ctx := context.Background()

	// Discover initial nodes
	nodeInfos, err := c.discovery.Discover(ctx, c.serviceName)
	if err != nil {
		return fmt.Errorf("failed to discover initial nodes: %w", err)
	}

	// Create locator
	config := c.configManager.GetConfig()
	locator, err := NewConsistentLocator(config.Locator)
	if err != nil {
		return fmt.Errorf("failed to create locator: %w", err)
	}

	// Add nodes to locator
	for _, nodeInfo := range nodeInfos {
		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := locator.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node %s to locator: %w", nodeInfo.ID, err)
		}
	}

	// Store the locator directly
	c.locator = locator

	return nil
}

// initialize initializes the client with current service nodes.
func (c *Client) initialize() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.initializeUnsafe()
}

// getLocator returns the underlying locator
func (c *Client) getLocator() Locator {
	return c.locator
}

// getPoolNodes returns all nodes from the locator
func (c *Client) getPoolNodes() []Node {
	locator := c.getLocator()
	if locator == nil {
		return nil
	}
	return locator.GetAllNodes()
}

// nodeNeedsUpdate checks if a node needs to be updated based on NodeInfo changes
func (c *Client) nodeNeedsUpdate(currentNode Node, newNodeInfo NodeInfo) bool {
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

// Start starts the client with service discovery and config watching.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("client is already running")
	}
	c.running = true
	c.mu.Unlock()

	// Start service discovery watching
	go c.watchServiceNodes(ctx)

	// Add config watcher
	c.configManager.AddConfigWatcher(c.handleConfigChange)

	return nil
}

// Stop stops the client.
func (c *Client) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	close(c.stopCh)
	c.mu.Unlock()

	// Close the underlying locator
	if c.locator != nil {
		return c.locator.Close()
	}

	return nil
}

// GetLocator returns the underlying locator for direct access.
func (c *Client) GetLocator() Locator {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.locator
}

// Close closes the client.
func (c *Client) Close() error {
	return c.Stop()
}

// Health returns the current health status of the client
func (c *Client) Health() *ClientHealth {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()

	return &ClientHealth{
		ServiceDiscoveryHealthy:   c.serviceDiscoveryHealthy,
		LastServiceDiscoveryError: c.lastServiceDiscoveryError,
		ServiceDiscoveryRetries:   c.serviceDiscoveryRetries,
		LastHealthCheck:           time.Now(),
	}
}

// IsHealthy returns true if the client is healthy
func (c *Client) IsHealthy() bool {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	return c.serviceDiscoveryHealthy
}

// updateServiceDiscoveryHealth updates the service discovery health status
func (c *Client) updateServiceDiscoveryHealth(healthy bool, err error) {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()

	c.serviceDiscoveryHealthy = healthy
	if err != nil {
		c.lastServiceDiscoveryError = err.Error()
		if !healthy {
			c.serviceDiscoveryRetries++
		}
	} else {
		c.lastServiceDiscoveryError = ""
		if healthy {
			c.serviceDiscoveryRetries = 0 // Reset retries on success
		}
	}
}

// watchServiceNodes watches for service node changes with retry mechanism.
func (c *Client) watchServiceNodes(ctx context.Context) {
	const (
		maxRetries    = 10
		baseDelay     = time.Second
		maxDelay      = time.Minute
		backoffFactor = 2.0
	)

	retryCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		// Attempt to watch service nodes
		nodesCh, err := c.discovery.Watch(ctx, c.serviceName)
		if err != nil {
			c.updateServiceDiscoveryHealth(false, err)
			fmt.Printf("Failed to watch service nodes (attempt %d/%d): %v\n", retryCount+1, maxRetries, err)

			retryCount++
			if retryCount >= maxRetries {
				fmt.Printf("Max retries reached for service discovery, giving up\n")
				return
			}

			// Exponential backoff
			multiplier := 1
			for i := 0; i < retryCount; i++ {
				multiplier *= 2
			}
			delay := time.Duration(int64(baseDelay) * int64(multiplier))
			if delay > maxDelay {
				delay = maxDelay
			}

			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-time.After(delay):
				continue
			}
		}

		// Successfully established watch, reset retry count and mark healthy
		retryCount = 0
		c.updateServiceDiscoveryHealth(true, nil)
		fmt.Printf("Successfully established service discovery watch\n")

		// Process events from the watch channel
		watchFailed := c.processServiceWatch(ctx, nodesCh)
		if !watchFailed {
			// Normal shutdown, don't retry
			return
		}

		// Watch failed, will retry after backoff
		c.updateServiceDiscoveryHealth(false, fmt.Errorf("service discovery watch channel closed unexpectedly"))
		fmt.Printf("Service discovery watch failed, will retry...\n")
	}
}

// processServiceWatch processes events from the service discovery watch channel
// Returns true if the watch failed and should be retried, false for normal shutdown
func (c *Client) processServiceWatch(ctx context.Context, nodesCh <-chan []NodeInfo) bool {
	for {
		select {
		case <-ctx.Done():
			return false // Normal shutdown
		case <-c.stopCh:
			return false // Normal shutdown
		case nodes, ok := <-nodesCh:
			if !ok {
				return true // Channel closed, should retry
			}
			c.handleServiceNodesChange(nodes)
		}
	}
}

// handleServiceNodesChange handles changes in service nodes with incremental updates.
func (c *Client) handleServiceNodesChange(newNodeInfos []NodeInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.locator == nil {
		// Pool not initialized yet, do full initialization
		if err := c.initializeUnsafe(); err != nil {
			fmt.Printf("Failed to initialize locator after node changes: %v\n", err)
		}
		return
	}

	// Get current nodes from the locator
	currentNodes := c.getPoolNodes()
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
			if c.nodeNeedsUpdate(currentNode, newNodeInfo) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}

	// Apply changes incrementally
	locator := c.getLocator()
	if locator == nil {
		fmt.Printf("Pool is nil, cannot apply incremental updates\n")
		return
	}

	// Remove nodes
	for _, nodeID := range nodesToRemove {
		if err := locator.RemoveNode(nodeID); err != nil {
			fmt.Printf("Failed to remove node %s: %v\n", nodeID, err)
		} else {
			fmt.Printf("Removed node: %s\n", nodeID)
		}
	}

	// Add new nodes
	for _, nodeInfo := range nodesToAdd {
		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			fmt.Printf("Failed to create node %s: %v\n", nodeInfo.ID, err)
			continue
		}

		if err := locator.AddNode(node); err != nil {
			fmt.Printf("Failed to add node %s: %v\n", nodeInfo.ID, err)
		} else {
			fmt.Printf("Added node: %s\n", nodeInfo.ID)
		}
	}

	// Update existing nodes
	for _, nodeInfo := range nodesToUpdate {
		// remove the old node and add the new one
		if err := locator.RemoveNode(nodeInfo.ID); err != nil {
			fmt.Printf("Failed to remove node %s for update: %v\n", nodeInfo.ID, err)
			continue
		}

		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			fmt.Printf("Failed to create updated node %s: %v\n", nodeInfo.ID, err)
			continue
		}

		if err := locator.AddNode(node); err != nil {
			fmt.Printf("Failed to add updated node %s: %v\n", nodeInfo.ID, err)
		} else {
			fmt.Printf("Updated node: %s\n", nodeInfo.ID)
		}
	}

	fmt.Printf("Incremental update completed: +%d -%d ~%d nodes\n",
		len(nodesToAdd), len(nodesToRemove), len(nodesToUpdate))
}

// handleConfigChange handles configuration changes with incremental updates.
func (c *Client) handleConfigChange(oldConfig, newConfig *ClientConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.locator == nil {
		// Pool not initialized yet, nothing to update
		return nil
	}

	// Compare configurations and apply incremental updates
	var needsPoolRecreation bool
	var healthCheckerUpdates []func() error

	// Check locator configuration changes
	if !c.locatorConfigEqual(oldConfig.Locator, newConfig.Locator) {
		// Pool configuration changed - this requires recreation
		needsPoolRecreation = true
		fmt.Printf("Pool configuration changed, will recreate locator\n")
	}

	// Check health checker configuration changes
	if !c.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker) {
		healthCheckerUpdates = append(healthCheckerUpdates, func() error {
			return c.updateHealthCheckerConfig(newConfig.HealthChecker)
		})
		fmt.Printf("Health checker configuration changed, will update\n")
	}

	// Check connection configuration changes
	if !c.connectionConfigEqual(oldConfig.Connection, newConfig.Connection) {
		// TODO:Connection config changes typically require node recreation
		// For now, just log it but not implement the complex logic
		fmt.Printf("Connection configuration changed (node recreation may be needed)\n")
	}

	// Apply updates, recreate the entire client
	if needsPoolRecreation {
		fmt.Printf("Recreating client due to locator configuration changes\n")
		return c.initializeUnsafe()
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

// locatorConfigEqual compares two locator configurations
func (c *Client) locatorConfigEqual(old, new LocatorConfig) bool {
	return old.PartitionCount == new.PartitionCount &&
		old.ReplicationFactor == new.ReplicationFactor &&
		old.Load == new.Load &&
		old.HashFunction == new.HashFunction
}

// healthCheckerConfigEqual compares two health checker configurations
func (c *Client) healthCheckerConfigEqual(old, new HealthCheckerConfig) bool {
	return old.Enabled == new.Enabled &&
		old.Interval == new.Interval &&
		old.Timeout == new.Timeout &&
		old.FailureThreshold == new.FailureThreshold &&
		old.RecoveryThreshold == new.RecoveryThreshold
}

// connectionConfigEqual compares two connection configurations
func (c *Client) connectionConfigEqual(old, new ConnectionConfig) bool {
	return old.MaxIdleConns == new.MaxIdleConns &&
		old.MaxActiveConns == new.MaxActiveConns &&
		old.IdleTimeout == new.IdleTimeout &&
		old.ConnectTimeout == new.ConnectTimeout &&
		old.ReadTimeout == new.ReadTimeout &&
		old.WriteTimeout == new.WriteTimeout
}

// updateHealthCheckerConfig updates the health checker configuration
func (c *Client) updateHealthCheckerConfig(newConfig HealthCheckerConfig) error {
	// Try to get the health checker from the client
	healthAwarePool := c.getHealthAwarePool()
	if healthAwarePool != nil {
		// Update the health checker configuration
		if err := healthAwarePool.UpdateHealthCheckerConfig(newConfig); err != nil {
			return fmt.Errorf("failed to update health checker config: %w", err)
		}
		fmt.Printf("Successfully updated health checker config: enabled=%v, interval=%v, timeout=%v\n",
			newConfig.Enabled, newConfig.Interval, newConfig.Timeout)
	} else {
		// No health-aware locator available, just log
		fmt.Printf("No health-aware locator available, config update logged: enabled=%v, interval=%v, timeout=%v\n",
			newConfig.Enabled, newConfig.Interval, newConfig.Timeout)
	}

	return nil
}

// getHealthAwarePool tries to extract a HealthAwarePool from the locator
func (c *Client) getHealthAwarePool() *HealthAwarePool {
	if c.locator == nil {
		return nil
	}

	// Check if the locator is a HealthAwarePool
	if hap, ok := c.locator.(*HealthAwarePool); ok {
		return hap
	}

	return nil
}

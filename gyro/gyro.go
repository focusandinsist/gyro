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
	watchers map[string][]chan []NodeInfo
}

// NewStaticServiceDiscovery creates a new static service discovery.
func NewStaticServiceDiscovery(addresses []string) *StaticServiceDiscovery {
	ssd := &StaticServiceDiscovery{
		services: make(map[string][]NodeInfo),
		watchers: make(map[string][]chan []NodeInfo),
	}

	// Convert addresses to NodeInfo
	if len(addresses) > 0 {
		nodes := make([]NodeInfo, len(addresses))
		for i, addr := range addresses {
			nodes[i] = NodeInfo{
				ID:      fmt.Sprintf("node-%d", i+1),
				Address: addr,
			}
		}
		// Register nodes for a default service name (will be overridden by actual service name)
		ssd.services["default"] = nodes
	}

	return ssd
}

// UpdateNodes updates the node list for a service.
func (ssd *StaticServiceDiscovery) UpdateNodes(serviceName string, addresses []string) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	// Convert addresses to NodeInfo
	nodes := make([]NodeInfo, len(addresses))
	for i, addr := range addresses {
		nodes[i] = NodeInfo{
			ID:      fmt.Sprintf("node-%d", i+1),
			Address: addr,
		}
	}

	ssd.services[serviceName] = nodes

	// Also update default if this is the first service
	if serviceName != "default" && len(ssd.services) == 1 {
		ssd.services["default"] = nodes
	}

	// Notify all watchers of this service
	if watchers, exists := ssd.watchers[serviceName]; exists {
		for _, ch := range watchers {
			select {
			case ch <- nodes:
			default:
				// Channel is full or closed, skip
			}
		}
	}
}

// Discover discovers available service nodes.
func (ssd *StaticServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]NodeInfo, error) {
	ssd.mu.RLock()
	defer ssd.mu.RUnlock()

	nodes, exists := ssd.services[serviceName]
	if !exists {
		// Try to use default nodes if service-specific nodes don't exist
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			// Copy default nodes and update service name
			result := make([]NodeInfo, len(defaultNodes))
			copy(result, defaultNodes)
			// Register these nodes for the requested service
			ssd.mu.RUnlock()
			ssd.mu.Lock()
			ssd.services[serviceName] = result
			ssd.mu.Unlock()
			ssd.mu.RLock()
			return result, nil
		}
		return []NodeInfo{}, nil
	}

	// Return a copy
	result := make([]NodeInfo, len(nodes))
	copy(result, nodes)
	return result, nil
}

// Watch watches for changes in service nodes.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	ch := make(chan []NodeInfo, 10) // Buffered channel for updates

	ssd.mu.Lock()
	// Register this watcher
	if ssd.watchers[serviceName] == nil {
		ssd.watchers[serviceName] = make([]chan []NodeInfo, 0)
	}
	ssd.watchers[serviceName] = append(ssd.watchers[serviceName], ch)
	ssd.mu.Unlock()

	go func() {
		defer func() {
			// Unregister this watcher when done
			ssd.mu.Lock()
			if watchers, exists := ssd.watchers[serviceName]; exists {
				for i, watcher := range watchers {
					if watcher == ch {
						ssd.watchers[serviceName] = append(watchers[:i], watchers[i+1:]...)
						break
					}
				}
			}
			ssd.mu.Unlock()
			close(ch)
		}()

		// Send initial nodes
		nodes, err := ssd.Discover(ctx, serviceName)
		if err != nil {
			return
		}

		select {
		case ch <- nodes:
		case <-ctx.Done():
			return
		}

		// Keep the channel open until context is cancelled
		// Updates will be sent via UpdateNodes
		<-ctx.Done()
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
	healthChecker HealthChecker
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

// NewClient creates a new client with dependency injection.
func NewClient(serviceName string, discovery ServiceDiscovery, configManager *ConfigManager, nodeFactory NodeFactory, healthChecker HealthChecker) (*Client, error) {
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
	if healthChecker == nil {
		return nil, fmt.Errorf("health checker cannot be nil")
	}

	client := &Client{
		serviceName:   serviceName,
		discovery:     discovery,
		configManager: configManager,
		nodeFactory:   nodeFactory,
		healthChecker: healthChecker,
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

	// Create base locator
	config := c.configManager.GetConfig()
	baseLocator, err := NewConsistentLocator(config.Locator)
	if err != nil {
		return fmt.Errorf("failed to create locator: %w", err)
	}

	// Add nodes to base locator
	for _, nodeInfo := range nodeInfos {
		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := baseLocator.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node %s to locator: %w", nodeInfo.ID, err)
		}
	}

	// Wrap with HealthAwarePool using injected HealthChecker
	healthAwarePool := NewHealthAwarePoolWithChecker(baseLocator, c.healthChecker)

	// Store the health-aware locator
	c.locator = healthAwarePool

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

	// Start health monitoring in a separate goroutine
	// This will wait for the locator to be initialized
	go c.startHealthMonitoringWhenReady(ctx)

	// Add config watcher
	c.configManager.AddConfigWatcher(c.handleConfigChange)

	return nil
}

// startHealthMonitoringWhenReady waits for the locator to be initialized and then starts health monitoring
func (c *Client) startHealthMonitoringWhenReady(ctx context.Context) {
	// Poll until locator is ready
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			locator := c.locator
			c.mu.RUnlock()

			if locator != nil {
				// Locator is ready, start health monitoring
				if healthAwarePool, ok := locator.(*HealthAwarePool); ok {
					healthAwarePool.StartHealthMonitoring(ctx)
				}
				return
			}
		}
	}
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
			// Also remove from health checker
			if c.healthChecker != nil {
				c.healthChecker.RemoveNode(nodeID)
			}
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
			// Also add to health checker
			if c.healthChecker != nil {
				c.healthChecker.AddNode(node)
			}
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

		// Also remove from health checker
		if c.healthChecker != nil {
			c.healthChecker.RemoveNode(nodeInfo.ID)
		}

		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			fmt.Printf("Failed to create updated node %s: %v\n", nodeInfo.ID, err)
			continue
		}

		if err := locator.AddNode(node); err != nil {
			fmt.Printf("Failed to add updated node %s: %v\n", nodeInfo.ID, err)
		} else {
			// Also add to health checker
			if c.healthChecker != nil {
				c.healthChecker.AddNode(node)
			}
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

// GetStats returns client statistics.
func (c *Client) GetStats() HealthAwarePoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.locator == nil {
		return HealthAwarePoolStats{}
	}

	// Count healthy/unhealthy nodes
	allNodes := c.locator.GetAllNodes()
	totalNodes := len(allNodes)
	healthyCount := 0
	for _, node := range allNodes {
		if c.healthChecker.IsNodeHealthy(node.ID()) {
			healthyCount++
		}
	}

	return HealthAwarePoolStats{
		TotalNodes:     totalNodes,
		HealthyNodes:   healthyCount,
		UnhealthyNodes: totalNodes - healthyCount,
	}
}

// GetClientForKey returns a client for the given key.
func (c *Client) GetClientForKey(ctx context.Context, key string) (interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.locator == nil {
		return nil, fmt.Errorf("client not started")
	}

	node, err := c.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for key %s: %w", key, err)
	}

	// Return the node itself for now
	// In a real implementation, this would return a protocol-specific client
	return node, nil
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
	// Directly update the injected health checker
	if err := c.healthChecker.UpdateConfig(newConfig); err != nil {
		return fmt.Errorf("failed to update health checker config: %w", err)
	}

	fmt.Printf("Successfully updated health checker config: enabled=%v, interval=%v, timeout=%v\n",
		newConfig.Enabled, newConfig.Interval, newConfig.Timeout)

	return nil
}

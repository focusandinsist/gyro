package gyro

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// discardLogger is the default logger for internal components: it never
// produces output, so a library consumer that doesn't call SetLogger sees
// nothing on stdout/stderr. Built manually (rather than via slog.DiscardHandler,
// added in Go 1.24) to stay compatible with the go.mod minimum version.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

// NodeInfo contains information about a service node.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Weight   int               `json:"weight,omitempty"`
}

// ServiceDiscovery provides service discovery capabilities.
type ServiceDiscovery interface {
	Discover(ctx context.Context, serviceName string) ([]NodeInfo, error)
	Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error)
	Register(ctx context.Context, serviceName string, node NodeInfo) error
	Unregister(ctx context.Context, serviceName string, nodeID string) error
}

// StaticServiceDiscovery is a fixed, in-memory ServiceDiscovery backed by an
// address list, useful for tests or clusters that don't change at runtime.
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

	if len(addresses) > 0 {
		nodes := make([]NodeInfo, len(addresses))
		for i, addr := range addresses {
			nodes[i] = NodeInfo{
				ID:      fmt.Sprintf("node-%d", i+1),
				Address: addr,
			}
		}
		// Stored under "default" so Discover can serve any service name
		// passed to NewClient without requiring a matching SetNodes call.
		ssd.services["default"] = nodes
	}

	return ssd
}

// UpdateNodes updates the node list for a service.
func (ssd *StaticServiceDiscovery) UpdateNodes(serviceName string, addresses []string) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	nodes := make([]NodeInfo, len(addresses))
	for i, addr := range addresses {
		nodes[i] = NodeInfo{
			ID:      fmt.Sprintf("node-%d", i+1),
			Address: addr,
		}
	}

	ssd.services[serviceName] = nodes

	if serviceName != "default" && len(ssd.services) == 1 {
		ssd.services["default"] = nodes
	}

	if watchers, exists := ssd.watchers[serviceName]; exists {
		for _, ch := range watchers {
			select {
			case ch <- nodes:
			default: // watcher isn't ready for this update, drop it rather than block
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
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			result := make([]NodeInfo, len(defaultNodes))
			copy(result, defaultNodes)

			ssd.mu.RUnlock()
			ssd.mu.Lock()
			ssd.services[serviceName] = result
			ssd.mu.Unlock()
			ssd.mu.RLock()
			return result, nil
		}
		return []NodeInfo{}, nil
	}

	result := make([]NodeInfo, len(nodes))
	copy(result, nodes)
	return result, nil
}

// Watch watches for changes in service nodes.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	ch := make(chan []NodeInfo, 10)

	ssd.mu.Lock()
	if ssd.watchers[serviceName] == nil {
		ssd.watchers[serviceName] = make([]chan []NodeInfo, 0)
	}
	ssd.watchers[serviceName] = append(ssd.watchers[serviceName], ch)
	ssd.mu.Unlock()

	go func() {
		defer func() {
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

		nodes, err := ssd.Discover(ctx, serviceName)
		if err != nil {
			return
		}

		select {
		case ch <- nodes:
		case <-ctx.Done():
			return
		}

		// Nothing to poll for; further updates arrive via UpdateNodes.
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

	for i, existing := range ssd.services[serviceName] {
		if existing.ID == node.ID {
			ssd.services[serviceName][i] = node
			return nil
		}
	}

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

	for i, node := range nodes {
		if node.ID == nodeID {
			// Order doesn't matter here, so swap-and-truncate instead of shifting.
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
	logger        atomic.Pointer[slog.Logger]

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

		// False until watchServiceNodes establishes its first watch.
		serviceDiscoveryHealthy: false,
	}
	client.logger.Store(discardLogger)

	if err := client.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	return client, nil
}

// SetLogger overrides the logger used for internal diagnostics (node churn,
// service discovery retries, config reloads). Passing nil restores the
// default no-op logger. Call this before Start so the logger also reaches
// components created during initialization (e.g. the internal locator).
func (c *Client) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardLogger
	}
	c.logger.Store(logger)
}

func (c *Client) log() *slog.Logger {
	return c.logger.Load()
}

// initializeUnsafe initializes the client with current service nodes (caller must hold lock).
func (c *Client) initializeUnsafe() error {
	ctx := context.Background()

	nodeInfos, err := c.discovery.Discover(ctx, c.serviceName)
	if err != nil {
		return fmt.Errorf("failed to discover initial nodes: %w", err)
	}

	config := c.configManager.GetConfig()
	baseLocator, err := NewConsistentLocator(config.Locator)
	if err != nil {
		return fmt.Errorf("failed to create locator: %w", err)
	}
	baseLocator.SetLogger(c.log())

	for _, nodeInfo := range nodeInfos {
		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := baseLocator.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node %s to locator: %w", nodeInfo.ID, err)
		}
	}

	healthAwarePool := NewHealthAwarePoolWithChecker(baseLocator, c.healthChecker)
	healthAwarePool.SetLogger(c.log())
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
	if currentNode.Address() != newNodeInfo.Address {
		return true
	}

	// TODO: metadata changes, weight changes, other config changes.

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

	go c.watchServiceNodes(ctx)
	go c.startHealthMonitoringWhenReady(ctx)
	c.configManager.AddConfigWatcher(c.handleConfigChange)

	return nil
}

// startHealthMonitoringWhenReady waits for the locator to be initialized and then starts health monitoring
func (c *Client) startHealthMonitoringWhenReady(ctx context.Context) {
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
			c.serviceDiscoveryRetries = 0
		}
	}
}

// watchServiceNodes watches for service node changes with retry mechanism.
func (c *Client) watchServiceNodes(ctx context.Context) {
	const (
		maxRetries = 10
		baseDelay  = time.Second
		maxDelay   = time.Minute
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

		nodesCh, err := c.discovery.Watch(ctx, c.serviceName)
		if err != nil {
			c.updateServiceDiscoveryHealth(false, err)
			c.log().Warn("service discovery watch failed", "attempt", retryCount+1, "max_retries", maxRetries, "error", err)

			retryCount++
			if retryCount >= maxRetries {
				c.log().Error("service discovery: giving up after max retries")
				return
			}

			// Exponential backoff.
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

		retryCount = 0
		c.updateServiceDiscoveryHealth(true, nil)
		c.log().Info("service discovery watch established")

		watchFailed := c.processServiceWatch(ctx, nodesCh)
		if !watchFailed {
			return
		}

		c.updateServiceDiscoveryHealth(false, fmt.Errorf("service discovery watch channel closed unexpectedly"))
		c.log().Warn("service discovery watch failed, retrying")
	}
}

// processServiceWatch processes events from the service discovery watch channel
// Returns true if the watch failed and should be retried, false for normal shutdown
func (c *Client) processServiceWatch(ctx context.Context, nodesCh <-chan []NodeInfo) bool {
	for {
		select {
		case <-ctx.Done():
			return false // normal shutdown
		case <-c.stopCh:
			return false // normal shutdown
		case nodes, ok := <-nodesCh:
			if !ok {
				return true // channel closed, caller should retry
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
		if err := c.initializeUnsafe(); err != nil {
			c.log().Error("failed to initialize locator after node change", "error", err)
		}
		return
	}

	currentNodes := c.getPoolNodes()
	currentNodeMap := make(map[string]Node)
	for _, node := range currentNodes {
		currentNodeMap[node.ID()] = node
	}

	newNodeMap := make(map[string]NodeInfo)
	for _, nodeInfo := range newNodeInfos {
		newNodeMap[nodeInfo.ID] = nodeInfo
	}

	var nodesToRemove []string
	for nodeID := range currentNodeMap {
		if _, exists := newNodeMap[nodeID]; !exists {
			nodesToRemove = append(nodesToRemove, nodeID)
		}
	}

	var nodesToAdd []NodeInfo
	for nodeID, nodeInfo := range newNodeMap {
		if _, exists := currentNodeMap[nodeID]; !exists {
			nodesToAdd = append(nodesToAdd, nodeInfo)
		}
	}

	var nodesToUpdate []NodeInfo
	for nodeID, newNodeInfo := range newNodeMap {
		if currentNode, exists := currentNodeMap[nodeID]; exists {
			if c.nodeNeedsUpdate(currentNode, newNodeInfo) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}

	locator := c.getLocator()
	if locator == nil {
		c.log().Error("locator is nil, cannot apply incremental update")
		return
	}

	for _, nodeID := range nodesToRemove {
		if err := locator.RemoveNode(nodeID); err != nil {
			c.log().Error("failed to remove node", "node_id", nodeID, "error", err)
		} else {
			if c.healthChecker != nil {
				c.healthChecker.RemoveNode(nodeID)
			}
			c.log().Info("node removed", "node_id", nodeID)
		}
	}

	for _, nodeInfo := range nodesToAdd {
		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNode(node); err != nil {
			c.log().Error("failed to add node", "node_id", nodeInfo.ID, "error", err)
		} else {
			if c.healthChecker != nil {
				c.healthChecker.AddNode(node)
			}
			c.log().Info("node added", "node_id", nodeInfo.ID)
		}
	}

	for _, nodeInfo := range nodesToUpdate {
		if err := locator.RemoveNode(nodeInfo.ID); err != nil {
			c.log().Error("failed to remove node for update", "node_id", nodeInfo.ID, "error", err)
			continue
		}
		if c.healthChecker != nil {
			c.healthChecker.RemoveNode(nodeInfo.ID)
		}

		node, err := c.nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create updated node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNode(node); err != nil {
			c.log().Error("failed to add updated node", "node_id", nodeInfo.ID, "error", err)
		} else {
			if c.healthChecker != nil {
				c.healthChecker.AddNode(node)
			}
			c.log().Info("node updated", "node_id", nodeInfo.ID)
		}
	}

	c.log().Info("incremental update completed",
		"added", len(nodesToAdd), "removed", len(nodesToRemove), "updated", len(nodesToUpdate))
}

// handleConfigChange handles configuration changes with incremental updates.
func (c *Client) handleConfigChange(oldConfig, newConfig *ClientConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.locator == nil {
		return nil
	}

	var needsPoolRecreation bool
	var healthCheckerUpdates []func() error

	if !c.locatorConfigEqual(oldConfig.Locator, newConfig.Locator) {
		needsPoolRecreation = true
		c.log().Info("locator config changed, recreating locator")
	}

	if !c.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker) {
		healthCheckerUpdates = append(healthCheckerUpdates, func() error {
			return c.updateHealthCheckerConfig(newConfig.HealthChecker)
		})
		c.log().Info("health checker config changed")
	}

	if !c.connectionConfigEqual(oldConfig.Connection, newConfig.Connection) {
		// TODO: connection config changes need node recreation; not implemented yet.
		c.log().Warn("connection config changed, node recreation not implemented")
	}

	if needsPoolRecreation {
		return c.initializeUnsafe()
	}

	for _, update := range healthCheckerUpdates {
		if err := update(); err != nil {
			c.log().Error("failed to update health checker config", "error", err)
			return err
		}
	}

	c.log().Info("config update completed")
	return nil
}

// GetStats returns client statistics.
func (c *Client) GetStats() HealthAwarePoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.locator == nil {
		return HealthAwarePoolStats{}
	}

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

// nativeClientProvider is implemented by protocol adapters (e.g. RedisNode,
// GRPCNode) that can hand back their underlying native client.
type nativeClientProvider interface {
	GetNativeClient() interface{}
}

// GetClientForKey returns the native protocol client (e.g. *redis.Client,
// *grpc.ClientConn) for the node that owns the given key. If the configured
// NodeFactory produces nodes that don't implement nativeClientProvider, the
// Node itself is returned instead.
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

	provider, ok := node.(nativeClientProvider)
	if !ok {
		return node, nil
	}

	native := provider.GetNativeClient()
	if native == nil {
		return nil, fmt.Errorf("node %s has no healthy native client", node.ID())
	}

	return native, nil
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
	if err := c.healthChecker.UpdateConfig(newConfig); err != nil {
		return fmt.Errorf("failed to update health checker config: %w", err)
	}

	c.log().Info("health checker config updated",
		"enabled", newConfig.Enabled, "interval", newConfig.Interval, "timeout", newConfig.Timeout)

	return nil
}

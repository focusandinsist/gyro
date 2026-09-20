package gyro

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
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
		nodes := nodeInfosFromAddresses(addresses)
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

	nodes := nodeInfosFromAddresses(addresses)

	ssd.services[serviceName] = nodes
	ssd.publishLocked(serviceName)

	if serviceName != "default" && len(ssd.services) == 1 {
		ssd.services["default"] = cloneNodeInfos(nodes)
		ssd.publishLocked("default")
	}
}

// Discover discovers available service nodes.
func (ssd *StaticServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]NodeInfo, error) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	nodes, exists := ssd.services[serviceName]
	if !exists {
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			result := cloneNodeInfos(defaultNodes)
			ssd.services[serviceName] = cloneNodeInfos(result)
			return result, nil
		}
		return []NodeInfo{}, nil
	}

	return cloneNodeInfos(nodes), nil
}

// Watch watches for changes in service nodes.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	ch := make(chan []NodeInfo, 1)

	ssd.mu.Lock()
	if ssd.watchers[serviceName] == nil {
		ssd.watchers[serviceName] = make([]chan []NodeInfo, 0)
	}
	ssd.watchers[serviceName] = append(ssd.watchers[serviceName], ch)
	nodes, exists := ssd.services[serviceName]
	if !exists {
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			nodes = cloneNodeInfos(defaultNodes)
			ssd.services[serviceName] = nodes
		}
	}
	ch <- cloneNodeInfos(nodes)
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
			ssd.services[serviceName][i] = cloneNodeInfo(node)
			ssd.publishLocked(serviceName)
			return nil
		}
	}

	ssd.services[serviceName] = append(ssd.services[serviceName], cloneNodeInfo(node))
	ssd.publishLocked(serviceName)
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
			ssd.publishLocked(serviceName)
			return nil
		}
	}

	return fmt.Errorf("node %s not found in service %s", nodeID, serviceName)
}

// SetNodes sets all nodes for a service (replaces all existing nodes of a services).
func (ssd *StaticServiceDiscovery) SetNodes(serviceName string, nodes []NodeInfo) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()

	ssd.services[serviceName] = cloneNodeInfos(nodes)
	ssd.publishLocked(serviceName)
}

func (ssd *StaticServiceDiscovery) publishLocked(serviceName string) {
	nodes := ssd.services[serviceName]
	for _, watcher := range ssd.watchers[serviceName] {
		snapshot := cloneNodeInfos(nodes)
		select {
		case watcher <- snapshot:
		default:
			// A watcher needs state, not every intermediate mutation. Replace its
			// pending snapshot so it eventually observes the latest topology.
			select {
			case <-watcher:
			default:
			}
			watcher <- snapshot
		}
	}
}

func cloneNodeInfos(nodes []NodeInfo) []NodeInfo {
	result := make([]NodeInfo, len(nodes))
	for i, node := range nodes {
		result[i] = cloneNodeInfo(node)
	}
	return result
}

func cloneNodeInfo(node NodeInfo) NodeInfo {
	result := node
	if node.Metadata != nil {
		result.Metadata = make(map[string]string, len(node.Metadata))
		for key, value := range node.Metadata {
			result.Metadata[key] = value
		}
	}
	return result
}

func nodeInfosFromAddresses(addresses []string) []NodeInfo {
	nodes := make([]NodeInfo, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		nodes = append(nodes, NodeInfo{ID: address, Address: address})
	}
	return nodes
}

func sortNodeInfosByID(nodes []NodeInfo) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
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
	lifecycleMu   sync.Mutex
	locator       Locator
	healthChecker HealthChecker
	serviceName   string
	discovery     ServiceDiscovery
	configManager *ConfigManager
	running       bool
	runCancel     context.CancelFunc
	configWatcher bool
	nodeFactory   NodeFactory
	nodeInfos     map[string]NodeInfo
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

// ConnectionConfigurableNodeFactory creates a node factory for a new
// connection configuration without mutating the factory used by the current
// runtime. Client configuration reloads use this capability to build a new
// locator before swapping it into the running client.
type ConnectionConfigurableNodeFactory interface {
	NodeFactory
	WithConnectionConfig(config ConnectionConfig) (NodeFactory, error)
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
		nodeInfos:     make(map[string]NodeInfo),

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

// buildLocatorUnsafe builds a locator from a configuration snapshot (caller
// must hold c.mu). It does not publish the locator to the client, so callers
// can validate the complete replacement before changing live state.
func (c *Client) buildLocatorUnsafe(config *ClientConfig, nodeFactory NodeFactory) (Locator, error) {
	ctx := context.Background()

	nodeInfos, err := c.discovery.Discover(ctx, c.serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to discover initial nodes: %w", err)
	}
	nodeInfos = cloneNodeInfos(nodeInfos)
	sortNodeInfosByID(nodeInfos)

	baseLocator, err := NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create locator: %w", err)
	}
	baseLocator.SetLogger(c.log())
	committed := false
	defer func() {
		if !committed {
			_ = baseLocator.Close()
		}
	}()

	for _, nodeInfo := range nodeInfos {
		node, err := nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := baseLocator.AddNode(node); err != nil {
			_ = node.Close()
			return nil, fmt.Errorf("failed to add node %s to locator: %w", nodeInfo.ID, err)
		}
	}
	committed = true
	if c.locator == nil {
		c.nodeInfos = make(map[string]NodeInfo)
		for _, nodeInfo := range nodeInfos {
			c.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
		}
	}
	return baseLocator, nil
}

// initializeUnsafe initializes the client with current service nodes (caller
// must hold c.mu).
func (c *Client) initializeUnsafe() error {
	config := c.configManager.GetConfig()
	baseLocator, err := c.buildLocatorUnsafe(config, c.nodeFactory)
	if err != nil {
		return err
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
	oldNodeInfo, exists := c.nodeInfos[newNodeInfo.ID]
	return !exists || oldNodeInfo.Weight != newNodeInfo.Weight || !stringMapEqual(oldNodeInfo.Metadata, newNodeInfo.Metadata)
}

func stringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

// Start starts the client with service discovery and config watching.
func (c *Client) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("client is already running")
	}

	// Stop releases the locator so a later Start can create a fresh run with
	// new node connections and a new HealthAwarePool.
	if c.locator == nil {
		if err := c.initializeUnsafe(); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("failed to initialize client for start: %w", err)
		}
	}

	runCtx, runCancel := context.WithCancel(ctx)
	c.runCancel = runCancel
	c.running = true
	registerConfigWatcher := !c.configWatcher
	c.configWatcher = true
	c.mu.Unlock()

	if registerConfigWatcher {
		c.configManager.AddConfigWatcher(c.handleConfigChange)
	}

	go c.watchServiceNodes(runCtx)
	go c.startHealthMonitoringWhenReady(runCtx)

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
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	runCancel := c.runCancel
	locator := c.locator
	c.runCancel = nil
	c.running = false
	c.locator = nil
	c.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}

	if locator != nil {
		return locator.Close()
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
	if c.nodeInfos == nil {
		c.nodeInfos = make(map[string]NodeInfo)
	}

	if !c.running {
		return
	}

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
	sort.Strings(nodesToRemove)

	var nodesToAdd []NodeInfo
	for nodeID, nodeInfo := range newNodeMap {
		if _, exists := currentNodeMap[nodeID]; !exists {
			nodesToAdd = append(nodesToAdd, nodeInfo)
		}
	}
	sortNodeInfosByID(nodesToAdd)

	var nodesToUpdate []NodeInfo
	for nodeID, newNodeInfo := range newNodeMap {
		if currentNode, exists := currentNodeMap[nodeID]; exists {
			if c.nodeNeedsUpdate(currentNode, newNodeInfo) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}
	sortNodeInfosByID(nodesToUpdate)

	locator := c.getLocator()
	if locator == nil {
		c.log().Error("locator is nil, cannot apply incremental update")
		return
	}

	for _, nodeID := range nodesToRemove {
		if err := locator.RemoveNode(nodeID); err != nil {
			c.log().Error("failed to remove node", "node_id", nodeID, "error", err)
		} else {
			delete(c.nodeInfos, nodeID)
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
			c.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
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
			c.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
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
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running || c.locator == nil {
		return nil
	}

	locatorChanged := !c.locatorConfigEqual(oldConfig.Locator, newConfig.Locator)
	connectionChanged := !c.connectionConfigEqual(oldConfig.Connection, newConfig.Connection)
	healthCheckerChanged := !c.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker)

	var (
		replacementLocator Locator
		replacementFactory NodeFactory
	)

	if locatorChanged || connectionChanged {
		replacementFactory = c.nodeFactory
		if connectionChanged {
			configurableFactory, ok := c.nodeFactory.(ConnectionConfigurableNodeFactory)
			if !ok {
				return fmt.Errorf("node factory does not support connection configuration updates")
			}

			var err error
			replacementFactory, err = configurableFactory.WithConnectionConfig(newConfig.Connection)
			if err != nil {
				return fmt.Errorf("failed to prepare node factory for connection config update: %w", err)
			}
		}

		var err error
		replacementLocator, err = c.buildLocatorUnsafe(newConfig, replacementFactory)
		if err != nil {
			return fmt.Errorf("failed to build replacement locator: %w", err)
		}
	}

	if healthCheckerChanged {
		if err := c.updateHealthCheckerConfig(newConfig.HealthChecker); err != nil {
			if replacementLocator != nil {
				_ = replacementLocator.Close()
			}
			c.log().Error("failed to update health checker config", "error", err)
			return err
		}
	}

	if replacementLocator != nil {
		healthAwarePool, ok := c.locator.(*HealthAwarePool)
		if !ok {
			_ = replacementLocator.Close()
			return fmt.Errorf("active locator does not support atomic replacement")
		}

		if err := healthAwarePool.ReplaceLocator(replacementLocator); err != nil {
			_ = replacementLocator.Close()
			if healthCheckerChanged {
				if rollbackErr := c.updateHealthCheckerConfig(oldConfig.HealthChecker); rollbackErr != nil {
					return fmt.Errorf("failed to replace active locator: %v; failed to roll back health checker config: %w", err, rollbackErr)
				}
			}
			return fmt.Errorf("failed to replace active locator: %w", err)
		}

		c.nodeFactory = replacementFactory
		c.log().Info("client locator replaced after configuration change",
			"locator_changed", locatorChanged, "connection_changed", connectionChanged)
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
	GetNativeClient() any
}

// GetClientForKey returns the native protocol client (e.g. *redis.Client,
// *grpc.ClientConn) for the node that owns the given key. If the configured
// NodeFactory produces nodes that don't implement nativeClientProvider, the
// Node itself is returned instead.
func (c *Client) GetClientForKey(ctx context.Context, key string) (any, error) {
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
	if err := ValidateHealthCheckerConfig(newConfig); err != nil {
		return fmt.Errorf("invalid health checker config: %w", err)
	}

	if err := c.healthChecker.UpdateConfig(newConfig); err != nil {
		return fmt.Errorf("failed to update health checker config: %w", err)
	}

	c.log().Info("health checker config updated",
		"enabled", newConfig.Enabled, "interval", newConfig.Interval, "timeout", newConfig.Timeout)

	return nil
}

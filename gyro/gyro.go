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
}

// ServiceDiscovery provides the read-side service discovery capabilities used
// by Client. Registration is optional and is exposed separately through
// ServiceRegistrar.
type ServiceDiscovery interface {
	Discover(ctx context.Context, serviceName string) ([]NodeInfo, error)
	Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error)
}

// ServiceRegistrar provides optional service registration capabilities.
type ServiceRegistrar interface {
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
			removed := false
			if watchers, exists := ssd.watchers[serviceName]; exists {
				for i, watcher := range watchers {
					if watcher == ch {
						ssd.watchers[serviceName] = append(watchers[:i], watchers[i+1:]...)
						removed = true
						break
					}
				}
			}
			if removed {
				close(ch)
			}
			ssd.mu.Unlock()
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

type clientDeps struct {
	serviceName   string
	discovery     ServiceDiscovery
	configManager *ConfigManager
	nodeFactory   NodeFactory
	healthChecker HealthChecker
}

type clientRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	pool   *HealthAwarePool
}

type clientState struct {
	run           *clientRun
	prepared      *HealthAwarePool
	nodeFactory   NodeFactory
	nodeInfos     map[string]NodeInfo
	configWatcher bool

	// Health tracking belongs to the client state snapshot, not to dependency wiring.
	serviceDiscoveryHealthy   bool
	lastServiceDiscoveryError string
	serviceDiscoveryRetries   int
	lastHealthCheck           time.Time
}

// Client provides configuration and service discovery.
type Client struct {
	stateMu     sync.RWMutex
	lifecycleMu sync.Mutex
	deps        clientDeps
	state       clientState
	logger      atomic.Pointer[slog.Logger]
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
		deps: clientDeps{
			serviceName:   serviceName,
			discovery:     discovery,
			configManager: configManager,
			nodeFactory:   nodeFactory,
			healthChecker: healthChecker,
		},
		state: clientState{
			nodeInfos:   make(map[string]NodeInfo),
			nodeFactory: nodeFactory,
			// False until watchServiceNodes establishes its first watch.
			serviceDiscoveryHealthy: false,
		},
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
	c.stateMu.RLock()
	var locator Locator
	if c.state.run != nil {
		locator = c.state.run.pool
	} else {
		locator = c.state.prepared
	}
	c.stateMu.RUnlock()
	if setter, ok := locator.(interface{ SetLogger(*slog.Logger) }); ok {
		setter.SetLogger(logger)
	}
}

func (c *Client) log() *slog.Logger {
	return c.logger.Load()
}

// buildLocatorUnsafe builds a locator from a configuration snapshot without
// publishing it. Discovery and node construction happen outside Client locks.
func (c *Client) buildLocatorUnsafe(config *ClientConfig, nodeFactory NodeFactory) (Locator, []NodeInfo, error) {
	ctx := context.Background()

	nodeInfos, err := c.deps.discovery.Discover(ctx, c.deps.serviceName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to discover initial nodes: %w", err)
	}
	nodeInfos = cloneNodeInfos(nodeInfos)
	sortNodeInfosByID(nodeInfos)

	baseLocator, err := NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create locator: %w", err)
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
			return nil, nil, fmt.Errorf("failed to create node %s: %w", nodeInfo.ID, err)
		}

		if err := baseLocator.AddNodeContext(context.Background(), node); err != nil {
			_ = node.Close()
			return nil, nil, fmt.Errorf("failed to add node %s to locator: %w", nodeInfo.ID, err)
		}
	}
	committed = true
	return baseLocator, nodeInfos, nil
}

// initialize prepares a complete pool without holding the client state lock,
// then publishes it with a short state transition.
func (c *Client) initialize() error {
	config := c.deps.configManager.GetConfig()
	c.stateMu.RLock()
	nodeFactory := c.state.nodeFactory
	healthChecker := c.deps.healthChecker
	c.stateMu.RUnlock()
	baseLocator, nodeInfos, err := c.buildLocatorUnsafe(config, nodeFactory)
	if err != nil {
		return err
	}

	healthAwarePool := NewHealthAwarePoolWithChecker(baseLocator, healthChecker)
	healthAwarePool.SetLogger(c.log())

	c.stateMu.Lock()
	if c.state.run != nil || c.state.prepared != nil {
		c.stateMu.Unlock()
		_ = healthAwarePool.Close()
		return nil
	}
	c.state.prepared = healthAwarePool
	c.state.nodeInfos = make(map[string]NodeInfo, len(nodeInfos))
	for _, nodeInfo := range nodeInfos {
		c.state.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
	}
	c.stateMu.Unlock()

	return nil
}

// getLocator returns the underlying locator
func (c *Client) getLocator() Locator {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.state.run == nil {
		return nil
	}
	return c.state.run.pool
}

// getPoolNodes returns all nodes from the locator
func (c *Client) getPoolNodes() []Node {
	locator := c.getLocator()
	if locator == nil {
		return nil
	}
	return locator.GetAllNodes()
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

	c.stateMu.RLock()
	run := c.state.run
	c.stateMu.RUnlock()
	if run != nil && run.ctx != nil {
		return fmt.Errorf("client is already running")
	}

	// Stop releases the locator so a later Start can create a fresh run with
	// new node connections and a new HealthAwarePool.
	c.stateMu.RLock()
	prepared := c.state.prepared
	c.stateMu.RUnlock()
	if prepared == nil {
		if err := c.initialize(); err != nil {
			return fmt.Errorf("failed to initialize client for start: %w", err)
		}
	}

	runCtx, runCancel := context.WithCancel(ctx)
	c.stateMu.Lock()
	run = &clientRun{ctx: runCtx, cancel: runCancel, pool: c.state.prepared}
	c.state.prepared = nil
	c.state.run = run
	registerConfigWatcher := !c.state.configWatcher
	c.state.configWatcher = true
	c.stateMu.Unlock()

	if registerConfigWatcher {
		c.deps.configManager.AddConfigWatcher(c.handleConfigChange)
	}

	go c.watchServiceNodes(run)
	run.pool.StartHealthMonitoring(runCtx)

	return nil
}

// Stop stops the client.
func (c *Client) Stop() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.stateMu.Lock()
	run := c.state.run
	prepared := c.state.prepared
	c.state.run = nil
	c.state.prepared = nil
	c.state.nodeInfos = nil
	c.stateMu.Unlock()
	if run != nil {
		if run.cancel != nil {
			run.cancel()
		}
		return run.pool.Close()
	}
	if prepared != nil {
		return prepared.Close()
	}

	return nil
}

// GetLocator returns the underlying locator for direct access.
func (c *Client) GetLocator() Locator {
	return c.getLocator()
}

// Close closes the client.
func (c *Client) Close() error {
	return c.Stop()
}

// Health returns the current health status of the client
func (c *Client) Health() *ClientHealth {
	lastHealthCheck := c.lastHealthCheckTime()
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()

	return &ClientHealth{
		ServiceDiscoveryHealthy:   c.state.serviceDiscoveryHealthy,
		LastServiceDiscoveryError: c.state.lastServiceDiscoveryError,
		ServiceDiscoveryRetries:   c.state.serviceDiscoveryRetries,
		LastHealthCheck:           lastHealthCheck,
	}
}

func (c *Client) lastHealthCheckTime() time.Time {
	checker := c.deps.healthChecker
	if provider, ok := checker.(interface{ LastCheckTime() time.Time }); ok {
		return provider.LastCheckTime()
	}
	return time.Time{}
}

// GetNodeForKey returns the routed node metadata without exposing a native
// protocol client. It is useful for observability and routing assertions.
func (c *Client) GetNodeForKey(ctx context.Context, key string) (Node, error) {
	locator := c.getLocator()
	if locator == nil {
		return nil, fmt.Errorf("client not started")
	}
	return locator.Get(ctx, key)
}

// IsHealthy returns true if the client is healthy
func (c *Client) IsHealthy() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state.serviceDiscoveryHealthy
}

// updateServiceDiscoveryHealth updates the service discovery health status
func (c *Client) updateServiceDiscoveryHealth(run *clientRun, healthy bool, err error) {
	// Serialize health publication with topology reconciliation so stateMu is
	// never held while another lifecycle operation performs node I/O.
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.state.run != run {
		return
	}

	c.state.serviceDiscoveryHealthy = healthy
	if err != nil {
		c.state.lastServiceDiscoveryError = err.Error()
		if !healthy {
			c.state.serviceDiscoveryRetries++
		}
	} else {
		c.state.lastServiceDiscoveryError = ""
		if healthy {
			c.state.serviceDiscoveryRetries = 0
		}
	}
}

// watchServiceNodes watches for service node changes with retry mechanism.
func (c *Client) watchServiceNodes(run *clientRun) {
	ctx := run.ctx
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

		nodesCh, err := c.deps.discovery.Watch(ctx, c.deps.serviceName)
		if err != nil {
			c.updateServiceDiscoveryHealth(run, false, err)
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
		c.updateServiceDiscoveryHealth(run, true, nil)
		c.log().Info("service discovery watch established")

		watchFailed := c.processServiceWatch(run, nodesCh)
		if !watchFailed {
			return
		}

		c.updateServiceDiscoveryHealth(run, false, fmt.Errorf("service discovery watch channel closed unexpectedly"))
		c.log().Warn("service discovery watch failed, retrying")
	}
}

// processServiceWatch processes events from the service discovery watch channel
// Returns true if the watch failed and should be retried, false for normal shutdown
func (c *Client) processServiceWatch(run *clientRun, nodesCh <-chan []NodeInfo) bool {
	for {
		select {
		case <-run.ctx.Done():
			return false // normal shutdown
		case nodes, ok := <-nodesCh:
			if !ok {
				return true // channel closed, caller should retry
			}
			c.reconcileServiceNodes(run, nodes)
		}
	}
}

// handleServiceNodesChange handles changes in service nodes with incremental updates.
func (c *Client) handleServiceNodesChange(ctx context.Context, newNodeInfos []NodeInfo) {
	c.stateMu.RLock()
	run := c.state.run
	c.stateMu.RUnlock()
	if run != nil {
		c.reconcileServiceNodes(run, newNodeInfos)
	}
}

// reconcileServiceNodes serializes topology preparation with configuration
// replacement and publishes only if this run still owns the client state.
func (c *Client) reconcileServiceNodes(run *clientRun, newNodeInfos []NodeInfo) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	if c.state.run != run || run.ctx == nil || run.ctx.Err() != nil {
		c.stateMu.RUnlock()
		return
	}
	locator := run.pool
	healthChecker := c.deps.healthChecker
	nodeFactory := c.state.nodeFactory
	currentInfos := make(map[string]NodeInfo, len(c.state.nodeInfos))
	for nodeID, nodeInfo := range c.state.nodeInfos {
		currentInfos[nodeID] = cloneNodeInfo(nodeInfo)
	}
	c.stateMu.RUnlock()
	currentNodes := locator.GetAllNodes()

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
			oldNodeInfo, hasOldInfo := currentInfos[nodeID]
			if currentNode.Address() != newNodeInfo.Address || !hasOldInfo || !stringMapEqual(oldNodeInfo.Metadata, newNodeInfo.Metadata) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}
	sortNodeInfosByID(nodesToUpdate)

	for _, nodeID := range nodesToRemove {
		if err := locator.RemoveNodeContext(run.ctx, nodeID); err != nil {
			c.log().Error("failed to remove node", "node_id", nodeID, "error", err)
		} else {
			delete(currentInfos, nodeID)
			if healthChecker != nil {
				healthChecker.RemoveNode(nodeID)
			}
			c.log().Info("node removed", "node_id", nodeID)
		}
	}

	for _, nodeInfo := range nodesToAdd {
		node, err := nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNodeContext(run.ctx, node); err != nil {
			_ = node.Close()
			c.log().Error("failed to add node", "node_id", nodeInfo.ID, "error", err)
		} else {
			currentInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			if healthChecker != nil {
				healthChecker.AddNode(node)
			}
			c.log().Info("node added", "node_id", nodeInfo.ID)
		}
	}

	for _, nodeInfo := range nodesToUpdate {
		if err := locator.RemoveNodeContext(run.ctx, nodeInfo.ID); err != nil {
			c.log().Error("failed to remove node for update", "node_id", nodeInfo.ID, "error", err)
			continue
		}
		if healthChecker != nil {
			healthChecker.RemoveNode(nodeInfo.ID)
		}

		node, err := nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create updated node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNodeContext(run.ctx, node); err != nil {
			_ = node.Close()
			c.log().Error("failed to add updated node", "node_id", nodeInfo.ID, "error", err)
		} else {
			currentInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			if healthChecker != nil {
				healthChecker.AddNode(node)
			}
			c.log().Info("node updated", "node_id", nodeInfo.ID)
		}
	}

	c.log().Info("incremental update completed",
		"added", len(nodesToAdd), "removed", len(nodesToRemove), "updated", len(nodesToUpdate))

	c.stateMu.Lock()
	if c.state.run == run && run.ctx.Err() == nil {
		c.state.nodeInfos = currentInfos
	}
	c.stateMu.Unlock()
}

// handleConfigChange handles configuration changes with incremental updates.
func (c *Client) handleConfigChange(oldConfig, newConfig *ClientConfig) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	run := c.state.run
	currentFactory := c.state.nodeFactory
	c.stateMu.RUnlock()
	if run == nil || run.ctx == nil || run.ctx.Err() != nil {
		return nil
	}
	activeLocator := run.pool

	locatorChanged := !c.locatorConfigEqual(oldConfig.Locator, newConfig.Locator)
	connectionChanged := !c.connectionConfigEqual(oldConfig.Connection, newConfig.Connection)
	healthCheckerChanged := !c.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker)

	var (
		replacementLocator   Locator
		replacementFactory   NodeFactory
		replacementNodeInfos []NodeInfo
	)

	if locatorChanged || connectionChanged {
		replacementFactory = currentFactory
		if connectionChanged {
			configurableFactory, ok := currentFactory.(ConnectionConfigurableNodeFactory)
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
		replacementLocator, replacementNodeInfos, err = c.buildLocatorUnsafe(newConfig, replacementFactory)
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
		if err := activeLocator.ReplaceLocator(replacementLocator); err != nil {
			_ = replacementLocator.Close()
			if healthCheckerChanged {
				if rollbackErr := c.updateHealthCheckerConfig(oldConfig.HealthChecker); rollbackErr != nil {
					return fmt.Errorf("failed to replace active locator: %v; failed to roll back health checker config: %w", err, rollbackErr)
				}
			}
			return fmt.Errorf("failed to replace active locator: %w", err)
		}

		c.stateMu.Lock()
		if c.state.run == run && run.ctx.Err() == nil {
			c.state.nodeFactory = replacementFactory
			c.state.nodeInfos = make(map[string]NodeInfo, len(replacementNodeInfos))
			for _, nodeInfo := range replacementNodeInfos {
				c.state.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			}
		}
		c.stateMu.Unlock()
		c.log().Info("client locator replaced after configuration change",
			"locator_changed", locatorChanged, "connection_changed", connectionChanged)
	}

	c.log().Info("config update completed")
	return nil
}

// GetStats returns client statistics.
func (c *Client) GetStats() HealthAwarePoolStats {
	locator := c.getLocator()
	if locator == nil {
		return HealthAwarePoolStats{}
	}

	allNodes := locator.GetAllNodes()
	totalNodes := len(allNodes)
	healthyCount := 0
	for _, node := range allNodes {
		if c.deps.healthChecker.IsNodeHealthy(node.ID()) {
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
	locator := c.getLocator()
	if locator == nil {
		return nil, fmt.Errorf("client not started")
	}

	node, err := locator.Get(ctx, key)
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

	checker, ok := c.deps.healthChecker.(ConfigurableHealthChecker)
	if !ok {
		return fmt.Errorf("health checker does not support runtime configuration")
	}
	if err := checker.UpdateConfig(newConfig); err != nil {
		return fmt.Errorf("failed to update health checker config: %w", err)
	}

	c.log().Info("health checker config updated",
		"enabled", newConfig.Enabled, "interval", newConfig.Interval, "timeout", newConfig.Timeout)

	return nil
}

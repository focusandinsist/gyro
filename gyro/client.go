package gyro

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Client coordinates routing, service discovery, health monitoring, and
// lifecycle transitions for a configured set of backend nodes.
type Client struct {
	stateMu     sync.RWMutex
	lifecycleMu sync.Mutex
	deps        clientDeps
	state       clientState
	logger      atomic.Pointer[slog.Logger]
}

// clientDeps holds the immutable dependencies supplied when a Client is built.
// Keeping them separate from clientState makes runtime transitions explicit.
type clientDeps struct {
	serviceName   string
	discovery     ServiceDiscovery
	configManager *ConfigManager
	nodeFactory   NodeFactory
	healthChecker HealthChecker
}

// clientRun is the owned runtime instance for one Start-to-Stop interval.
// A new instance is created on every restart so old goroutines cannot publish
// into a later run.
type clientRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	pool   *HealthAwarePool
}

// clientState contains mutable runtime data protected by Client.stateMu.
// The run and prepared fields represent the active and not-yet-started pool;
// nodeInfos and health fields are the latest published snapshots.
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
}

// discardLogger is the default logger for internal components: it never
// produces output, so a library consumer that doesn't call SetLogger sees
// nothing on stdout/stderr. Built manually (rather than via slog.DiscardHandler,
// added in Go 1.24) to stay compatible with the go.mod minimum version.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

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
func (c *Client) buildLocatorUnsafe(config *Config, nodeFactory NodeFactory) (Locator, []NodeInfo, error) {
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

// getLocator returns the active underlying locator.
func (c *Client) getLocator() Locator {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.state.run == nil {
		return nil
	}
	return c.state.run.pool
}

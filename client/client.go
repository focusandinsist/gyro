package client

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"gyro"
	"gyro/internal/topology"
)

// Client coordinates routing, service discovery, health monitoring, and
// lifecycle transitions for a configured set of backend nodes.
type Client struct {
	stateMu     sync.RWMutex
	lifecycleMu sync.Mutex
	topologyMu  sync.Mutex
	deps        clientDeps
	state       clientState
	logger      atomic.Pointer[slog.Logger]
}

// clientDeps holds the immutable dependencies supplied when a Client is built.
// Keeping them separate from clientState makes runtime transitions explicit.
type clientDeps struct {
	serviceName   string
	discovery     gyro.ServiceDiscovery
	configManager *ConfigManager
	nodeFactory   gyro.NodeFactory
	healthChecker gyro.HealthChecker
}

// NewClient creates a client with application-provided discovery, node
// factory, configuration, and health-checking dependencies. Most applications
// should use a protocol adapter's convenience constructor instead.
func NewClient(serviceName string, discovery gyro.ServiceDiscovery, configManager *ConfigManager, nodeFactory gyro.NodeFactory, healthChecker gyro.HealthChecker) (*Client, error) {
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
			nodeInfos:      make(map[string]gyro.NodeInfo),
			nodeFactory:    nodeFactory,
			topologyStore:  topology.NewStore(),
			retiredSources: make(map[string]struct{}),
			// False until watchServiceNodes establishes its first watch.
			serviceDiscoveryHealthy: false,
			topologyStale:           true,
		},
	}
	client.logger.Store(discardLogger)

	if err := client.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	return client, nil
}

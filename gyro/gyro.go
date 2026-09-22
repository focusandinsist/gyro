// Package gyro provides client-side consistent-hash routing with health-aware
// failover, service discovery, and runtime configuration updates.
//
// Most applications should use a protocol adapter such as gyro/redis or
// gyro/grpc. The core Client API is available when an application needs to
// provide its own discovery, node factory, or health checker.
package gyro

import "fmt"

// NewClient creates a client with application-provided discovery, node
// factory, configuration, and health-checking dependencies. Most applications
// should use a protocol adapter's convenience constructor instead.
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

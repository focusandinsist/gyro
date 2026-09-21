package gyro

import (
	"context"
	"fmt"
	"time"
)

// Health returns the current health status of the client.
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

// IsHealthy returns true if the client is healthy.
func (c *Client) IsHealthy() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state.serviceDiscoveryHealthy
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

// GetClientForKey returns the native protocol client for the node that owns
// the given key. If the node does not expose a native client, the Node itself
// is returned instead.
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

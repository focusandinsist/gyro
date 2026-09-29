package client

import (
	"context"
	"fmt"
	"time"

	"github.com/focusandinsist/gyro/gyro"
)

// ClientHealth represents the health status of a client.
type ClientHealth struct {
	ServiceDiscoveryHealthy   bool      `json:"service_discovery_healthy"`
	LastServiceDiscoveryError string    `json:"last_service_discovery_error,omitempty"`
	ServiceDiscoveryRetries   int       `json:"service_discovery_retries"`
	LastHealthCheck           time.Time `json:"last_health_check"`
}

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
func (c *Client) GetNodeForKey(ctx context.Context, key string) (gyro.Node, error) {
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
func (c *Client) GetStats() gyro.HealthAwarePoolStats {
	locator := c.getLocator()
	if locator == nil {
		return gyro.HealthAwarePoolStats{}
	}

	allNodes := locator.GetAllNodes()
	totalNodes := len(allNodes)
	healthyCount := 0
	for _, node := range allNodes {
		if c.deps.healthChecker.IsNodeHealthy(node.ID()) {
			healthyCount++
		}
	}

	return gyro.HealthAwarePoolStats{
		TotalNodes:     totalNodes,
		HealthyNodes:   healthyCount,
		UnhealthyNodes: totalNodes - healthyCount,
	}
}

// Package testbed provides test cluster management for integration testing.
package testbed

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/focusandinsist/gyro/test/integration/fake_servers"
)

// TestCluster manages a cluster of fake servers for integration testing.
type TestCluster struct {
	mu sync.RWMutex

	// Server management
	grpcServers  map[int]*fake_servers.FakeGRPCServer
	redisServers map[int]*fake_servers.FakeRedisServer
	controlAPI   *fake_servers.ControlAPI

	// Configuration
	controlPort int
	started     bool
}

// ClusterConfig defines the configuration for a test cluster.
type ClusterConfig struct {
	GRPCPorts   []int
	RedisPorts  []int
	ControlPort int
}

// DefaultClusterConfig returns a default cluster configuration.
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		GRPCPorts:   []int{8001, 8002, 8003},
		RedisPorts:  []int{16379, 16380, 16381}, // Use non-standard ports to avoid conflicts
		ControlPort: 9999,
	}
}

// NewTestCluster creates a new test cluster.
func NewTestCluster(config ClusterConfig) *TestCluster {
	cluster := &TestCluster{
		grpcServers:  make(map[int]*fake_servers.FakeGRPCServer),
		redisServers: make(map[int]*fake_servers.FakeRedisServer),
		controlPort:  config.ControlPort,
	}

	// Create control API
	cluster.controlAPI = fake_servers.NewControlAPI(config.ControlPort)

	// Create gRPC servers
	for _, port := range config.GRPCPorts {
		server := fake_servers.NewFakeGRPCServer(port)
		cluster.grpcServers[port] = server
		cluster.controlAPI.RegisterGRPCServer(port, server)
	}

	// Create Redis servers
	for _, port := range config.RedisPorts {
		server := fake_servers.NewFakeRedisServer(port)
		cluster.redisServers[port] = server
		cluster.controlAPI.RegisterRedisServer(port, server)
	}

	return cluster
}

// Start starts all servers in the cluster.
func (c *TestCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("cluster already started")
	}

	log.Println("Starting test cluster...")

	// Start control API first
	if err := c.controlAPI.Start(); err != nil {
		return fmt.Errorf("failed to start control API: %w", err)
	}

	// Start gRPC servers
	for port, server := range c.grpcServers {
		if err := server.Start(); err != nil {
			return fmt.Errorf("failed to start gRPC server on port %d: %w", port, err)
		}
	}

	// Start Redis servers
	for port, server := range c.redisServers {
		if err := server.Start(); err != nil {
			return fmt.Errorf("failed to start Redis server on port %d: %w", port, err)
		}
	}

	c.started = true

	// Wait for all servers to be ready
	time.Sleep(500 * time.Millisecond)

	log.Printf("Test cluster started successfully:")
	log.Printf("  Control API: http://localhost:%d", c.controlPort)
	log.Printf("  gRPC servers: %v", c.getGRPCPorts())
	log.Printf("  Redis servers: %v", c.getRedisPorts())

	return nil
}

// Stop stops all servers in the cluster.
func (c *TestCluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return
	}

	log.Println("Stopping test cluster...")

	// Stop all gRPC servers
	for port, server := range c.grpcServers {
		log.Printf("Stopping gRPC server on port %d", port)
		server.Stop()
	}

	// Stop all Redis servers
	for port, server := range c.redisServers {
		log.Printf("Stopping Redis server on port %d", port)
		server.Stop()
	}

	// Stop control API
	if c.controlAPI != nil {
		c.controlAPI.Stop()
	}

	c.started = false
	log.Println("Test cluster stopped")
}

// StartGRPCServer starts a new gRPC server on the specified port.
func (c *TestCluster) StartGRPCServer(port int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if server already exists
	if _, exists := c.grpcServers[port]; exists {
		return fmt.Errorf("gRPC server already running on port %d", port)
	}

	// Create and start new server
	server := fake_servers.NewFakeGRPCServer(port)
	if err := server.Start(); err != nil {
		return fmt.Errorf("failed to start gRPC server on port %d: %w", port, err)
	}

	c.grpcServers[port] = server
	log.Printf("Started new gRPC server on port %d", port)
	return nil
}

// StartRedisServer starts a new Redis server on the specified port.
func (c *TestCluster) StartRedisServer(port int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if server already exists
	if _, exists := c.redisServers[port]; exists {
		return fmt.Errorf("Redis server already running on port %d", port)
	}

	// Create and start new server
	server := fake_servers.NewFakeRedisServer(port)
	if err := server.Start(); err != nil {
		return fmt.Errorf("failed to start Redis server on port %d: %w", port, err)
	}

	c.redisServers[port] = server
	log.Printf("Started new Redis server on port %d", port)
	return nil
}

// StopGRPCServer stops a gRPC server on the specified port.
func (c *TestCluster) StopGRPCServer(port int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	server, exists := c.grpcServers[port]
	if !exists {
		return fmt.Errorf("no gRPC server running on port %d", port)
	}

	server.Stop()
	delete(c.grpcServers, port)
	log.Printf("Stopped gRPC server on port %d", port)
	return nil
}

// StopRedisServer stops a Redis server on the specified port.
func (c *TestCluster) StopRedisServer(port int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	server, exists := c.redisServers[port]
	if !exists {
		return fmt.Errorf("no Redis server running on port %d", port)
	}

	server.Stop()
	delete(c.redisServers, port)
	log.Printf("Stopped Redis server on port %d", port)
	return nil
}

// GetGRPCAddresses returns the addresses of all gRPC servers.
func (c *TestCluster) GetGRPCAddresses() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := make([]string, 0, len(c.grpcServers))
	for port := range c.grpcServers {
		addresses = append(addresses, fmt.Sprintf("localhost:%d", port))
	}
	return addresses
}

// GetRedisAddresses returns the addresses of all Redis servers.
func (c *TestCluster) GetRedisAddresses() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addresses := make([]string, 0, len(c.redisServers))
	for port := range c.redisServers {
		addresses = append(addresses, fmt.Sprintf("localhost:%d", port))
	}
	return addresses
}

// GetControlAPIURL returns the control API URL.
func (c *TestCluster) GetControlAPIURL() string {
	return fmt.Sprintf("http://localhost:%d", c.controlPort)
}

// SetServerHealth sets the health status of a specific server.
func (c *TestCluster) SetServerHealth(serverType string, port int, healthy bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch serverType {
	case "grpc":
		if server, exists := c.grpcServers[port]; exists {
			server.SetHealthy(healthy)
			return nil
		}
		return fmt.Errorf("gRPC server on port %d not found", port)

	case "redis":
		if server, exists := c.redisServers[port]; exists {
			server.SetHealthy(healthy)
			return nil
		}
		return fmt.Errorf("Redis server on port %d not found", port)

	default:
		return fmt.Errorf("unknown server type: %s", serverType)
	}
}

// GetServerStats returns statistics for a specific server.
func (c *TestCluster) GetServerStats(serverType string, port int) (map[string]any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch serverType {
	case "grpc":
		if server, exists := c.grpcServers[port]; exists {
			return server.GetStats(), nil
		}
		return nil, fmt.Errorf("gRPC server on port %d not found", port)

	case "redis":
		if server, exists := c.redisServers[port]; exists {
			return server.GetStats(), nil
		}
		return nil, fmt.Errorf("Redis server on port %d not found", port)

	default:
		return nil, fmt.Errorf("unknown server type: %s", serverType)
	}
}

// WaitForHealthCheck waits for health check cycles to complete.
func (c *TestCluster) WaitForHealthCheck(cycles int) {
	// Assuming health check interval is 1 second, wait for specified cycles
	duration := time.Duration(cycles) * time.Second
	log.Printf("Waiting %v for %d health check cycles", duration, cycles)
	time.Sleep(duration)
}

// Helper methods

func (c *TestCluster) getGRPCPorts() []int {
	ports := make([]int, 0, len(c.grpcServers))
	for port := range c.grpcServers {
		ports = append(ports, port)
	}
	return ports
}

func (c *TestCluster) getRedisPorts() []int {
	ports := make([]int, 0, len(c.redisServers))
	for port := range c.redisServers {
		ports = append(ports, port)
	}
	return ports
}

// IsStarted returns whether the cluster is started.
func (c *TestCluster) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// GetClusterStatus returns the overall status of the cluster.
func (c *TestCluster) GetClusterStatus() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := map[string]any{
		"started":       c.started,
		"control_port":  c.controlPort,
		"grpc_servers":  make([]map[string]any, 0),
		"redis_servers": make([]map[string]any, 0),
	}

	for port, server := range c.grpcServers {
		stats := server.GetStats()
		stats["port"] = port
		status["grpc_servers"] = append(status["grpc_servers"].([]map[string]any), stats)
	}

	for port, server := range c.redisServers {
		stats := server.GetStats()
		stats["port"] = port
		status["redis_servers"] = append(status["redis_servers"].([]map[string]any), stats)
	}

	return status
}

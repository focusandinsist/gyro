package gyro

import (
	"context"
	"time"
)

// NodeInfo contains information about a service node.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Weight   int               `json:"weight,omitempty"`
}

type Node interface {
	ID() string
	Address() string
	IsHealthy(ctx context.Context) bool
	Close() error
}

type Pool interface {
	Get(ctx context.Context, key string) (Node, error)
	GetReplicas(ctx context.Context, key string, count int) ([]Node, error)
	AddNode(node Node) error
	RemoveNode(nodeID string) error
	GetAllNodes() []Node
	Close() error
}

// HealthChecker provides health checking capabilities for nodes.
type HealthChecker interface {
	// Check performs a health check on the given node.
	Check(ctx context.Context, node Node) error

	// StartMonitoring starts continuous health monitoring.
	StartMonitoring(ctx context.Context, interval time.Duration)

	// StopMonitoring stops health monitoring.
	StopMonitoring()
}

// LoadBalancer provides load balancing strategies for node selection.
type LoadBalancer interface {
	// Select selects the best node from the given candidates.
	Select(ctx context.Context, nodes []Node, key string) (Node, error)

	// UpdateStats updates the load statistics for a node.
	UpdateStats(nodeID string, latency time.Duration, success bool)
}

// ServiceDiscovery provides dynamic service discovery capabilities.
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

// ClientConfig the configuration for Gyro clients.
// This is a composition of individual component configurations.
type ClientConfig struct {
	// Pool configures the connection pool behavior.
	Pool PoolConfig `json:"pool"`

	// HealthChecker configures health checking behavior.
	HealthChecker HealthCheckerConfig `json:"health_checker"`

	// Connection configures connection-specific behavior.
	Connection ConnectionConfig `json:"connection"`
}

// ConnectionConfig configures connection-specific behavior.
type ConnectionConfig struct {
	// MaxIdleConns is the maximum number of idle connections per node.
	MaxIdleConns int `json:"max_idle_conns"`

	// MaxActiveConns is the maximum number of active connections per node.
	MaxActiveConns int `json:"max_active_conns"`

	// IdleTimeout is the timeout for idle connections.
	IdleTimeout time.Duration `json:"idle_timeout"`

	// ConnectTimeout is the timeout for establishing connections.
	ConnectTimeout time.Duration `json:"connect_timeout"`

	// ReadTimeout is the timeout for read operations.
	ReadTimeout time.Duration `json:"read_timeout"`

	// WriteTimeout is the timeout for write operations.
	WriteTimeout time.Duration `json:"write_timeout"`
}

// DefaultConnectionConfig .
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		MaxIdleConns:   10,
		MaxActiveConns: 100,
		IdleTimeout:    5 * time.Minute,
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

// DefaultClientConfig .
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Pool:          DefaultPoolConfig(),
		HealthChecker: DefaultHealthCheckerConfig(),
		Connection:    DefaultConnectionConfig(),
	}
}

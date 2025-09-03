package gyro

import (
	"time"
)

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

// Package redis provides the Redis protocol adapter and convenience client
// for Gyro's health-aware consistent-hash routing.
package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/routed"
)

// Connection is the adapter connection abstraction.
type Connection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetNativeClient() any
}

// DefaultConnection wraps a real go-redis client.
type DefaultConnection struct {
	address   string
	client    *goredis.Client
	connected atomic.Bool
}

// NewConnection creates a connection backed by go-redis.
func NewConnection(address string, config gyro.ConnectionConfig) (Connection, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:            address,
		Protocol:        2, // for broader compatibility
		DialTimeout:     config.ConnectTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolSize:        config.MaxActiveConns,
		MinIdleConns:    config.MaxIdleConns,
		ConnMaxIdleTime: config.IdleTimeout,
	})

	conn := &DefaultConnection{
		address: address,
		client:  client,
	}
	conn.connected.Store(true) // optimistic; Ping() will correct this

	return conn, nil
}

// Close closes the Redis connection.
func (c *DefaultConnection) Close() error {
	c.connected.Store(false)
	return c.client.Close()
}

// IsConnected returns the last known connectivity state (cheap, no I/O).
// Call Ping to actually verify and refresh this state.
func (c *DefaultConnection) IsConnected() bool {
	return c.connected.Load()
}

// Ping tests the connection against the real Redis server.
func (c *DefaultConnection) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		c.connected.Store(false)
		return fmt.Errorf("redis ping to %s failed: %w", c.address, err)
	}
	c.connected.Store(true)
	return nil
}

// GetNativeClient returns the underlying *redis.Client for direct use with
// the full go-redis API.
func (c *DefaultConnection) GetNativeClient() any {
	return c.client
}

// Node adapts a Redis connection to the gyro.Node interface.
type Node struct {
	id      string
	address string
	conn    Connection
	mu      sync.RWMutex
	closed  bool
}

func NewNode(id, address string, conn Connection) *Node {
	return &Node{
		id:      id,
		address: address,
		conn:    conn,
	}
}

func (rn *Node) ID() string {
	return rn.id
}

func (rn *Node) Address() string {
	return rn.address
}

func (rn *Node) IsHealthy(ctx context.Context) bool {
	rn.mu.RLock()
	closed := rn.closed
	rn.mu.RUnlock()
	if closed {
		return false
	}

	return rn.conn.Ping(ctx) == nil
}

func (rn *Node) Close() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.closed {
		return nil
	}
	rn.closed = true
	return rn.conn.Close()
}

func (rn *Node) GetNativeClient() any {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.closed {
		return nil
	}

	return rn.conn.GetNativeClient()
}

type ClientConfig struct {
	Locator       gyro.LocatorConfig       `json:"locator"`
	HealthChecker gyro.HealthCheckerConfig `json:"health_checker"`
	Connection    gyro.ConnectionConfig    `json:"connection"`
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Locator:       gyro.DefaultLocatorConfig(),
		HealthChecker: gyro.DefaultHealthCheckerConfig(),
		Connection:    gyro.DefaultConnectionConfig(),
	}
}

// Client routes requests to Redis cluster nodes.
type Client struct {
	locator gyro.Locator
	config  *ClientConfig
	runtime *routed.Runtime
}

// NewClient creates a client-side sharded Redis cluster client. Each
// address gets its own go-redis connection; routing between them is done
// via consistent hashing.
func NewClient(addresses []string, config *ClientConfig) (*Client, error) {
	return newClient(addresses, config, nil, nil)
}

func newClient(addresses []string, config *ClientConfig, factory *NodeFactory, healthChecker gyro.HealthChecker) (*Client, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one Redis address is required")
	}
	if config == nil {
		config = DefaultClientConfig()
	}
	configSnapshot := *config
	config = &configSnapshot
	if err := gyro.ValidateHealthCheckerConfig(config.HealthChecker); err != nil {
		return nil, fmt.Errorf("invalid health checker config: %w", err)
	}

	if factory == nil {
		factory = &NodeFactory{config: config, newConnection: NewConnection}
	}
	runtime, err := routed.New(addresses, config.Locator, config.HealthChecker, "redis", factory.CreateNode, healthChecker)
	if err != nil {
		return nil, err
	}
	return &Client{locator: runtime.Locator(), config: config, runtime: runtime}, nil
}

func (rc *Client) GetClientForKey(ctx context.Context, key string) (any, error) {
	node, err := rc.GetNodeForKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for key '%s': %w", key, err)
	}

	redisNode, ok := node.(*Node)
	if !ok {
		return nil, fmt.Errorf("node %s is not a Redis node", node.ID())
	}

	nativeClient := redisNode.GetNativeClient()
	if nativeClient == nil {
		return nil, fmt.Errorf("node %s has no healthy client", node.ID())
	}

	return nativeClient, nil
}

// GetNodeForKey returns the routed node metadata for observability and tests.
func (rc *Client) GetNodeForKey(ctx context.Context, key string) (gyro.Node, error) {
	node, err := rc.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get node for key '%s': %w", key, err)
	}
	return node, nil
}

func (rc *Client) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]any, error) {
	clients, err := rc.runtime.Replicas(ctx, key, replicaCount, nativeClient)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas for key '%s': %w", key, err)
	}
	return clients, nil
}

func (rc *Client) GetAllClients() map[string]any {
	return rc.runtime.All(nativeClient)
}

func nativeClient(node gyro.Node) (any, bool) {
	redisNode, ok := node.(*Node)
	if !ok {
		return nil, false
	}
	return redisNode.GetNativeClient(), true
}

// Close closes all connections and releases resources.
func (rc *Client) Close() error {
	return rc.runtime.Close()
}

func NewCluster(addresses []string) (*Client, error) {
	return NewClient(addresses, nil)
}

// NodeFactory creates Redis nodes.
type NodeFactory struct {
	config        *ClientConfig
	newConnection func(address string, config gyro.ConnectionConfig) (Connection, error)
}

// NewNodeFactory creates a new Redis node factory.
func NewNodeFactory() *NodeFactory {
	return &NodeFactory{
		config:        DefaultClientConfig(),
		newConnection: NewConnection,
	}
}

// WithConnectionConfig returns an independent factory for a new connection
// configuration. The current factory remains unchanged until a Client has
// successfully built and published the replacement locator.
func (f *NodeFactory) WithConnectionConfig(connectionConfig gyro.ConnectionConfig) (gyro.NodeFactory, error) {
	if f == nil || f.config == nil {
		return nil, fmt.Errorf("Redis node factory is not initialized")
	}

	configCopy := *f.config
	configCopy.Connection = connectionConfig
	return &NodeFactory{config: &configCopy, newConnection: f.newConnection}, nil
}

// CreateNode creates a new Redis node from NodeInfo.
func (f *NodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	newConnection := f.newConnection
	if newConnection == nil {
		newConnection = NewConnection
	}
	conn, err := newConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection to %s: %w", info.Address, err)
	}

	return NewNode(info.ID, info.Address, conn), nil
}

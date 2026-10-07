// Package redis provides the Redis protocol adapter and convenience client
// for Gyro's health-aware consistent-hash routing.
package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"

	"gyro"
	"gyro/internal/policy"
	"gyro/internal/routed"
	"gyro/internal/routing"
)

// Connection is the adapter connection abstraction.
type Connection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetNativeClient() *goredis.Client
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
func (c *DefaultConnection) GetNativeClient() *goredis.Client {
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

func (rn *Node) GetNativeClient() *goredis.Client {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.closed {
		return nil
	}

	return rn.conn.GetNativeClient()
}

type ClientConfig struct {
	gyro.RoutingConfig
	Connection gyro.ConnectionConfig `json:"connection"`
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		RoutingConfig: gyro.DefaultRoutingConfig(),
		Connection:    gyro.DefaultConnectionConfig(),
	}
}

// Client routes requests to Redis cluster nodes.
type Client struct {
	locator gyro.Locator
	config  *ClientConfig
	runtime *routed.Runtime
}

// ClientLease keeps a routed Redis client alive until Release is called.
type ClientLease struct {
	client *goredis.Client
	node   *routing.NodeLease
}

func (l *ClientLease) Client() *goredis.Client { return l.client }
func (l *ClientLease) Release() error          { return l.node.Release() }

func (rc *Client) BorrowClientForKey(ctx context.Context, key string) (*ClientLease, error) {
	lease, err := rc.runtime.BorrowNodeForKey(ctx, key)
	if err != nil {
		return nil, err
	}
	node, ok := lease.Node().(*Node)
	if !ok {
		_ = lease.Release()
		return nil, fmt.Errorf("routed node is not a Redis node")
	}
	client := node.GetNativeClient()
	if client == nil {
		_ = lease.Release()
		return nil, fmt.Errorf("node %s has no healthy client", node.ID())
	}
	return &ClientLease{client: client, node: lease}, nil
}

// NewClient creates a client-side sharded Redis cluster client. Each
// address gets its own go-redis connection; routing between them is done
// via consistent hashing.
func NewClient(addresses []string, config *ClientConfig) (*Client, error) {
	return newClient(addresses, config, nil, nil)
}

func newClient(addresses []string, config *ClientConfig, factory *NodeFactory, healthChecker gyro.HealthChecker) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}
	configSnapshot := *config
	config = &configSnapshot
	if factory == nil {
		factory = &NodeFactory{config: config, newConnection: NewConnection}
	}
	runtime, err := routed.NewFixedWithPolicy(addresses, config.RoutingConfig, "redis", factory.CreateNode, healthChecker, policy.HealthyCandidate{AllowUnknown: true})
	if err != nil {
		return nil, err
	}
	return &Client{locator: runtime.Locator(), config: config, runtime: runtime}, nil
}

// GetClientForKey returns the current native client. Use BorrowClientForKey
// when the connection must remain open across a topology change.
func (rc *Client) GetClientForKey(ctx context.Context, key string) (*goredis.Client, error) {
	lease, err := rc.BorrowClientForKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for key '%s': %w", key, err)
	}
	defer lease.Release()
	return lease.Client(), nil
}

// GetRedisClientForKey returns the typed Redis resource for a routed key.
func (rc *Client) GetRedisClientForKey(ctx context.Context, key string) (*goredis.Client, error) {
	return rc.GetClientForKey(ctx, key)
}

// GetNodeForKey returns the routed node metadata for observability and tests.
func (rc *Client) GetNodeForKey(ctx context.Context, key string) (gyro.Node, error) {
	node, err := rc.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get node for key '%s': %w", key, err)
	}
	return node, nil
}

// GetClientsForReplicas returns current candidates without health filtering
// or leases. They may close when membership changes.
func (rc *Client) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]*goredis.Client, error) {
	nodes, err := rc.locator.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas for key '%s': %w", key, err)
	}
	clients := make([]*goredis.Client, 0, len(nodes))
	for _, node := range nodes {
		if redisNode, ok := node.(*Node); ok {
			if client := redisNode.GetNativeClient(); client != nil {
				clients = append(clients, client)
			}
		}
	}
	return clients, nil
}

// GetAllClients returns a snapshot of current native clients.
func (rc *Client) GetAllClients() map[string]*goredis.Client {
	clients := make(map[string]*goredis.Client)
	for _, node := range rc.locator.GetAllNodes() {
		if redisNode, ok := node.(*Node); ok {
			if client := redisNode.GetNativeClient(); client != nil {
				clients[node.ID()] = client
			}
		}
	}
	return clients
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

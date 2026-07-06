package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"

	"gyro/gyro"
)

// RedisConnection a Redis connection interface.
type RedisConnection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetNativeClient() interface{}
}

// DefaultRedisConnection wraps a real go-redis client.
type DefaultRedisConnection struct {
	address   string
	client    *goredis.Client
	connected atomic.Bool
}

// NewRedisConnection creates a new Redis connection.
func NewRedisConnection(address string, config gyro.ConnectionConfig) (RedisConnection, error) {
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

	conn := &DefaultRedisConnection{
		address: address,
		client:  client,
	}
	conn.connected.Store(true) // optimistic; Ping() will correct this

	return conn, nil
}

// Close closes the Redis connection.
func (c *DefaultRedisConnection) Close() error {
	c.connected.Store(false)
	return c.client.Close()
}

// IsConnected returns the last known connectivity state (cheap, no I/O).
// Call Ping to actually verify and refresh this state.
func (c *DefaultRedisConnection) IsConnected() bool {
	return c.connected.Load()
}

// Ping tests the connection against the real Redis server.
func (c *DefaultRedisConnection) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		c.connected.Store(false)
		return fmt.Errorf("redis ping to %s failed: %w", c.address, err)
	}
	c.connected.Store(true)
	return nil
}

// GetNativeClient returns the underlying *redis.Client for direct use with
// the full go-redis API.
func (c *DefaultRedisConnection) GetNativeClient() interface{} {
	return c.client
}

// RedisNode adapts a Redis connection to the gyro.Node interface.
type RedisNode struct {
	id      string
	address string
	conn    RedisConnection
	mu      sync.RWMutex
	healthy bool
}

func NewRedisNode(id, address string, conn RedisConnection) *RedisNode {
	return &RedisNode{
		id:      id,
		address: address,
		conn:    conn,
		healthy: true,
	}
}

func (rn *RedisNode) ID() string {
	return rn.id
}

func (rn *RedisNode) Address() string {
	return rn.address
}

func (rn *RedisNode) IsHealthy(ctx context.Context) bool {
	if err := rn.conn.Ping(ctx); err != nil {
		rn.mu.Lock()
		rn.healthy = false
		rn.mu.Unlock()
		return false
	}

	rn.mu.Lock()
	rn.healthy = true
	rn.mu.Unlock()
	return true
}

func (rn *RedisNode) Close() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.healthy = false
	return rn.conn.Close()
}

func (rn *RedisNode) GetNativeClient() interface{} {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if !rn.healthy {
		return nil
	}

	return rn.conn.GetNativeClient()
}

type RedisClientConfig struct {
	Locator    gyro.LocatorConfig    `json:"locator"`
	Connection gyro.ConnectionConfig `json:"connection"`
}

func DefaultRedisClientConfig() *RedisClientConfig {
	return &RedisClientConfig{
		Locator:    gyro.DefaultLocatorConfig(),
		Connection: gyro.DefaultConnectionConfig(),
	}
}

// RedisClient routes requests to Redis cluster nodes.
type RedisClient struct {
	locator gyro.Locator
	config  *RedisClientConfig
}

// NewRedisClient creates a client-side sharded Redis cluster client. Each
// address gets its own go-redis connection; routing between them is done
// via consistent hashing.
func NewRedisClient(addresses []string, config *RedisClientConfig) (*RedisClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one Redis address is required")
	}
	if config == nil {
		config = DefaultRedisClientConfig()
	}

	locator, err := gyro.NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection locator: %w", err)
	}

	factory := &RedisNodeFactory{config: config}
	for i, addr := range addresses {
		node, err := factory.CreateNode(gyro.NodeInfo{
			ID:      fmt.Sprintf("redis-%d", i+1),
			Address: addr,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create node for %s: %w", addr, err)
		}
		if err := locator.AddNode(node); err != nil {
			return nil, fmt.Errorf("failed to add node for %s: %w", addr, err)
		}
	}

	return &RedisClient{locator: locator, config: config}, nil
}

func (rc *RedisClient) GetClientForKey(ctx context.Context, key string) (interface{}, error) {
	node, err := rc.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get node for key '%s': %w", key, err)
	}

	redisNode, ok := node.(*RedisNode)
	if !ok {
		return nil, fmt.Errorf("node %s is not a Redis node", node.ID())
	}

	nativeClient := redisNode.GetNativeClient()
	if nativeClient == nil {
		return nil, fmt.Errorf("node %s has no healthy client", node.ID())
	}

	return nativeClient, nil
}

func (rc *RedisClient) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]interface{}, error) {
	nodes, err := rc.locator.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas for key '%s': %w", key, err)
	}

	clients := make([]interface{}, 0, len(nodes))
	for _, node := range nodes {
		redisNode, ok := node.(*RedisNode)
		if !ok {
			continue
		}

		nativeClient := redisNode.GetNativeClient()
		if nativeClient != nil {
			clients = append(clients, nativeClient)
		}
	}

	return clients, nil
}

func (rc *RedisClient) GetAllClients() map[string]interface{} {
	nodes := rc.locator.GetAllNodes()
	clients := make(map[string]interface{})

	for _, node := range nodes {
		redisNode, ok := node.(*RedisNode)
		if !ok {
			continue
		}

		nativeClient := redisNode.GetNativeClient()
		if nativeClient != nil {
			clients[node.ID()] = nativeClient
		}
	}

	return clients
}

// Close closes all connections and releases resources.
func (rc *RedisClient) Close() error {
	return rc.locator.Close()
}

func NewRedisCluster(addresses []string) (*RedisClient, error) {
	return NewRedisClient(addresses, nil)
}

// RedisNodeFactory creates Redis nodes.
type RedisNodeFactory struct {
	config *RedisClientConfig
}

// NewRedisNodeFactory creates a new Redis node factory.
func NewRedisNodeFactory() *RedisNodeFactory {
	return &RedisNodeFactory{
		config: DefaultRedisClientConfig(),
	}
}

// CreateNode creates a new Redis node from NodeInfo.
func (f *RedisNodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	conn, err := NewRedisConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection to %s: %w", info.Address, err)
	}

	return NewRedisNode(info.ID, info.Address, conn), nil
}

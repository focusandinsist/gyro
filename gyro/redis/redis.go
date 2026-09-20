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
	GetNativeClient() any
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
func (c *DefaultRedisConnection) GetNativeClient() any {
	return c.client
}

// RedisNode adapts a Redis connection to the gyro.Node interface.
type RedisNode struct {
	id      string
	address string
	conn    RedisConnection
	mu      sync.RWMutex
	closed  bool
}

func NewRedisNode(id, address string, conn RedisConnection) *RedisNode {
	return &RedisNode{
		id:      id,
		address: address,
		conn:    conn,
	}
}

func (rn *RedisNode) ID() string {
	return rn.id
}

func (rn *RedisNode) Address() string {
	return rn.address
}

func (rn *RedisNode) IsHealthy(ctx context.Context) bool {
	rn.mu.RLock()
	closed := rn.closed
	rn.mu.RUnlock()
	if closed {
		return false
	}

	return rn.conn.Ping(ctx) == nil
}

func (rn *RedisNode) Close() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.closed {
		return nil
	}
	rn.closed = true
	return rn.conn.Close()
}

func (rn *RedisNode) GetNativeClient() any {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	if rn.closed {
		return nil
	}

	return rn.conn.GetNativeClient()
}

type RedisClientConfig struct {
	Locator       gyro.LocatorConfig       `json:"locator"`
	HealthChecker gyro.HealthCheckerConfig `json:"health_checker"`
	Connection    gyro.ConnectionConfig    `json:"connection"`
}

func DefaultRedisClientConfig() *RedisClientConfig {
	return &RedisClientConfig{
		Locator:       gyro.DefaultLocatorConfig(),
		HealthChecker: gyro.DefaultHealthCheckerConfig(),
		Connection:    gyro.DefaultConnectionConfig(),
	}
}

// RedisClient routes requests to Redis cluster nodes.
type RedisClient struct {
	locator gyro.Locator
	config  *RedisClientConfig
	cancel  context.CancelFunc
}

// NewRedisClient creates a client-side sharded Redis cluster client. Each
// address gets its own go-redis connection; routing between them is done
// via consistent hashing.
func NewRedisClient(addresses []string, config *RedisClientConfig) (*RedisClient, error) {
	return newRedisClient(addresses, config, nil, nil)
}

func newRedisClient(addresses []string, config *RedisClientConfig, factory *RedisNodeFactory, healthChecker gyro.HealthChecker) (*RedisClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one Redis address is required")
	}
	if config == nil {
		config = DefaultRedisClientConfig()
	}
	configSnapshot := *config
	config = &configSnapshot
	if err := gyro.ValidateHealthCheckerConfig(config.HealthChecker); err != nil {
		return nil, fmt.Errorf("invalid health checker config: %w", err)
	}

	locator, err := gyro.NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection locator: %w", err)
	}

	if factory == nil {
		factory = &RedisNodeFactory{config: config, newConnection: NewRedisConnection}
	}
	for i, addr := range addresses {
		node, err := factory.CreateNode(gyro.NodeInfo{
			ID:      fmt.Sprintf("redis-%d", i+1),
			Address: addr,
		})
		if err != nil {
			_ = locator.Close()
			return nil, fmt.Errorf("failed to create node for %s: %w", addr, err)
		}
		if err := locator.AddNode(node); err != nil {
			_ = node.Close()
			_ = locator.Close()
			return nil, fmt.Errorf("failed to add node for %s: %w", addr, err)
		}
	}
	if healthChecker == nil {
		healthChecker = gyro.NewDefaultHealthChecker(config.HealthChecker)
	}
	healthAwarePool := gyro.NewHealthAwarePoolWithChecker(locator, healthChecker)
	healthCtx, cancel := context.WithCancel(context.Background())
	healthAwarePool.StartHealthMonitoring(healthCtx)

	return &RedisClient{locator: healthAwarePool, config: config, cancel: cancel}, nil
}

func (rc *RedisClient) GetClientForKey(ctx context.Context, key string) (any, error) {
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

func (rc *RedisClient) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]any, error) {
	nodes, err := rc.locator.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas for key '%s': %w", key, err)
	}

	clients := make([]any, 0, len(nodes))
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

func (rc *RedisClient) GetAllClients() map[string]any {
	nodes := rc.locator.GetAllNodes()
	clients := make(map[string]any)

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
	if rc.cancel != nil {
		rc.cancel()
	}
	return rc.locator.Close()
}

func NewRedisCluster(addresses []string) (*RedisClient, error) {
	return NewRedisClient(addresses, nil)
}

// RedisNodeFactory creates Redis nodes.
type RedisNodeFactory struct {
	config        *RedisClientConfig
	newConnection func(address string, config gyro.ConnectionConfig) (RedisConnection, error)
}

// NewRedisNodeFactory creates a new Redis node factory.
func NewRedisNodeFactory() *RedisNodeFactory {
	return &RedisNodeFactory{
		config:        DefaultRedisClientConfig(),
		newConnection: NewRedisConnection,
	}
}

// WithConnectionConfig returns an independent factory for a new connection
// configuration. The current factory remains unchanged until a Client has
// successfully built and published the replacement locator.
func (f *RedisNodeFactory) WithConnectionConfig(connectionConfig gyro.ConnectionConfig) (gyro.NodeFactory, error) {
	if f == nil || f.config == nil {
		return nil, fmt.Errorf("Redis node factory is not initialized")
	}

	configCopy := *f.config
	configCopy.Connection = connectionConfig
	return &RedisNodeFactory{config: &configCopy, newConnection: f.newConnection}, nil
}

// CreateNode creates a new Redis node from NodeInfo.
func (f *RedisNodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	newConnection := f.newConnection
	if newConnection == nil {
		newConnection = NewRedisConnection
	}
	conn, err := newConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection to %s: %w", info.Address, err)
	}

	return NewRedisNode(info.ID, info.Address, conn), nil
}

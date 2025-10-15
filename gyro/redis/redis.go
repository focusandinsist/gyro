package redis

import (
	"context"
	"fmt"
	"sync"

	"gyro/gyro"
)

// RedisConnection a Redis connection interface.
type RedisConnection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetNativeClient() interface{}
}

// DefaultRedisConnection is a default implementation of RedisConnection.
type DefaultRedisConnection struct {
	address   string
	connected bool
	mu        sync.RWMutex
}

// NewRedisConnection creates a new Redis connection.
func NewRedisConnection(address string, config gyro.ConnectionConfig) (RedisConnection, error) {
	conn := &DefaultRedisConnection{
		address:   address,
		connected: true, // Simulate successful connection for testing
	}
	return conn, nil
}

// Close closes the Redis connection.
func (c *DefaultRedisConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
	return nil
}

// IsConnected returns whether the connection is active.
func (c *DefaultRedisConnection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// Ping tests the connection.
func (c *DefaultRedisConnection) Ping(ctx context.Context) error {
	if !c.IsConnected() {
		return fmt.Errorf("connection is not active")
	}
	return nil
}

// GetNativeClient returns the native Redis client.
func (c *DefaultRedisConnection) GetNativeClient() interface{} {
	return nil // Placeholder for actual Redis client
}

// RedisNode .
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
	rn.mu.RLock()
	if !rn.healthy || !rn.conn.IsConnected() {
		rn.mu.RUnlock()
		return false
	}
	rn.mu.RUnlock()

	if err := rn.conn.Ping(ctx); err != nil {
		rn.mu.Lock()
		rn.healthy = false
		rn.mu.Unlock()
		return false
	}

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

func NewRedisClient(addresses []string, config *RedisClientConfig) (*RedisClient, error) {
	// Redis client requires real Redis connection implementation
	return nil, fmt.Errorf("Redis client requires real Redis connection implementation")
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
	// Create Redis connection
	conn, err := NewRedisConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection to %s: %w", info.Address, err)
	}

	return NewRedisNode(info.ID, info.Address, conn), nil
}

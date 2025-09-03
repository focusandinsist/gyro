package gyro

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RedisConnection a Redis connection interface.
type RedisConnection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetNativeClient() interface{}
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
	Pool       PoolConfig       `json:"pool"`
	Connection ConnectionConfig `json:"connection"`
}

func DefaultRedisClientConfig() *RedisClientConfig {
	return &RedisClientConfig{
		Pool:       DefaultPoolConfig(),
		Connection: DefaultConnectionConfig(),
	}
}

// RedisClient routes requests to Redis cluster nodes.
type RedisClient struct {
	pool   Pool
	config *RedisClientConfig
}

func NewRedisClient(addresses []string, config *RedisClientConfig) (*RedisClient, error) {
	if config == nil {
		config = DefaultRedisClientConfig()
	}

	pool, err := NewConsistentPool(config.Pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	for i, address := range addresses {
		nodeID := fmt.Sprintf("redis-%d", i)

		conn := &MockRedisConnection{
			address:      address,
			connected:    true,
			nativeClient: &MockNativeRedisClient{address: address, data: make(map[string]string)},
		}

		node := NewRedisNode(nodeID, address, conn)
		if err := pool.AddNode(node); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to add Redis node %s: %w", nodeID, err)
		}
	}

	return &RedisClient{
		pool:   pool,
		config: config,
	}, nil
}

func (rc *RedisClient) GetClientForKey(ctx context.Context, key string) (interface{}, error) {
	node, err := rc.pool.Get(ctx, key)
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
	nodes, err := rc.pool.GetReplicas(ctx, key, replicaCount)
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
	nodes := rc.pool.GetAllNodes()
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
	return rc.pool.Close()
}

type MockNativeRedisClient struct {
	address string
	data    map[string]string
	mu      sync.RWMutex
}

func (mnrc *MockNativeRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	mnrc.mu.Lock()
	defer mnrc.mu.Unlock()

	if mnrc.data == nil {
		mnrc.data = make(map[string]string)
	}

	mnrc.data[key] = fmt.Sprintf("%v", value)
	return nil
}

func (mnrc *MockNativeRedisClient) Get(ctx context.Context, key string) (string, error) {
	mnrc.mu.RLock()
	defer mnrc.mu.RUnlock()

	value, exists := mnrc.data[key]
	if !exists {
		return "", fmt.Errorf("redis: nil")
	}
	return value, nil
}

func (mnrc *MockNativeRedisClient) Del(ctx context.Context, keys ...string) (int64, error) {
	mnrc.mu.Lock()
	defer mnrc.mu.Unlock()

	count := int64(0)
	for _, key := range keys {
		if _, exists := mnrc.data[key]; exists {
			delete(mnrc.data, key)
			count++
		}
	}
	return count, nil
}

func (mnrc *MockNativeRedisClient) Ping(ctx context.Context) error {
	return nil
}

type MockRedisConnection struct {
	address      string
	connected    bool
	nativeClient *MockNativeRedisClient
	mu           sync.RWMutex
}

func (mrc *MockRedisConnection) GetNativeClient() interface{} {
	mrc.mu.RLock()
	defer mrc.mu.RUnlock()
	return mrc.nativeClient
}

func (mrc *MockRedisConnection) Ping(ctx context.Context) error {
	if !mrc.connected {
		return fmt.Errorf("connection is not established")
	}
	return nil
}

func (mrc *MockRedisConnection) Close() error {
	mrc.connected = false
	return nil
}

func (mrc *MockRedisConnection) IsConnected() bool {
	return mrc.connected
}

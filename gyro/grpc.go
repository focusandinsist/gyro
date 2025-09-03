package gyro

import (
	"context"
	"fmt"
	"sync"
)

type GRPCConnection interface {
	Close() error
	IsConnected() bool
	GetState() string
	GetNativeClient() interface{}
}

type GRPCNode struct {
	id      string
	address string
	conn    GRPCConnection
	mu      sync.RWMutex
	healthy bool
}

func NewGRPCNode(id, address string, conn GRPCConnection) *GRPCNode {
	return &GRPCNode{
		id:      id,
		address: address,
		conn:    conn,
		healthy: true,
	}
}

func (gn *GRPCNode) ID() string {
	return gn.id
}

func (gn *GRPCNode) Address() string {
	return gn.address
}

func (gn *GRPCNode) IsHealthy(ctx context.Context) bool {
	gn.mu.RLock()
	if !gn.healthy || !gn.conn.IsConnected() {
		gn.mu.RUnlock()
		return false
	}
	gn.mu.RUnlock()

	state := gn.conn.GetState()
	if state != "READY" && state != "IDLE" {
		gn.mu.Lock()
		gn.healthy = false
		gn.mu.Unlock()
		return false
	}

	return true
}

func (gn *GRPCNode) Close() error {
	gn.mu.Lock()
	defer gn.mu.Unlock()

	gn.healthy = false
	return gn.conn.Close()
}

func (gn *GRPCNode) GetNativeClient() interface{} {
	gn.mu.RLock()
	defer gn.mu.RUnlock()

	if !gn.healthy {
		return nil
	}

	return gn.conn.GetNativeClient()
}

type GRPCClientConfig struct {
	Pool       PoolConfig       `json:"pool"`
	Connection ConnectionConfig `json:"connection"`
}

func DefaultGRPCClientConfig() *GRPCClientConfig {
	return &GRPCClientConfig{
		Pool:       DefaultPoolConfig(),
		Connection: DefaultConnectionConfig(),
	}
}

type GRPCClient struct {
	pool   Pool
	config *GRPCClientConfig
}

func NewGRPCClient(addresses []string, config *GRPCClientConfig) (*GRPCClient, error) {
	if config == nil {
		config = DefaultGRPCClientConfig()
	}

	// Create the connection pool
	pool, err := NewConsistentPool(config.Pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create gRPC nodes and add them to the pool
	for i, address := range addresses {
		nodeID := fmt.Sprintf("grpc-%d", i)

		conn := &MockGRPCConnection{
			address:   address,
			connected: true,
			state:     "READY",
		}

		node := NewGRPCNode(nodeID, address, conn)
		if err := pool.AddNode(node); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to add gRPC node %s: %w", nodeID, err)
		}
	}

	return &GRPCClient{
		pool:   pool,
		config: config,
	}, nil
}

// GetClientForKey returns the native gRPC client for the given key.
// Routes the key to the correct gRPC node and returns the native client for direct use.
func (gc *GRPCClient) GetClientForKey(ctx context.Context, key string) (interface{}, error) {
	node, err := gc.pool.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get node for key '%s': %w", key, err)
	}

	grpcNode, ok := node.(*GRPCNode)
	if !ok {
		return nil, fmt.Errorf("gyro: internal error, node %s is not a gRPC node", node.ID())
	}

	nativeClient := grpcNode.GetNativeClient()
	if nativeClient == nil {
		return nil, fmt.Errorf("gyro: node %s has no healthy native client", node.ID())
	}

	return nativeClient, nil
}

// GetClientsForReplicas returns native gRPC clients for replica nodes.
func (gc *GRPCClient) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]interface{}, error) {
	nodes, err := gc.pool.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get replicas for key '%s': %w", key, err)
	}

	clients := make([]interface{}, 0, len(nodes))
	for _, node := range nodes {
		grpcNode, ok := node.(*GRPCNode)
		if !ok {
			continue // Skip non-gRPC nodes
		}

		nativeClient := grpcNode.GetNativeClient()
		if nativeClient != nil {
			clients = append(clients, nativeClient)
		}
	}

	return clients, nil
}

// GetAllClients returns native gRPC clients for all nodes.
func (gc *GRPCClient) GetAllClients() map[string]interface{} {
	nodes := gc.pool.GetAllNodes()
	clients := make(map[string]interface{})

	for _, node := range nodes {
		grpcNode, ok := node.(*GRPCNode)
		if !ok {
			continue // Skip non-gRPC nodes
		}

		nativeClient := grpcNode.GetNativeClient()
		if nativeClient != nil {
			clients[node.ID()] = nativeClient
		}
	}

	return clients
}

// Close closes all connections and releases resources.
func (gc *GRPCClient) Close() error {
	return gc.pool.Close()
}

type MockGRPCConnection struct {
	address   string
	connected bool
	state     string
	mu        sync.RWMutex
}

func (mgc *MockGRPCConnection) GetNativeClient() interface{} {
	mgc.mu.RLock()
	defer mgc.mu.RUnlock()
	// In real implementation, this would return *grpc.ClientConn
	return mgc
}

// Close closes the connection.
func (mgc *MockGRPCConnection) Close() error {
	mgc.mu.Lock()
	defer mgc.mu.Unlock()

	mgc.connected = false
	mgc.state = "SHUTDOWN"
	return nil
}

// IsConnected returns true if the connection is established.
func (mgc *MockGRPCConnection) IsConnected() bool {
	mgc.mu.RLock()
	defer mgc.mu.RUnlock()
	return mgc.connected
}

// GetState returns the current connection state.
func (mgc *MockGRPCConnection) GetState() string {
	mgc.mu.RLock()
	defer mgc.mu.RUnlock()
	return mgc.state
}

func NewGRPCCluster(addresses []string) (*GRPCClient, error) {
	return NewGRPCClient(addresses, nil)
}

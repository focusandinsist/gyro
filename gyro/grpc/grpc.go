package grpc

import (
	"context"
	"fmt"
	"sync"

	"gyro/gyro"
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
	Locator    gyro.LocatorConfig    `json:"locator"`
	Connection gyro.ConnectionConfig `json:"connection"`
}

func DefaultGRPCClientConfig() *GRPCClientConfig {
	return &GRPCClientConfig{
		Locator:    gyro.DefaultLocatorConfig(),
		Connection: gyro.DefaultConnectionConfig(),
	}
}

type GRPCClient struct {
	locator gyro.Locator
	config  *GRPCClientConfig
}

func NewGRPCClient(addresses []string, config *GRPCClientConfig) (*GRPCClient, error) {
	if config == nil {
		config = DefaultGRPCClientConfig()
	}

	// Create the connection locator
	locator, err := gyro.NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection locator: %w", err)
	}

	// Create gRPC nodes and add them to the locator
	// Note: This requires real gRPC connection implementation
	return nil, fmt.Errorf("gRPC client requires real gRPC connection implementation")

	return &GRPCClient{
		locator: locator,
		config:  config,
	}, nil
}

// GetClientForKey returns the native gRPC client for the given key.
// Routes the key to the correct gRPC node and returns the native client for direct use.
func (gc *GRPCClient) GetClientForKey(ctx context.Context, key string) (interface{}, error) {
	node, err := gc.locator.Get(ctx, key)
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
	nodes, err := gc.locator.GetReplicas(ctx, key, replicaCount)
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
	nodes := gc.locator.GetAllNodes()
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
	return gc.locator.Close()
}

func NewGRPCCluster(addresses []string) (*GRPCClient, error) {
	return NewGRPCClient(addresses, nil)
}

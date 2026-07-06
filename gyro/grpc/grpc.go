package grpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"gyro/gyro"
)

type GRPCConnection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetState() string
	GetNativeClient() any
}

// DefaultGRPCConnection wraps a real *grpc.ClientConn.
type DefaultGRPCConnection struct {
	address   string
	conn      *grpc.ClientConn
	connected atomic.Bool
}

// NewGRPCConnection creates a new gRPC connection. grpc.NewClient does not
// dial eagerly, so the connection is established lazily on first RPC / health
// check. TLS is not configured here (insecure transport); if the backend
// requires TLS, extend gyro.ConnectionConfig with credential options.
func NewGRPCConnection(address string, config gyro.ConnectionConfig) (GRPCConnection, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client for %s: %w", address, err)
	}

	c := &DefaultGRPCConnection{
		address: address,
		conn:    conn,
	}
	c.connected.Store(true) // optimistic; Ping() will correct this

	return c, nil
}

// Close closes the gRPC connection.
func (c *DefaultGRPCConnection) Close() error {
	c.connected.Store(false)
	return c.conn.Close()
}

// IsConnected returns the last known connectivity state (cheap, no I/O).
// Call Ping to actually verify and refresh this state.
func (c *DefaultGRPCConnection) IsConnected() bool {
	return c.connected.Load()
}

// GetState returns the current gRPC connectivity state.
func (c *DefaultGRPCConnection) GetState() string {
	return c.conn.GetState().String()
}

// Ping verifies the connection is usable. It uses the standard
// grpc.health.v1.Health service (https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
// If the target does not implement that service (codes.Unimplemented), Ping
// falls back to the raw connectivity state so services that haven't wired up
// health checking aren't unnecessarily marked unhealthy.
func (c *DefaultGRPCConnection) Ping(ctx context.Context) error {
	client := healthpb.NewHealthClient(c.conn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err == nil {
		healthy := resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
		c.connected.Store(healthy)
		if !healthy {
			return fmt.Errorf("grpc target %s reported status %s", c.address, resp.GetStatus())
		}
		return nil
	}

	if status.Code(err) == codes.Unimplemented {
		state := c.conn.GetState()
		healthy := state == connectivity.Ready || state == connectivity.Idle
		c.connected.Store(healthy)
		if !healthy {
			return fmt.Errorf("grpc target %s connectivity state is %s", c.address, state)
		}
		return nil
	}

	c.connected.Store(false)
	return fmt.Errorf("grpc health check to %s failed: %w", c.address, err)
}

// GetNativeClient returns the underlying *grpc.ClientConn. Callers create
// their own generated service stubs from it, e.g. pb.NewUserServiceClient(conn).
func (c *DefaultGRPCConnection) GetNativeClient() any {
	return c.conn
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
	if err := gn.conn.Ping(ctx); err != nil {
		gn.mu.Lock()
		gn.healthy = false
		gn.mu.Unlock()
		return false
	}

	gn.mu.Lock()
	gn.healthy = true
	gn.mu.Unlock()
	return true
}

func (gn *GRPCNode) Close() error {
	gn.mu.Lock()
	defer gn.mu.Unlock()

	gn.healthy = false
	return gn.conn.Close()
}

func (gn *GRPCNode) GetNativeClient() any {
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

// NewGRPCClient creates a client-side sharded gRPC cluster client. Each
// address gets its own *grpc.ClientConn; routing between them is done via
// consistent hashing.
func NewGRPCClient(addresses []string, config *GRPCClientConfig) (*GRPCClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one gRPC address is required")
	}
	if config == nil {
		config = DefaultGRPCClientConfig()
	}

	locator, err := gyro.NewConsistentLocator(config.Locator)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection locator: %w", err)
	}

	factory := &GRPCNodeFactory{config: config}
	for i, addr := range addresses {
		node, err := factory.CreateNode(gyro.NodeInfo{
			ID:      fmt.Sprintf("grpc-%d", i+1),
			Address: addr,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create node for %s: %w", addr, err)
		}
		if err := locator.AddNode(node); err != nil {
			return nil, fmt.Errorf("failed to add node for %s: %w", addr, err)
		}
	}

	return &GRPCClient{
		locator: locator,
		config:  config,
	}, nil
}

// GetClientForKey returns the native gRPC client for the given key.
// Routes the key to the correct gRPC node and returns the native client for direct use.
func (gc *GRPCClient) GetClientForKey(ctx context.Context, key string) (any, error) {
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
func (gc *GRPCClient) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]any, error) {
	nodes, err := gc.locator.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get replicas for key '%s': %w", key, err)
	}

	clients := make([]any, 0, len(nodes))
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
func (gc *GRPCClient) GetAllClients() map[string]any {
	nodes := gc.locator.GetAllNodes()
	clients := make(map[string]any)

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

// GRPCNodeFactory creates gRPC nodes.
type GRPCNodeFactory struct {
	config *GRPCClientConfig
}

// NewGRPCNodeFactory creates a new gRPC node factory.
func NewGRPCNodeFactory() *GRPCNodeFactory {
	return &GRPCNodeFactory{
		config: DefaultGRPCClientConfig(),
	}
}

// CreateNode creates a new gRPC node from NodeInfo.
func (f *GRPCNodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	conn, err := NewGRPCConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", info.Address, err)
	}

	return NewGRPCNode(info.ID, info.Address, conn), nil
}

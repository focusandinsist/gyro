// Package grpc provides the gRPC protocol adapter and convenience client for
// Gyro's health-aware consistent-hash routing.
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
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"gyro"
	"gyro/internal/policy"
	"gyro/internal/routed"
	"gyro/internal/routing"
)

type Connection interface {
	Ping(ctx context.Context) error
	Close() error
	IsConnected() bool
	GetState() string
	GetNativeClient() *grpc.ClientConn
}

// DefaultConnection wraps a real *grpc.ClientConn.
type DefaultConnection struct {
	address   string
	conn      *grpc.ClientConn
	config    gyro.ConnectionConfig
	connected atomic.Bool
}

// NewConnection creates a gRPC connection and applies the connection
// timeouts to health-check dialing and RPC contexts. gRPC multiplexes streams over one
// transport, so MaxActiveConns and MaxIdleConns do not map to a connection
// pool and are intentionally not used.
func NewConnection(address string, config gyro.ConnectionConfig) (Connection, error) {
	if config.ConnectTimeout < 0 || config.ReadTimeout < 0 || config.WriteTimeout < 0 || config.IdleTimeout < 0 {
		return nil, fmt.Errorf("gRPC connection timeouts cannot be negative")
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    config.IdleTimeout,
			Timeout: config.WriteTimeout,
		}),
		grpc.WithUnaryInterceptor(timeoutUnaryInterceptor(config)),
		grpc.WithStreamInterceptor(timeoutStreamInterceptor(config)),
	}
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client for %s: %w", address, err)
	}

	c := &DefaultConnection{
		address: address,
		conn:    conn,
		config:  config,
	}
	c.connected.Store(true) // optimistic; Ping() will correct this

	return c, nil
}

func timeoutUnaryInterceptor(config gyro.ConnectionConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		timeout := config.ReadTimeout
		if timeout <= 0 || (config.ConnectTimeout > 0 && config.ConnectTimeout < timeout) {
			timeout = config.ConnectTimeout
		}
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return invoker(ctx, method, request, reply, cc, options...)
	}
}

func timeoutStreamInterceptor(config gyro.ConnectionConfig) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		timeout := config.ReadTimeout
		if timeout <= 0 || (config.WriteTimeout > 0 && config.WriteTimeout < timeout) {
			timeout = config.WriteTimeout
		}
		if timeout <= 0 {
			return streamer(ctx, desc, cc, method, options...)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		stream, err := streamer(ctx, desc, cc, method, options...)
		if err != nil {
			cancel()
			return nil, err
		}
		return &cancelingClientStream{ClientStream: stream, cancel: cancel}, nil
	}
}

type cancelingClientStream struct {
	grpc.ClientStream
	cancel context.CancelFunc
}

func (s *cancelingClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	s.cancel()
	return err
}

// Close closes the gRPC connection.
func (c *DefaultConnection) Close() error {
	c.connected.Store(false)
	return c.conn.Close()
}

// IsConnected returns the last known connectivity state (cheap, no I/O).
// Call Ping to actually verify and refresh this state.
func (c *DefaultConnection) IsConnected() bool {
	return c.connected.Load()
}

// GetState returns the current gRPC connectivity state.
func (c *DefaultConnection) GetState() string {
	return c.conn.GetState().String()
}

// Ping verifies the connection is usable. It uses the standard
// grpc.health.v1.Health service (https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
// If the target does not implement that service (codes.Unimplemented), Ping
// falls back to the raw connectivity state so services that haven't wired up
// health checking aren't unnecessarily marked unhealthy.
func (c *DefaultConnection) Ping(ctx context.Context) error {
	if c.config.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.ConnectTimeout)
		defer cancel()
	}
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
func (c *DefaultConnection) GetNativeClient() *grpc.ClientConn {
	return c.conn
}

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

func (gn *Node) ID() string {
	return gn.id
}

func (gn *Node) Address() string {
	return gn.address
}

func (gn *Node) IsHealthy(ctx context.Context) bool {
	gn.mu.RLock()
	closed := gn.closed
	gn.mu.RUnlock()
	if closed {
		return false
	}

	return gn.conn.Ping(ctx) == nil
}

func (gn *Node) Close() error {
	gn.mu.Lock()
	defer gn.mu.Unlock()

	if gn.closed {
		return nil
	}
	gn.closed = true
	return gn.conn.Close()
}

func (gn *Node) GetNativeClient() *grpc.ClientConn {
	gn.mu.RLock()
	defer gn.mu.RUnlock()

	if gn.closed {
		return nil
	}

	return gn.conn.GetNativeClient()
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

type Client struct {
	locator gyro.Locator
	config  *ClientConfig
	runtime *routed.Runtime
}

// ClientLease keeps a routed gRPC connection alive until Release is called.
type ClientLease struct {
	conn *grpc.ClientConn
	node *routing.NodeLease
}

func (l *ClientLease) Client() *grpc.ClientConn { return l.conn }
func (l *ClientLease) Release() error           { return l.node.Release() }

func (gc *Client) BorrowClientForKey(ctx context.Context, key string) (*ClientLease, error) {
	lease, err := gc.runtime.BorrowNodeForKey(ctx, key)
	if err != nil {
		return nil, err
	}
	node, ok := lease.Node().(*Node)
	if !ok {
		_ = lease.Release()
		return nil, fmt.Errorf("routed node is not a gRPC node")
	}
	conn := node.GetNativeClient()
	if conn == nil {
		_ = lease.Release()
		return nil, fmt.Errorf("node %s has no healthy native client", node.ID())
	}
	return &ClientLease{conn: conn, node: lease}, nil
}

// NewClient creates a client-side sharded gRPC cluster client. Each
// address gets its own *grpc.ClientConn; routing between them is done via
// consistent hashing.
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
	runtime, err := routed.NewFixedWithPolicy(addresses, config.RoutingConfig, "grpc", factory.CreateNode, healthChecker, policy.HealthyCandidate{AllowUnknown: true})
	if err != nil {
		return nil, err
	}
	return &Client{locator: runtime.Locator(), config: config, runtime: runtime}, nil
}

// GetClientForKey returns the current native connection. Use BorrowClientForKey
// when it must remain open across a topology change.
func (gc *Client) GetClientForKey(ctx context.Context, key string) (*grpc.ClientConn, error) {
	lease, err := gc.BorrowClientForKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get client for key '%s': %w", key, err)
	}
	defer lease.Release()
	return lease.Client(), nil
}

// GetGRPCConnForKey returns the typed gRPC resource for a routed key.
func (gc *Client) GetGRPCConnForKey(ctx context.Context, key string) (*grpc.ClientConn, error) {
	return gc.GetClientForKey(ctx, key)
}

// GetNodeForKey returns the routed node metadata for observability and tests.
func (gc *Client) GetNodeForKey(ctx context.Context, key string) (gyro.Node, error) {
	node, err := gc.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get node for key '%s': %w", key, err)
	}
	return node, nil
}

// GetClientsForReplicas returns current candidates without health filtering
// or leases. They may close when membership changes.
func (gc *Client) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]*grpc.ClientConn, error) {
	nodes, err := gc.locator.GetReplicas(ctx, key, replicaCount)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get replicas for key '%s': %w", key, err)
	}
	clients := make([]*grpc.ClientConn, 0, len(nodes))
	for _, node := range nodes {
		if grpcNode, ok := node.(*Node); ok {
			if client := grpcNode.GetNativeClient(); client != nil {
				clients = append(clients, client)
			}
		}
	}
	return clients, nil
}

// GetAllClients returns a snapshot of current native connections.
func (gc *Client) GetAllClients() map[string]*grpc.ClientConn {
	clients := make(map[string]*grpc.ClientConn)
	for _, node := range gc.locator.GetAllNodes() {
		if grpcNode, ok := node.(*Node); ok {
			if client := grpcNode.GetNativeClient(); client != nil {
				clients[node.ID()] = client
			}
		}
	}
	return clients
}

// Close closes all connections and releases resources.
func (gc *Client) Close() error {
	return gc.runtime.Close()
}

func NewCluster(addresses []string) (*Client, error) {
	return NewClient(addresses, nil)
}

// NodeFactory creates gRPC nodes.
type NodeFactory struct {
	config        *ClientConfig
	newConnection func(address string, config gyro.ConnectionConfig) (Connection, error)
}

// NewNodeFactory creates a new gRPC node factory.
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
		return nil, fmt.Errorf("gRPC node factory is not initialized")
	}

	configCopy := *f.config
	configCopy.Connection = connectionConfig
	return &NodeFactory{config: &configCopy, newConnection: f.newConnection}, nil
}

// CreateNode creates a new gRPC node from NodeInfo.
func (f *NodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	newConnection := f.newConnection
	if newConnection == nil {
		newConnection = NewConnection
	}
	conn, err := newConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", info.Address, err)
	}

	return NewNode(info.ID, info.Address, conn), nil
}

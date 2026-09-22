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

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/routed"
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
	config    gyro.ConnectionConfig
	connected atomic.Bool
}

// NewGRPCConnection creates a gRPC connection and applies the connection
// timeouts to health-check dialing and RPC contexts. gRPC multiplexes streams over one
// transport, so MaxActiveConns and MaxIdleConns do not map to a connection
// pool and are intentionally not used.
func NewGRPCConnection(address string, config gyro.ConnectionConfig) (GRPCConnection, error) {
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

	c := &DefaultGRPCConnection{
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
func (c *DefaultGRPCConnection) GetNativeClient() any {
	return c.conn
}

type GRPCNode struct {
	id      string
	address string
	conn    GRPCConnection
	mu      sync.RWMutex
	closed  bool
}

func NewGRPCNode(id, address string, conn GRPCConnection) *GRPCNode {
	return &GRPCNode{
		id:      id,
		address: address,
		conn:    conn,
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
	closed := gn.closed
	gn.mu.RUnlock()
	if closed {
		return false
	}

	return gn.conn.Ping(ctx) == nil
}

func (gn *GRPCNode) Close() error {
	gn.mu.Lock()
	defer gn.mu.Unlock()

	if gn.closed {
		return nil
	}
	gn.closed = true
	return gn.conn.Close()
}

func (gn *GRPCNode) GetNativeClient() any {
	gn.mu.RLock()
	defer gn.mu.RUnlock()

	if gn.closed {
		return nil
	}

	return gn.conn.GetNativeClient()
}

type GRPCClientConfig struct {
	Locator       gyro.LocatorConfig       `json:"locator"`
	HealthChecker gyro.HealthCheckerConfig `json:"health_checker"`
	Connection    gyro.ConnectionConfig    `json:"connection"`
}

func DefaultGRPCClientConfig() *GRPCClientConfig {
	return &GRPCClientConfig{
		Locator:       gyro.DefaultLocatorConfig(),
		HealthChecker: gyro.DefaultHealthCheckerConfig(),
		Connection:    gyro.DefaultConnectionConfig(),
	}
}

type GRPCClient struct {
	locator gyro.Locator
	config  *GRPCClientConfig
	runtime *routed.Runtime
}

// NewGRPCClient creates a client-side sharded gRPC cluster client. Each
// address gets its own *grpc.ClientConn; routing between them is done via
// consistent hashing.
func NewGRPCClient(addresses []string, config *GRPCClientConfig) (*GRPCClient, error) {
	return newGRPCClient(addresses, config, nil, nil)
}

func newGRPCClient(addresses []string, config *GRPCClientConfig, factory *GRPCNodeFactory, healthChecker gyro.HealthChecker) (*GRPCClient, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one gRPC address is required")
	}
	if config == nil {
		config = DefaultGRPCClientConfig()
	}
	configSnapshot := *config
	config = &configSnapshot
	if err := gyro.ValidateHealthCheckerConfig(config.HealthChecker); err != nil {
		return nil, fmt.Errorf("invalid health checker config: %w", err)
	}

	if factory == nil {
		factory = &GRPCNodeFactory{config: config, newConnection: NewGRPCConnection}
	}
	runtime, err := routed.New(addresses, config.Locator, config.HealthChecker, "grpc", factory.CreateNode, healthChecker)
	if err != nil {
		return nil, err
	}
	return &GRPCClient{locator: runtime.Locator(), config: config, runtime: runtime}, nil
}

// GetClientForKey returns the native gRPC client for the given key.
// Routes the key to the correct gRPC node and returns the native client for direct use.
func (gc *GRPCClient) GetClientForKey(ctx context.Context, key string) (any, error) {
	node, err := gc.GetNodeForKey(ctx, key)
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

// GetNodeForKey returns the routed node metadata for observability and tests.
func (gc *GRPCClient) GetNodeForKey(ctx context.Context, key string) (gyro.Node, error) {
	node, err := gc.locator.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get node for key '%s': %w", key, err)
	}
	return node, nil
}

// GetClientsForReplicas returns native gRPC clients for replica nodes.
func (gc *GRPCClient) GetClientsForReplicas(ctx context.Context, key string, replicaCount int) ([]any, error) {
	clients, err := gc.runtime.Replicas(ctx, key, replicaCount, grpcNativeClient)
	if err != nil {
		return nil, fmt.Errorf("gyro: failed to get replicas for key '%s': %w", key, err)
	}
	return clients, nil
}

// GetAllClients returns native gRPC clients for all nodes.
func (gc *GRPCClient) GetAllClients() map[string]any {
	return gc.runtime.All(grpcNativeClient)
}

func grpcNativeClient(node gyro.Node) (any, bool) {
	grpcNode, ok := node.(*GRPCNode)
	if !ok {
		return nil, false
	}
	return grpcNode.GetNativeClient(), true
}

// Close closes all connections and releases resources.
func (gc *GRPCClient) Close() error {
	return gc.runtime.Close()
}

func NewGRPCCluster(addresses []string) (*GRPCClient, error) {
	return NewGRPCClient(addresses, nil)
}

// GRPCNodeFactory creates gRPC nodes.
type GRPCNodeFactory struct {
	config        *GRPCClientConfig
	newConnection func(address string, config gyro.ConnectionConfig) (GRPCConnection, error)
}

// NewGRPCNodeFactory creates a new gRPC node factory.
func NewGRPCNodeFactory() *GRPCNodeFactory {
	return &GRPCNodeFactory{
		config:        DefaultGRPCClientConfig(),
		newConnection: NewGRPCConnection,
	}
}

// WithConnectionConfig returns an independent factory for a new connection
// configuration. The current factory remains unchanged until a Client has
// successfully built and published the replacement locator.
func (f *GRPCNodeFactory) WithConnectionConfig(connectionConfig gyro.ConnectionConfig) (gyro.NodeFactory, error) {
	if f == nil || f.config == nil {
		return nil, fmt.Errorf("gRPC node factory is not initialized")
	}

	configCopy := *f.config
	configCopy.Connection = connectionConfig
	return &GRPCNodeFactory{config: &configCopy, newConnection: f.newConnection}, nil
}

// CreateNode creates a new gRPC node from NodeInfo.
func (f *GRPCNodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	newConnection := f.newConnection
	if newConnection == nil {
		newConnection = NewGRPCConnection
	}
	conn, err := newConnection(info.Address, f.config.Connection)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to %s: %w", info.Address, err)
	}

	return NewGRPCNode(info.ID, info.Address, conn), nil
}

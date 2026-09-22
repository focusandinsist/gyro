package routed

import (
	"context"
	"fmt"
	"sync"

	"github.com/focusandinsist/gyro/gyro"
)

// Runtime owns the shared lifecycle of a routed adapter: locator construction,
// node rollback, health monitoring, and shutdown. Protocol packages retain
// ownership of their connection and native-client types.
type Runtime struct {
	locator gyro.Locator
	pool    *gyro.HealthAwarePool
	cancel  context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// New builds a routed runtime from protocol-specific node creation logic.
func New(addresses []string, locatorConfig gyro.LocatorConfig, healthConfig gyro.HealthCheckerConfig, idPrefix string, create func(gyro.NodeInfo) (gyro.Node, error), checker gyro.HealthChecker) (*Runtime, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one %s address is required", idPrefix)
	}
	if create == nil {
		return nil, fmt.Errorf("node creator cannot be nil")
	}
	base, err := gyro.NewConsistentLocator(locatorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection locator: %w", err)
	}
	for i, address := range addresses {
		node, err := create(gyro.NodeInfo{ID: fmt.Sprintf("%s-%d", idPrefix, i+1), Address: address})
		if err != nil {
			_ = base.Close()
			return nil, fmt.Errorf("failed to create node for %s: %w", address, err)
		}
		if err := base.AddNodeContext(context.Background(), node); err != nil {
			_ = node.Close()
			_ = base.Close()
			return nil, fmt.Errorf("failed to add node for %s: %w", address, err)
		}
	}
	if checker == nil {
		checker = gyro.NewDefaultHealthChecker(healthConfig)
	}
	pool := gyro.NewHealthAwarePoolWithChecker(base, checker)
	healthCtx, cancel := context.WithCancel(context.Background())
	pool.StartHealthMonitoring(healthCtx)
	return &Runtime{locator: pool, pool: pool, cancel: cancel}, nil
}

// Locator returns the health-aware routed locator.
func (r *Runtime) Locator() gyro.Locator { return r.locator }

// Pool returns the health-aware pool for adapter-specific operations.
func (r *Runtime) Pool() *gyro.HealthAwarePool { return r.pool }

// Replicas selects candidates and converts supported nodes to native clients.
func (r *Runtime) Replicas(ctx context.Context, key string, count int, native func(gyro.Node) (any, bool)) ([]any, error) {
	nodes, err := r.locator.GetReplicas(ctx, key, count)
	if err != nil {
		return nil, err
	}
	clients := make([]any, 0, len(nodes))
	for _, node := range nodes {
		if client, ok := native(node); ok && client != nil {
			clients = append(clients, client)
		}
	}
	return clients, nil
}

// All converts all supported nodes to native clients keyed by node ID.
func (r *Runtime) All(native func(gyro.Node) (any, bool)) map[string]any {
	clients := make(map[string]any)
	for _, node := range r.locator.GetAllNodes() {
		if client, ok := native(node); ok && client != nil {
			clients[node.ID()] = client
		}
	}
	return clients
}

// Close stops health monitoring and closes all routed nodes once.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		if r.pool != nil {
			r.closeErr = r.pool.Close()
		}
	})
	return r.closeErr
}

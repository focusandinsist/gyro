package routed

import (
	"context"
	"fmt"
	"sync"

	"gyro"
	"gyro/internal/health"
	"gyro/internal/policy"
	"gyro/internal/routing"
)

// Runtime owns the shared lifecycle of a routed adapter: locator construction,
// node rollback, health monitoring, and shutdown. Protocol packages retain
// ownership of their connection and native-client types.
type Runtime struct {
	locator gyro.Locator
	pool    *routing.HealthAwarePool
	cancel  context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// New builds a routed runtime from protocol-specific node creation logic.
func New(addresses []string, locatorConfig gyro.LocatorConfig, healthConfig gyro.HealthCheckerConfig, idPrefix string, create func(gyro.NodeInfo) (gyro.Node, error), checker gyro.HealthChecker) (*Runtime, error) {
	return NewWithPolicy(addresses, locatorConfig, healthConfig, idPrefix, create, checker, policy.PrimaryOnly{})
}

// NewWithPolicy makes adapter failover semantics explicit at construction.
func NewWithPolicy(addresses []string, locatorConfig gyro.LocatorConfig, healthConfig gyro.HealthCheckerConfig, idPrefix string, create func(gyro.NodeInfo) (gyro.Node, error), checker gyro.HealthChecker, policy gyro.FailurePolicy) (*Runtime, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one %s address is required", idPrefix)
	}
	if create == nil {
		return nil, fmt.Errorf("node creator cannot be nil")
	}
	base, err := routing.NewLocator(locatorConfig)
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
		checker = health.NewDefaultHealthChecker(healthConfig)
	}
	pool := routing.NewHealthAwarePoolWithCheckerAndPolicy(base, checker, policy)
	healthCtx, cancel := context.WithCancel(context.Background())
	pool.StartHealthMonitoring(healthCtx)
	return &Runtime{locator: pool, pool: pool, cancel: cancel}, nil
}

// Locator returns the health-aware routed locator.
func (r *Runtime) Locator() gyro.Locator { return r.locator }

func (r *Runtime) BorrowNodeForKey(ctx context.Context, key string) (*routing.NodeLease, error) {
	return r.pool.BorrowNodeForKey(ctx, key)
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

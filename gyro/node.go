package gyro

import "context"

// NodeFactory creates nodes from NodeInfo.
type NodeFactory interface {
	CreateNode(info NodeInfo) (Node, error)
}

// ConnectionConfigurableNodeFactory creates a node factory for a new
// connection configuration without mutating the factory used by the current
// runtime.
type ConnectionConfigurableNodeFactory interface {
	NodeFactory
	WithConnectionConfig(config ConnectionConfig) (NodeFactory, error)
}

// Node is the minimum backend abstraction required by routing and health.
type Node interface {
	ID() string
	Address() string
	IsHealthy(ctx context.Context) bool
	Close() error
}

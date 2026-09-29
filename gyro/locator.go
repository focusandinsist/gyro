package gyro

import (
	"context"
	"errors"
)

// Locator is the transitional node-routing surface used by runtime clients.
// GetReplicas returns candidates in selector order; its first element must
// equal Get for the same key and unchanged membership. Close releases the
// resources owned by its implementation.
type Locator interface {
	Get(context.Context, string) (Node, error)
	GetReplicas(context.Context, string, int) ([]Node, error)
	AddNodeContext(context.Context, Node) error
	RemoveNodeContext(context.Context, string) error
	GetAllNodes() []Node
	Close() error
}

var ErrLocatorClosed = errors.New("locator is closed")

// LocatorConfig controls consistent-hash candidate ordering.
type LocatorConfig struct {
	PartitionCount    int     `json:"partition_count"`
	ReplicationFactor int     `json:"replication_factor"`
	Load              float64 `json:"load"`
	HashFunction      string  `json:"hash_function"`
}

func DefaultLocatorConfig() LocatorConfig {
	return LocatorConfig{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		HashFunction:      "xxhash",
	}
}

type LocatorStats struct {
	TotalNodes int `json:"total_nodes"`
}

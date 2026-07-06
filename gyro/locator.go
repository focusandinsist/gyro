package gyro

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/focusandinsist/consistent-go/consistent"
)

type Node interface {
	ID() string
	Address() string
	IsHealthy(ctx context.Context) bool
	Close() error
}

type Locator interface {
	Get(ctx context.Context, key string) (Node, error)
	GetReplicas(ctx context.Context, key string, count int) ([]Node, error)
	AddNode(node Node) error
	RemoveNode(nodeID string) error
	GetAllNodes() []Node
	Close() error
}

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

type ConsistentLocator struct {
	mu     sync.RWMutex
	nodes  map[string]Node
	ring   *consistent.Consistent
	config LocatorConfig
	logger atomic.Pointer[slog.Logger]
}

func NewConsistentLocator(config LocatorConfig) (*ConsistentLocator, error) {
	var hasher consistent.Hasher
	switch config.HashFunction {
	case "", "xxhash":
		hasher = consistent.NewXXHasher()
	case "murmur3":
		hasher = consistent.NewMurmurHash3Hasher()
	default:
		return nil, fmt.Errorf("unknown hash function: %s", config.HashFunction)
	}

	consistentConfig := consistent.Config{
		Hasher:            hasher,
		PartitionCount:    config.PartitionCount,
		ReplicationFactor: config.ReplicationFactor,
		Load:              config.Load,
	}

	ring, err := consistent.New(consistentConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consistent hash ring: %w", err)
	}

	cl := &ConsistentLocator{
		nodes:  make(map[string]Node),
		ring:   ring,
		config: config,
	}
	cl.logger.Store(discardLogger)

	return cl, nil
}

// SetLogger overrides the logger used for internal diagnostics. Passing nil
// restores the default no-op logger.
func (cl *ConsistentLocator) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardLogger
	}
	cl.logger.Store(logger)
}

func (cl *ConsistentLocator) log() *slog.Logger {
	return cl.logger.Load()
}

// Get retrieves a node for the given key using consistent hashing.
// Note: this only performs location, not health checking - that's
// HealthAwarePool's responsibility.
func (cl *ConsistentLocator) Get(ctx context.Context, key string) (Node, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	if len(cl.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available in ring")
	}

	nodeID, err := cl.ring.LocateKey(ctx, []byte(key))
	if err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, fmt.Errorf("failed to locate node for key: %s", key)
	}

	node, exists := cl.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found in ring", nodeID)
	}

	return node, nil
}

// GetReplicas returns N nodes closest to the key for replication.
func (cl *ConsistentLocator) GetReplicas(ctx context.Context, key string, count int) ([]Node, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	if len(cl.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available in locator")
	}

	if count <= 0 {
		return []Node{}, nil
	}

	nodeIDs, err := cl.ring.LocateReplicas(ctx, []byte(key), count)
	if err != nil {
		return nil, fmt.Errorf("failed to locate replicas for key %s: %w", key, err)
	}

	replicas := make([]Node, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if node, exists := cl.nodes[nodeID]; exists {
			replicas = append(replicas, node)
		}
	}

	return replicas, nil
}

// AddNode adds a new node to the locator.
func (cl *ConsistentLocator) AddNode(node Node) error {
	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}

	nodeID := node.ID()
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	if _, exists := cl.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in locator", nodeID)
	}

	if err := cl.ring.Add(context.Background(), nodeID); err != nil {
		return fmt.Errorf("failed to add node %s to consistent hash ring: %w", nodeID, err)
	}

	cl.nodes[nodeID] = node

	return nil
}

// RemoveNode removes a node from the locator.
func (cl *ConsistentLocator) RemoveNode(nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	var nodeToClose Node
	func() {
		cl.mu.Lock()
		defer cl.mu.Unlock()

		node, exists := cl.nodes[nodeID]
		if !exists {
			return
		}

		if err := cl.ring.Remove(context.Background(), nodeID); err != nil {
			cl.log().Warn("failed to remove node from consistent hash ring", "node_id", nodeID, "error", err)
		}

		delete(cl.nodes, nodeID)
		nodeToClose = node
	}()

	if nodeToClose == nil {
		return fmt.Errorf("node %s not found in locator", nodeID)
	}

	// Close outside the lock so a slow Close() doesn't block other locator operations.
	if err := nodeToClose.Close(); err != nil {
		cl.log().Warn("failed to close node", "node_id", nodeID, "error", err)
	}

	return nil
}

// GetAllNodes returns all nodes in the locator.
func (cl *ConsistentLocator) GetAllNodes() []Node {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	nodes := make([]Node, 0, len(cl.nodes))
	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// Close closes all connections and releases resources.
func (cl *ConsistentLocator) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	var lastErr error
	for nodeID, node := range cl.nodes {
		if err := node.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close node %s: %w", nodeID, err)
		}
	}

	cl.nodes = make(map[string]Node)

	return lastErr
}

// GetStats returns statistics about the locator.
// Note: health status is not included - that's HealthChecker/HealthAwarePool's responsibility.
func (cl *ConsistentLocator) GetStats() LocatorStats {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	return LocatorStats{
		TotalNodes: len(cl.nodes),
	}
}

// LocatorStats contains statistics about a locator.
type LocatorStats struct {
	TotalNodes int `json:"total_nodes"`
}

package gyro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/focusandinsist/consistent-go/consistent"
)

// Locator routes keys to nodes and owns node membership and shutdown.
type Locator interface {
	Get(ctx context.Context, key string) (Node, error)
	GetReplicas(ctx context.Context, key string, count int) ([]Node, error)
	AddNodeContext(ctx context.Context, node Node) error
	RemoveNodeContext(ctx context.Context, nodeID string) error
	GetAllNodes() []Node
	Close() error
}

var ErrLocatorClosed = errors.New("locator is closed")

// LocatorConfig controls the consistent-hash ring layout.
type LocatorConfig struct {
	PartitionCount    int     `json:"partition_count"`
	ReplicationFactor int     `json:"replication_factor"`
	Load              float64 `json:"load"`
	HashFunction      string  `json:"hash_function"`
}

type hashRing interface {
	LocateKey(ctx context.Context, key []byte) (string, error)
	LocateReplicas(ctx context.Context, key []byte, count int) ([]string, error)
	Add(ctx context.Context, member string) error
	Remove(ctx context.Context, member string) error
}

func DefaultLocatorConfig() LocatorConfig {
	return LocatorConfig{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		HashFunction:      "xxhash",
	}
}

// ConsistentLocator implements Locator with a consistent hash ring.
type ConsistentLocator struct {
	mu     sync.RWMutex
	nodes  map[string]Node
	ring   hashRing
	config LocatorConfig
	closed bool
	logger atomic.Pointer[slog.Logger]
}

func NewConsistentLocator(config LocatorConfig) (*ConsistentLocator, error) {
	ring, err := newHashRing(config)
	if err != nil {
		return nil, err
	}

	cl := &ConsistentLocator{
		nodes:  make(map[string]Node),
		ring:   ring,
		config: config,
	}
	cl.logger.Store(discardLogger)

	return cl, nil
}

func newHashRing(config LocatorConfig) (hashRing, error) {
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

	return ring, nil
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

	if cl.closed {
		return nil, ErrLocatorClosed
	}
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

	if cl.closed {
		return nil, ErrLocatorClosed
	}
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

// AddNodeContext adds a node while honoring the caller's cancellation and
// deadline during ring rebalancing.
func (cl *ConsistentLocator) AddNodeContext(ctx context.Context, node Node) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}
	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}

	nodeID := node.ID()
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.closed {
		return ErrLocatorClosed
	}
	if _, exists := cl.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in locator", nodeID)
	}

	if err := cl.ring.Add(ctx, nodeID); err != nil {
		return fmt.Errorf("failed to add node %s to consistent hash ring: %w", nodeID, err)
	}

	cl.nodes[nodeID] = node

	return nil
}

// RemoveNodeContext removes a node while honoring the caller's cancellation
// and deadline during ring rebalancing.
func (cl *ConsistentLocator) RemoveNodeContext(ctx context.Context, nodeID string) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	cl.mu.Lock()
	if cl.closed {
		cl.mu.Unlock()
		return ErrLocatorClosed
	}
	nodeToClose, exists := cl.nodes[nodeID]
	if !exists {
		cl.mu.Unlock()
		return fmt.Errorf("node %s not found in locator", nodeID)
	}
	if err := cl.ring.Remove(ctx, nodeID); err != nil {
		cl.mu.Unlock()
		return fmt.Errorf("failed to remove node %s from consistent hash ring: %w", nodeID, err)
	}
	delete(cl.nodes, nodeID)
	cl.mu.Unlock()

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
	newRing, err := newHashRing(cl.config)
	if err != nil {
		return fmt.Errorf("failed to reset closed locator ring: %w", err)
	}

	cl.mu.Lock()
	if cl.closed {
		cl.mu.Unlock()
		return nil
	}
	cl.closed = true
	nodes := cl.nodes
	cl.nodes = make(map[string]Node)
	cl.ring = newRing
	cl.mu.Unlock()

	var closeErr error
	for nodeID, node := range nodes {
		if err := node.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("failed to close node %s: %w", nodeID, err))
		}
	}

	return closeErr
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

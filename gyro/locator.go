package gyro

import (
	"context"
	"fmt"
	"sync"

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
		HashFunction:      "crc64",
	}
}

type ConsistentLocator struct {
	mu     sync.RWMutex
	nodes  map[string]Node
	ring   *consistent.Consistent
	config LocatorConfig
}

func NewConsistentLocator(config LocatorConfig) (*ConsistentLocator, error) {
	var hasher consistent.Hasher
	switch config.HashFunction {
	case "fnv":
		hasher = consistent.NewFNVHasher()
	case "crc64":
		fallthrough
	default:
		hasher = consistent.NewCRC64Hasher()
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

	return &ConsistentLocator{
		nodes:  make(map[string]Node),
		ring:   ring,
		config: config,
	}, nil
}

// Get retrieves a node for the given key using consistent hashing.
func (cl *ConsistentLocator) Get(ctx context.Context, key string) (Node, error) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	if len(cl.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available in ring")
	}

	// Use consistent hashing to find the node
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

	// Check if node is healthy
	if !node.IsHealthy(ctx) {
		// Try to find a replica
		replicas, err := cl.GetReplicas(ctx, key, 2)
		if err != nil || len(replicas) < 2 {
			return nil, fmt.Errorf("primary node %s is unhealthy and no healthy replicas found", nodeID)
		}
		// Return the first healthy replica (excluding the primary)
		for _, replica := range replicas[1:] {
			if replica.IsHealthy(ctx) {
				return replica, nil
			}
		}
		return nil, fmt.Errorf("no healthy nodes found for key: %s", key)
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

	// Get replica node IDs from consistent hash ring
	nodeIDs, err := cl.ring.LocateReplicas(ctx, []byte(key), count)
	if err != nil {
		return nil, fmt.Errorf("failed to locate replicas for key %s: %w", key, err)
	}

	// Convert node IDs to Node objects
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

	// Check if node already exists
	if _, exists := cl.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in locator", nodeID)
	}

	// Add node to consistent hash ring
	if err := cl.ring.Add(context.Background(), nodeID); err != nil {
		return fmt.Errorf("failed to add node %s to consistent hash ring: %w", nodeID, err)
	}

	// Add node to local map
	cl.nodes[nodeID] = node

	return nil
}

// RemoveNode removes a node from the locator.
func (cl *ConsistentLocator) RemoveNode(nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Check if node exists
	node, exists := cl.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found in locator", nodeID)
	}

	// Remove node from consistent hash ring
	if err := cl.ring.Remove(context.Background(), nodeID); err != nil {
		return fmt.Errorf("failed to remove node %s from consistent hash ring: %w", nodeID, err)
	}

	// Close the node connection
	if err := node.Close(); err != nil {
		// TODO: use a proper logger
		fmt.Printf("Warning: failed to close node %s: %v\n", nodeID, err)
	}

	// Remove node from local map
	delete(cl.nodes, nodeID)

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

	// Clear all nodes
	cl.nodes = make(map[string]Node)

	return lastErr
}

// GetStats returns statistics about the locator.
func (cl *ConsistentLocator) GetStats() LocatorStats {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	stats := LocatorStats{
		TotalNodes:     len(cl.nodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
	}

	ctx := context.Background()
	for _, node := range cl.nodes {
		if node.IsHealthy(ctx) {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
	}

	return stats
}

// LocatorStats contains statistics about a locator.
type LocatorStats struct {
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	UnhealthyNodes int `json:"unhealthy_nodes"`
}

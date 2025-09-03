package gyro

import (
	"context"
	"fmt"
	"sync"

	"github.com/focusandinsist/consistent-go/consistent"
)

type PoolConfig struct {
	PartitionCount    int     `json:"partition_count"`
	ReplicationFactor int     `json:"replication_factor"`
	Load              float64 `json:"load"`
	HashFunction      string  `json:"hash_function"`
}

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		HashFunction:      "crc64",
	}
}

type ConsistentPool struct {
	mu     sync.RWMutex
	nodes  map[string]Node
	ring   *consistent.Consistent
	config PoolConfig
}

func NewConsistentPool(config PoolConfig) (*ConsistentPool, error) {
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

	return &ConsistentPool{
		nodes:  make(map[string]Node),
		ring:   ring,
		config: config,
	}, nil
}

// Get retrieves a node for the given key using consistent hashing.
func (p *ConsistentPool) Get(ctx context.Context, key string) (Node, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available in pool")
	}

	// Use consistent hashing to find the node
	nodeID, err := p.ring.LocateKey(ctx, []byte(key))
	if err != nil {
		return nil, err
	}
	if nodeID == "" {
		return nil, fmt.Errorf("failed to locate node for key: %s", key)
	}

	node, exists := p.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found in pool", nodeID)
	}

	// Check if node is healthy
	if !node.IsHealthy(ctx) {
		// Try to find a replica
		replicas, err := p.GetReplicas(ctx, key, 2)
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
func (p *ConsistentPool) GetReplicas(ctx context.Context, key string, count int) ([]Node, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.nodes) == 0 {
		return nil, fmt.Errorf("no nodes available in pool")
	}

	if count <= 0 {
		return []Node{}, nil
	}

	// Get replica node IDs from consistent hash ring
	nodeIDs, err := p.ring.LocateReplicas(ctx, []byte(key), count)
	if err != nil {
		return nil, fmt.Errorf("failed to locate replicas for key %s: %w", key, err)
	}

	// Convert node IDs to Node objects
	replicas := make([]Node, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if node, exists := p.nodes[nodeID]; exists {
			replicas = append(replicas, node)
		}
	}

	return replicas, nil
}

// AddNode adds a new node to the pool.
func (p *ConsistentPool) AddNode(node Node) error {
	if node == nil {
		return fmt.Errorf("node cannot be nil")
	}

	nodeID := node.ID()
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if node already exists
	if _, exists := p.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists in pool", nodeID)
	}

	// Add node to consistent hash ring
	if err := p.ring.Add(context.Background(), nodeID); err != nil {
		return fmt.Errorf("failed to add node %s to consistent hash ring: %w", nodeID, err)
	}

	// Add node to local map
	p.nodes[nodeID] = node

	return nil
}

// RemoveNode removes a node from the pool.
func (p *ConsistentPool) RemoveNode(nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if node exists
	node, exists := p.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found in pool", nodeID)
	}

	// Remove node from consistent hash ring
	if err := p.ring.Remove(context.Background(), nodeID); err != nil {
		return fmt.Errorf("failed to remove node %s from consistent hash ring: %w", nodeID, err)
	}

	// Close the node connection
	if err := node.Close(); err != nil {
		// TODO: use a proper logger
		fmt.Printf("Warning: failed to close node %s: %v\n", nodeID, err)
	}

	// Remove node from local map
	delete(p.nodes, nodeID)

	return nil
}

// GetAllNodes returns all nodes in the pool.
func (p *ConsistentPool) GetAllNodes() []Node {
	p.mu.RLock()
	defer p.mu.RUnlock()

	nodes := make([]Node, 0, len(p.nodes))
	for _, node := range p.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// Close closes all connections and releases resources.
func (p *ConsistentPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for nodeID, node := range p.nodes {
		if err := node.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close node %s: %w", nodeID, err)
		}
	}

	// Clear all nodes
	p.nodes = make(map[string]Node)

	return lastErr
}

// GetStats returns statistics about the pool.
func (p *ConsistentPool) GetStats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := PoolStats{
		TotalNodes:     len(p.nodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
	}

	ctx := context.Background()
	for _, node := range p.nodes {
		if node.IsHealthy(ctx) {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
	}

	return stats
}

// PoolStats contains statistics about a pool.
type PoolStats struct {
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	UnhealthyNodes int `json:"unhealthy_nodes"`
}

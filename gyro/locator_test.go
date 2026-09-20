package gyro

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type removeFailingRing struct {
	hashRing
	err error
}

func (r *removeFailingRing) Remove(context.Context, string) error {
	return r.err
}

func TestConsistentLocator_AddNode(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	// Test adding nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")

	err = locator.AddNode(node1)
	if err != nil {
		t.Fatalf("Failed to add node1: %v", err)
	}

	err = locator.AddNode(node2)
	if err != nil {
		t.Fatalf("Failed to add node2: %v", err)
	}

	// Verify nodes are added
	allNodes := locator.GetAllNodes()
	if len(allNodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(allNodes))
	}

	// Verify stats
	stats := locator.GetStats()
	if stats.TotalNodes != 2 {
		t.Errorf("Expected TotalNodes=2, got %d", stats.TotalNodes)
	}
}

func TestConsistentLocator_RemoveNode(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	// Add nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")

	locator.AddNode(node1)
	locator.AddNode(node2)

	// Remove node1
	err = locator.RemoveNode("node1")
	if err != nil {
		t.Fatalf("Failed to remove node1: %v", err)
	}

	// Verify node is removed
	allNodes := locator.GetAllNodes()
	if len(allNodes) != 1 {
		t.Errorf("Expected 1 node after removal, got %d", len(allNodes))
	}

	if allNodes[0].ID() != "node2" {
		t.Errorf("Expected remaining node to be node2, got %s", allNodes[0].ID())
	}

	// Verify stats
	stats := locator.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("Expected TotalNodes=1 after removal, got %d", stats.TotalNodes)
	}
}

func TestConsistentLocator_GetConsistency(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	// Add nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")
	node3 := NewMockNode("node3", "127.0.0.1:6381")

	locator.AddNode(node1)
	locator.AddNode(node2)
	locator.AddNode(node3)

	ctx := context.Background()
	testKey := "test_key_123"

	// Test consistency: same key should always return same node
	var firstNode Node
	for i := 0; i < 10; i++ {
		node, err := locator.Get(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get node for key %s: %v", testKey, err)
		}

		if i == 0 {
			firstNode = node
		} else if node.ID() != firstNode.ID() {
			t.Errorf("Inconsistent routing: expected %s, got %s on iteration %d",
				firstNode.ID(), node.ID(), i)
		}
	}
}

func TestConsistentLocator_GetDistribution(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	// Add nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")
	node3 := NewMockNode("node3", "127.0.0.1:6381")

	locator.AddNode(node1)
	locator.AddNode(node2)
	locator.AddNode(node3)

	ctx := context.Background()

	// Test distribution: different keys should be distributed across nodes
	nodeCount := make(map[string]int)
	totalKeys := 1000

	for i := 0; i < totalKeys; i++ {
		key := "test_key_" + string(rune(i))
		node, err := locator.Get(ctx, key)
		if err != nil {
			t.Fatalf("Failed to get node for key %s: %v", key, err)
		}
		nodeCount[node.ID()]++
	}

	// Verify all nodes receive some keys
	if len(nodeCount) != 3 {
		t.Errorf("Expected keys distributed to 3 nodes, got %d", len(nodeCount))
	}

	// Verify reasonable distribution (each node should get roughly 1/3 of keys)
	// Allow for some variance due to hash distribution
	expectedPerNode := totalKeys / 3
	tolerance := expectedPerNode / 2 // 50% tolerance

	for nodeID, count := range nodeCount {
		if count < expectedPerNode-tolerance || count > expectedPerNode+tolerance {
			t.Logf("Node %s got %d keys (expected ~%d)", nodeID, count, expectedPerNode)
		}
	}

	t.Logf("Key distribution: %v", nodeCount)
}

func TestConsistentLocator_GetReplicas(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	// Add nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")
	node3 := NewMockNode("node3", "127.0.0.1:6381")

	locator.AddNode(node1)
	locator.AddNode(node2)
	locator.AddNode(node3)

	ctx := context.Background()
	testKey := "test_key_replicas"

	// Test getting 2 replicas
	replicas, err := locator.GetReplicas(ctx, testKey, 2)
	if err != nil {
		t.Fatalf("Failed to get replicas: %v", err)
	}

	if len(replicas) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(replicas))
	}

	// Verify replicas are different nodes
	if replicas[0].ID() == replicas[1].ID() {
		t.Errorf("Replicas should be different nodes, got %s and %s",
			replicas[0].ID(), replicas[1].ID())
	}

	// Test getting all replicas (should return all available nodes)
	allReplicas, err := locator.GetReplicas(ctx, testKey, 3) // Exact number of available nodes
	if err != nil {
		t.Fatalf("Failed to get all replicas: %v", err)
	}

	if len(allReplicas) != 3 {
		t.Errorf("Expected 3 replicas (all nodes), got %d", len(allReplicas))
	}

	// Verify all replicas are unique
	nodeIDs := make(map[string]bool)
	for _, replica := range allReplicas {
		if nodeIDs[replica.ID()] {
			t.Errorf("Duplicate replica node: %s", replica.ID())
		}
		nodeIDs[replica.ID()] = true
	}
}

func TestConsistentLocator_EmptyLocator(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	ctx := context.Background()

	// Test Get on empty locator
	_, err = locator.Get(ctx, "test_key")
	if err == nil {
		t.Error("Expected error when getting from empty locator")
	}

	// Test GetReplicas on empty locator
	_, err = locator.GetReplicas(ctx, "test_key", 1)
	if err == nil {
		t.Error("Expected error when getting replicas from empty locator")
	}

	// Test RemoveNode on empty locator
	err = locator.RemoveNode("nonexistent")
	if err == nil {
		t.Error("Expected error when removing from empty locator")
	}
}

func TestConsistentLocator_DuplicateNode(t *testing.T) {
	config := DefaultLocatorConfig()

	locator, err := NewConsistentLocator(config)
	if err != nil {
		t.Fatalf("Failed to create locator: %v", err)
	}

	node1 := NewMockNode("node1", "127.0.0.1:6379")

	// Add node first time
	err = locator.AddNode(node1)
	if err != nil {
		t.Fatalf("Failed to add node first time: %v", err)
	}

	// Try to add same node again
	err = locator.AddNode(node1)
	if err == nil {
		t.Error("Expected error when adding duplicate node")
	}

	// Verify only one node exists
	allNodes := locator.GetAllNodes()
	if len(allNodes) != 1 {
		t.Errorf("Expected 1 node after duplicate add attempt, got %d", len(allNodes))
	}
}

func TestConsistentLocator_RemoveFailureKeepsNodeAndConnection(t *testing.T) {
	locator, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatalf("NewConsistentLocator failed: %v", err)
	}
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")
	if err := locator.AddNode(node1); err != nil {
		t.Fatalf("AddNode(node1) failed: %v", err)
	}
	if err := locator.AddNode(node2); err != nil {
		t.Fatalf("AddNode(node2) failed: %v", err)
	}
	removeErr := errors.New("ring removal failed")
	locator.ring = &removeFailingRing{hashRing: locator.ring, err: removeErr}

	if err := locator.RemoveNode(node1.ID()); !errors.Is(err, removeErr) {
		t.Fatalf("RemoveNode error = %v, want %v", err, removeErr)
	}
	if got := len(locator.GetAllNodes()); got != 2 {
		t.Fatalf("failed ring removal changed node map size to %d, want 2", got)
	}
	if !node1.IsHealthy(context.Background()) {
		t.Fatal("failed ring removal closed the node connection")
	}
	for i := 0; i < 100; i++ {
		node, err := locator.Get(context.Background(), fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("Get after failed removal returned split ring/map state: %v", err)
		}
		if node.ID() != node1.ID() && node.ID() != node2.ID() {
			t.Fatalf("Get returned unknown node %q", node.ID())
		}
	}
}

type blockingCloseNode struct {
	id      string
	started chan struct{}
	release chan struct{}
}

func (n *blockingCloseNode) ID() string                     { return n.id }
func (n *blockingCloseNode) Address() string                { return n.id }
func (n *blockingCloseNode) IsHealthy(context.Context) bool { return true }
func (n *blockingCloseNode) Close() error {
	select {
	case <-n.started:
	default:
		close(n.started)
	}
	<-n.release
	return nil
}

func TestConsistentLocator_CloseDoesNotHoldLockDuringNodeClose(t *testing.T) {
	locator, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	node := &blockingCloseNode{id: "node", started: make(chan struct{}), release: make(chan struct{})}
	if err := locator.AddNode(node); err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- locator.Close() }()
	select {
	case <-node.started:
	case <-time.After(time.Second):
		t.Fatal("node close did not start")
	}
	operationDone := make(chan error, 1)
	go func() { operationDone <- locator.AddNode(NewMockNode("new", "new")) }()
	select {
	case err := <-operationDone:
		if err != nil {
			t.Fatalf("AddNode while Close was closing detached node failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("locator lock was held while node Close blocked")
	}
	close(node.release)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

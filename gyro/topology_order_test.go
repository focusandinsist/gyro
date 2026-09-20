package gyro

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

type operationRecordingLocator struct {
	nodes      map[string]Node
	operations []string
}

func newOperationRecordingLocator(nodes ...Node) *operationRecordingLocator {
	result := &operationRecordingLocator{nodes: make(map[string]Node)}
	for _, node := range nodes {
		result.nodes[node.ID()] = node
	}
	return result
}

func (l *operationRecordingLocator) Get(context.Context, string) (Node, error) {
	return nil, fmt.Errorf("not implemented")
}

func (l *operationRecordingLocator) GetReplicas(context.Context, string, int) ([]Node, error) {
	return nil, fmt.Errorf("not implemented")
}

func (l *operationRecordingLocator) AddNode(node Node) error {
	l.operations = append(l.operations, "add:"+node.ID())
	l.nodes[node.ID()] = node
	return nil
}

func (l *operationRecordingLocator) RemoveNode(nodeID string) error {
	l.operations = append(l.operations, "remove:"+nodeID)
	delete(l.nodes, nodeID)
	return nil
}

func (l *operationRecordingLocator) GetAllNodes() []Node {
	result := make([]Node, 0, len(l.nodes))
	for _, node := range l.nodes {
		result = append(result, node)
	}
	return result
}

func (l *operationRecordingLocator) Close() error { return nil }

func TestClientAppliesTopologyChangesInNodeIDOrder(t *testing.T) {
	want := []string{
		"remove:node-b",
		"remove:node-d",
		"add:node-a",
		"add:node-c",
		"remove:node-e",
		"add:node-e",
	}
	for iteration := 0; iteration < 100; iteration++ {
		locator := newOperationRecordingLocator(
			NewMockNode("node-b", "old-b"),
			NewMockNode("node-d", "old-d"),
			NewMockNode("node-e", "old-e"),
		)
		client := &Client{
			locator:       locator,
			running:       true,
			nodeFactory:   NewMockNodeFactory(),
			healthChecker: &controllableHealthChecker{config: DefaultHealthCheckerConfig()},
		}
		client.logger.Store(discardLogger)
		client.handleServiceNodesChange([]NodeInfo{
			{ID: "node-e", Address: "new-e"},
			{ID: "node-c", Address: "new-c"},
			{ID: "node-a", Address: "new-a"},
		})

		if !reflect.DeepEqual(locator.operations, want) {
			t.Fatalf("iteration %d: operations = %v, want %v", iteration, locator.operations, want)
		}
	}
}

func TestClientAddressReorderDoesNotRecreateNodes(t *testing.T) {
	addresses := []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
	discovery := NewStaticServiceDiscovery(addresses)
	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	factory := NewMockNodeFactory()
	client, err := NewClient(
		"orders",
		discovery,
		NewConfigManager(config),
		factory,
		NewDefaultHealthChecker(config.HealthChecker),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	original := make(map[string]Node)
	currentNodes := client.GetLocator().GetAllNodes()
	for _, node := range currentNodes {
		original[node.ID()] = node
	}
	discovery.UpdateNodes("orders", []string{addresses[2], addresses[0], addresses[1]})
	reordered, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Discover after reorder failed: %v", err)
	}
	client.mu.Lock()
	client.running = true
	client.mu.Unlock()
	client.handleServiceNodesChange(reordered)
	client.mu.Lock()
	client.running = false
	client.mu.Unlock()

	currentNodes = client.GetLocator().GetAllNodes()
	if len(currentNodes) != len(original) {
		t.Fatalf("address reorder changed node count from %d to %d", len(original), len(currentNodes))
	}
	for _, node := range currentNodes {
		if original[node.ID()] != node {
			t.Fatalf("address reorder recreated node %q", node.ID())
		}
	}
}

func TestClientInitialRingIsDeterministicAcrossDiscoveryOrder(t *testing.T) {
	nodes := []NodeInfo{
		{ID: "node-d", Address: "127.0.0.1:8004"},
		{ID: "node-b", Address: "127.0.0.1:8002"},
		{ID: "node-a", Address: "127.0.0.1:8001"},
		{ID: "node-c", Address: "127.0.0.1:8003"},
	}
	reordered := []NodeInfo{nodes[2], nodes[3], nodes[1], nodes[0]}
	first := newDeterministicTestClient(t, nodes)
	defer first.Close()
	second := newDeterministicTestClient(t, reordered)
	defer second.Close()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		firstNode, err := first.GetLocator().Get(context.Background(), key)
		if err != nil {
			t.Fatalf("first locator Get(%q) failed: %v", key, err)
		}
		secondNode, err := second.GetLocator().Get(context.Background(), key)
		if err != nil {
			t.Fatalf("second locator Get(%q) failed: %v", key, err)
		}
		if firstNode.ID() != secondNode.ID() {
			t.Fatalf("Get(%q) differs by discovery order: %q vs %q", key, firstNode.ID(), secondNode.ID())
		}
	}
}

func newDeterministicTestClient(t *testing.T, nodes []NodeInfo) *Client {
	t.Helper()
	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	client, err := NewClient(
		"deterministic-ring",
		NewMockServiceDiscovery(nodes),
		NewConfigManager(config),
		NewMockNodeFactory(),
		NewDefaultHealthChecker(config.HealthChecker),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	return client
}

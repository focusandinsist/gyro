package gyro

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func waitForNodeChecks(t *testing.T, node *MockNode, minimum int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if node.GetCheckCallCount() >= minimum {
			return
		}
		<-ticker.C
	}
	t.Fatalf("node %s did not receive %d health checks", node.ID(), minimum)
}

type clientLockCheckingFactory struct {
	client   *Client
	lockHeld atomic.Bool
}

func (f *clientLockCheckingFactory) CreateNode(info NodeInfo) (Node, error) {
	if f.client != nil {
		if !f.client.stateMu.TryLock() {
			f.lockHeld.Store(true)
		} else {
			f.client.stateMu.Unlock()
		}
	}
	return &clientLockCheckingNode{Node: NewMockNode(info.ID, info.Address), factory: f}, nil
}

type clientLockCheckingNode struct {
	Node
	factory *clientLockCheckingFactory
}

func (n *clientLockCheckingNode) Close() error {
	if n.factory.client != nil {
		if !n.factory.client.stateMu.TryLock() {
			n.factory.lockHeld.Store(true)
		} else {
			n.factory.client.stateMu.Unlock()
		}
	}
	return n.Node.Close()
}

func TestClientTopologyReconcileDoesNotHoldStateLockDuringNodeIO(t *testing.T) {
	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	factory := &clientLockCheckingFactory{}
	client, err := NewClient(
		"lock-boundary-service",
		NewMockServiceDiscovery([]NodeInfo{{ID: "node-1", Address: "node-1"}}),
		NewConfigManager(config),
		factory,
		NewDefaultHealthChecker(config.HealthChecker),
	)
	if err != nil {
		t.Fatal(err)
	}
	factory.client = client
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.handleServiceNodesChange(context.Background(), []NodeInfo{
		{ID: "node-1", Address: "node-1"},
		{ID: "node-2", Address: "node-2"},
	})
	client.handleServiceNodesChange(context.Background(), []NodeInfo{
		{ID: "node-2", Address: "node-2"},
	})

	if factory.lockHeld.Load() {
		t.Fatal("Client held its state lock during node creation or close")
	}
	if err := client.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if factory.lockHeld.Load() {
		t.Fatal("Client held its state lock while rebuilding nodes on restart")
	}
}

func TestClientTopologyReconcileRebuildsNodeWhenMetadataChanges(t *testing.T) {
	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	factory := NewMockNodeFactory()
	client, err := NewClient(
		"metadata-change-service",
		NewMockServiceDiscovery([]NodeInfo{{
			ID: "node-1", Address: "node-1", Metadata: map[string]string{"zone": "a"},
		}}),
		NewConfigManager(config),
		factory,
		NewDefaultHealthChecker(config.HealthChecker),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	oldNode := factory.GetMockNode("node-1")
	client.handleServiceNodesChange(context.Background(), []NodeInfo{{
		ID: "node-1", Address: "node-1", Metadata: map[string]string{"zone": "b"},
	}})

	newNode := factory.GetMockNode("node-1")
	if newNode == oldNode {
		t.Fatal("metadata change did not rebuild the node")
	}
	if oldNode.IsHealthy(context.Background()) {
		t.Fatal("metadata change did not close the replaced node")
	}
}

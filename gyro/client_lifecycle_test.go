package gyro

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func (m *MockServiceDiscovery) Register(_ context.Context, _ string, node NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = append(m.nodes, node)
	return nil
}

func (m *MockServiceDiscovery) Unregister(_ context.Context, _ string, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, node := range m.nodes {
		if node.ID == nodeID {
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("node %s not found", nodeID)
}

func newLifecycleTestClient(t *testing.T) (*Client, *ConfigManager, *MockNodeFactory) {
	t.Helper()

	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	configManager := NewConfigManager(config)
	discovery := NewMockServiceDiscovery([]NodeInfo{
		{ID: "node-1", Address: "127.0.0.1:6379"},
	})
	nodeFactory := NewMockNodeFactory()
	healthChecker := NewDefaultHealthChecker(config.HealthChecker)

	client, err := NewClient(
		"lifecycle-test-service",
		discovery,
		configManager,
		nodeFactory,
		healthChecker,
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	return client, configManager, nodeFactory
}

func TestClientCloseBeforeStartReleasesInitialLocator(t *testing.T) {
	client, _, nodeFactory := newLifecycleTestClient(t)
	initialNode := nodeFactory.GetMockNode("node-1")
	if initialNode == nil {
		t.Fatal("expected NewClient to create the initial node")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close before Start failed: %v", err)
	}

	if client.GetLocator() != nil {
		t.Fatal("locator must be cleared after Close")
	}
	if initialNode.IsHealthy(context.Background()) {
		t.Fatal("Close before Start did not close the initial node")
	}
}

func TestClientCanRestartAfterStop(t *testing.T) {
	client, _, nodeFactory := newLifecycleTestClient(t)
	ctx := context.Background()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	firstRunLocator := client.GetLocator()
	if firstRunLocator == nil {
		t.Fatal("first Start did not expose a locator")
	}

	if err := client.Stop(); err != nil {
		t.Fatalf("Stop after first Start failed: %v", err)
	}
	if client.GetLocator() != nil {
		t.Fatal("Stop must clear the locator before a later Start")
	}

	if err := client.Start(ctx); err != nil {
		t.Fatalf("second Start failed: %v", err)
	}
	secondRunLocator := client.GetLocator()
	if secondRunLocator == nil {
		t.Fatal("second Start did not recreate a locator")
	}
	if secondRunLocator == firstRunLocator {
		t.Fatal("second Start reused the closed locator")
	}
	if nodeFactory.GetMockNode("node-1") == nil {
		t.Fatal("second Start did not recreate the node")
	}

	if err := client.Stop(); err != nil {
		t.Fatalf("Stop after second Start failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("repeated Close after Stop failed: %v", err)
	}
}

func TestClientRegistersConfigWatcherOnlyOnceAcrossRestarts(t *testing.T) {
	client, configManager, _ := newLifecycleTestClient(t)
	ctx := context.Background()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	if err := client.Stop(); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatalf("second Start failed: %v", err)
	}
	if err := client.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}

	configManager.mu.RLock()
	watcherCount := len(configManager.watchers)
	configManager.mu.RUnlock()
	if watcherCount != 1 {
		t.Fatalf("expected one config watcher after restart, got %d", watcherCount)
	}
}

func TestClientIgnoresConfigUpdatesWhileStopped(t *testing.T) {
	client, configManager, _ := newLifecycleTestClient(t)

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := client.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	newConfig := *configManager.GetConfig()
	newConfig.Locator.PartitionCount++
	if err := configManager.UpdateConfig(&newConfig); err != nil {
		t.Fatalf("UpdateConfig while stopped failed: %v", err)
	}

	if client.GetLocator() != nil {
		t.Fatal("a stopped client must not recreate a locator from a config watcher")
	}
}

func TestClientConfigReloadReplacesAndClosesOldLocator(t *testing.T) {
	client, configManager, nodeFactory := newLifecycleTestClient(t)
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	oldPool := client.GetLocator()
	oldNode := nodeFactory.GetMockNode("node-1")
	if oldPool == nil || oldNode == nil {
		t.Fatal("expected an initialized client locator and node")
	}

	newConfig := *configManager.GetConfig()
	newConfig.Locator.PartitionCount++
	if err := configManager.UpdateConfig(&newConfig); err != nil {
		t.Fatalf("locator config update failed: %v", err)
	}

	if client.GetLocator() != oldPool {
		t.Fatal("configuration reload should preserve the health-aware pool instance")
	}
	if oldNode.IsHealthy(context.Background()) {
		t.Fatal("configuration reload did not close the old node connection")
	}
	if nodeFactory.GetMockNode("node-1") == oldNode {
		t.Fatal("configuration reload reused the old node connection")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestClientConfigReloadKeepsHealthMonitoringActive(t *testing.T) {
	config := DefaultClientConfig()
	config.HealthChecker = HealthCheckerConfig{
		Enabled:           true,
		Interval:          5 * time.Millisecond,
		Timeout:           5 * time.Millisecond,
		FailureThreshold:  1,
		RecoveryThreshold: 1,
	}
	configManager := NewConfigManager(config)
	discovery := NewMockServiceDiscovery([]NodeInfo{{ID: "node-1", Address: "127.0.0.1:6379"}})
	nodeFactory := NewMockNodeFactory()
	client, err := NewClient(
		"config-reload-health-service",
		discovery,
		configManager,
		nodeFactory,
		NewDefaultHealthChecker(config.HealthChecker),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	oldNode := nodeFactory.GetMockNode("node-1")
	waitForNodeChecks(t, oldNode, 1)

	newConfig := *configManager.GetConfig()
	newConfig.Locator.PartitionCount++
	if err := configManager.UpdateConfig(&newConfig); err != nil {
		t.Fatalf("locator config update failed: %v", err)
	}

	newNode := nodeFactory.GetMockNode("node-1")
	if newNode == oldNode {
		t.Fatal("configuration reload reused the old node connection")
	}
	waitForNodeChecks(t, newNode, 1)
}

func TestClientRejectsUnsupportedConnectionConfigReload(t *testing.T) {
	client, configManager, nodeFactory := newLifecycleTestClient(t)
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	oldConfig := configManager.GetConfig()
	oldNode := nodeFactory.GetMockNode("node-1")
	newConfig := *oldConfig
	newConfig.Connection.ConnectTimeout++
	if err := configManager.UpdateConfig(&newConfig); err == nil {
		t.Fatal("expected unsupported connection config reload to fail")
	}

	currentConfig := configManager.GetConfig()
	if currentConfig.Connection != oldConfig.Connection {
		t.Fatal("failed connection config update was not rolled back")
	}
	if !oldNode.IsHealthy(context.Background()) {
		t.Fatal("failed connection config update closed the active node")
	}
}

func waitForNodeChecks(t *testing.T, node *MockNode, minimum int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.GetCheckCallCount() >= minimum {
			return
		}
		time.Sleep(10 * time.Millisecond)
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

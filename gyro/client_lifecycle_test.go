package gyro

import (
	"context"
	"fmt"
	"testing"
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

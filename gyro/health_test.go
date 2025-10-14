package gyro

import (
	"context"
	"testing"
	"time"
)

func TestDefaultHealthChecker_FailureThreshold(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}

	checker := NewDefaultHealthChecker(config)
	mockNode := NewMockNode("test-node", "127.0.0.1:6379")

	// Add mock health listener
	listener := NewMockHealthListener()
	checker.AddHealthListener(listener.AsHealthListener())

	// Add node to checker
	checker.AddNode(mockNode)

	// Initially node should be healthy
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Node should be initially healthy")
	}

	// Set node to unhealthy
	mockNode.SetHealthy(false)

	ctx := context.Background()

	// First failure - should still be considered healthy
	err := checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Node should still be healthy after 1 failure (threshold=3)")
	}

	// Second failure - should still be considered healthy
	err = checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Node should still be healthy after 2 failures (threshold=3)")
	}

	// Third failure - should now be considered unhealthy
	err = checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if checker.IsNodeHealthy("test-node") {
		t.Error("Node should be unhealthy after 3 failures (threshold=3)")
	}

	// Give some time for async health listener to be called
	time.Sleep(10 * time.Millisecond)

	// Verify health listener was triggered
	events := listener.GetEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 health event, got %d", len(events))
	}

	if len(events) > 0 {
		event := events[0]
		if event.NodeID != "test-node" {
			t.Errorf("Expected event for test-node, got %s", event.NodeID)
		}
		if event.Healthy {
			t.Error("Expected unhealthy event")
		}
	}
}

func TestDefaultHealthChecker_RecoveryThreshold(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  2,
		RecoveryThreshold: 3,
	}

	checker := NewDefaultHealthChecker(config)
	mockNode := NewMockNode("test-node", "127.0.0.1:6379")

	// Add mock health listener
	listener := NewMockHealthListener()
	checker.AddHealthListener(listener.AsHealthListener())

	// Add node to checker
	checker.AddNode(mockNode)

	ctx := context.Background()

	// Make node unhealthy first
	mockNode.SetHealthy(false)

	// Trigger failures to make node unhealthy
	checker.Check(ctx, mockNode) // 1st failure
	checker.Check(ctx, mockNode) // 2nd failure - should be unhealthy now

	if checker.IsNodeHealthy("test-node") {
		t.Error("Node should be unhealthy after 2 failures")
	}

	// Wait for the unhealthy event to be processed
	time.Sleep(10 * time.Millisecond)

	// Clear events to focus on recovery
	listener.Clear()

	// Now make node healthy again
	mockNode.SetHealthy(true)

	// First success - should still be considered unhealthy
	err := checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if checker.IsNodeHealthy("test-node") {
		t.Error("Node should still be unhealthy after 1 success (recovery threshold=3)")
	}

	// Second success - should still be considered unhealthy
	err = checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if checker.IsNodeHealthy("test-node") {
		t.Error("Node should still be unhealthy after 2 successes (recovery threshold=3)")
	}

	// Third success - should now be considered healthy
	err = checker.Check(ctx, mockNode)
	if err != nil {
		t.Errorf("Check method should not return error, got: %v", err)
	}
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Node should be healthy after 3 successes (recovery threshold=3)")
	}

	// Give some time for async health listener to be called
	time.Sleep(10 * time.Millisecond)

	// Verify health listener was triggered for recovery
	events := listener.GetEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 recovery event, got %d", len(events))
	}

	if len(events) > 0 {
		event := events[0]
		if event.NodeID != "test-node" {
			t.Errorf("Expected event for test-node, got %s", event.NodeID)
		}
		if !event.Healthy {
			t.Error("Expected healthy recovery event")
		}
	}
}

func TestDefaultHealthChecker_MultipleNodes(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  2,
		RecoveryThreshold: 2,
	}

	checker := NewDefaultHealthChecker(config)

	// Create multiple mock nodes
	node1 := NewMockNode("node1", "127.0.0.1:6379")
	node2 := NewMockNode("node2", "127.0.0.1:6380")
	node3 := NewMockNode("node3", "127.0.0.1:6381")

	// Add mock health listener
	listener := NewMockHealthListener()
	checker.AddHealthListener(listener.AsHealthListener())

	// Add nodes to checker
	checker.AddNode(node1)
	checker.AddNode(node2)
	checker.AddNode(node3)

	// Initially all nodes should be healthy
	if !checker.IsNodeHealthy("node1") || !checker.IsNodeHealthy("node2") || !checker.IsNodeHealthy("node3") {
		t.Error("All nodes should be initially healthy")
	}

	ctx := context.Background()

	// Make node2 unhealthy
	node2.SetHealthy(false)

	// Trigger failures for node2 only
	checker.Check(ctx, node2) // 1st failure
	checker.Check(ctx, node2) // 2nd failure - should be unhealthy now

	// Check status: node1 and node3 should be healthy, node2 should be unhealthy
	if !checker.IsNodeHealthy("node1") {
		t.Error("Node1 should still be healthy")
	}
	if checker.IsNodeHealthy("node2") {
		t.Error("Node2 should be unhealthy")
	}
	if !checker.IsNodeHealthy("node3") {
		t.Error("Node3 should still be healthy")
	}

	// Give some time for async health listener to be called
	time.Sleep(10 * time.Millisecond)

	// Verify only one health event for node2
	events := listener.GetEvents()
	if len(events) != 1 {
		t.Errorf("Expected 1 health event, got %d", len(events))
	}

	if len(events) > 0 {
		event := events[0]
		if event.NodeID != "node2" {
			t.Errorf("Expected event for node2, got %s", event.NodeID)
		}
		if event.Healthy {
			t.Error("Expected unhealthy event for node2")
		}
	}
}

func TestDefaultHealthChecker_RemoveNode(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  2,
		RecoveryThreshold: 2,
	}

	checker := NewDefaultHealthChecker(config)
	mockNode := NewMockNode("test-node", "127.0.0.1:6379")

	// Add node to checker
	checker.AddNode(mockNode)

	// Verify node is tracked
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Node should be initially healthy")
	}

	// Remove node
	checker.RemoveNode("test-node")

	// Verify node is no longer tracked (DefaultHealthChecker returns true for unknown nodes)
	// This is the expected behavior - unknown nodes are assumed healthy
	if !checker.IsNodeHealthy("test-node") {
		t.Error("Unknown nodes should be considered healthy by default")
	}
}

func TestDefaultHealthChecker_ConfigUpdate(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  2,
		RecoveryThreshold: 2,
	}

	checker := NewDefaultHealthChecker(config)

	// Verify initial config
	currentConfig := checker.GetConfig()
	if currentConfig.FailureThreshold != 2 {
		t.Errorf("Expected initial FailureThreshold=2, got %d", currentConfig.FailureThreshold)
	}

	// Update config
	newConfig := HealthCheckerConfig{
		Enabled:           true,
		Interval:          200 * time.Millisecond,
		Timeout:           100 * time.Millisecond,
		FailureThreshold:  5,
		RecoveryThreshold: 3,
	}

	err := checker.UpdateConfig(newConfig)
	if err != nil {
		t.Fatalf("Failed to update config: %v", err)
	}

	// Verify config was updated
	updatedConfig := checker.GetConfig()
	if updatedConfig.FailureThreshold != 5 {
		t.Errorf("Expected updated FailureThreshold=5, got %d", updatedConfig.FailureThreshold)
	}
	if updatedConfig.Interval != 200*time.Millisecond {
		t.Errorf("Expected updated Interval=200ms, got %v", updatedConfig.Interval)
	}

	// Verify IsEnabled reflects the config
	if !checker.IsEnabled() {
		t.Error("Checker should be enabled")
	}

	// Test disabling
	disabledConfig := newConfig
	disabledConfig.Enabled = false

	err = checker.UpdateConfig(disabledConfig)
	if err != nil {
		t.Fatalf("Failed to update config to disabled: %v", err)
	}

	if checker.IsEnabled() {
		t.Error("Checker should be disabled")
	}
}

func TestDefaultHealthChecker_HealthListener(t *testing.T) {
	config := HealthCheckerConfig{
		Enabled:           true,
		Interval:          100 * time.Millisecond,
		Timeout:           50 * time.Millisecond,
		FailureThreshold:  1, // Quick failure for testing
		RecoveryThreshold: 1, // Quick recovery for testing
	}

	checker := NewDefaultHealthChecker(config)
	mockNode := NewMockNode("test-node", "127.0.0.1:6379")

	// Add multiple listeners
	listener1 := NewMockHealthListener()
	listener2 := NewMockHealthListener()

	checker.AddHealthListener(listener1.AsHealthListener())
	checker.AddHealthListener(listener2.AsHealthListener())

	// Add node to checker
	checker.AddNode(mockNode)

	ctx := context.Background()

	// Make node unhealthy
	mockNode.SetHealthy(false)
	checker.Check(ctx, mockNode) // Should trigger unhealthy event

	// Make node healthy again
	mockNode.SetHealthy(true)
	checker.Check(ctx, mockNode) // Should trigger healthy event

	// Give some time for async health listeners to be called
	time.Sleep(10 * time.Millisecond)

	// Verify both listeners received both events
	events1 := listener1.GetEvents()
	events2 := listener2.GetEvents()

	if len(events1) != 2 {
		t.Errorf("Listener1 expected 2 events, got %d", len(events1))
	}
	if len(events2) != 2 {
		t.Errorf("Listener2 expected 2 events, got %d", len(events2))
	}

	// Verify event sequence
	if len(events1) >= 2 {
		if events1[0].Healthy {
			t.Error("First event should be unhealthy")
		}
		if !events1[1].Healthy {
			t.Error("Second event should be healthy")
		}
	}
}

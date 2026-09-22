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

	listenerContext, cancelListener := context.WithTimeout(context.Background(), time.Second)
	defer cancelListener()
	if !listener.WaitForEvents(1, listenerContext) {
		t.Fatal("health listener did not receive the unhealthy event")
	}

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

func TestDefaultHealthCheckerAddNodeKeepsLastCheckTimeZero(t *testing.T) {
	checker := NewDefaultHealthChecker(DefaultHealthCheckerConfig())
	checker.AddNode(NewMockNode("unprobed", "127.0.0.1:6379"))

	stats := checker.GetNodeStats("unprobed")
	if stats == nil {
		t.Fatal("missing stats for added node")
	}
	if !stats.LastCheckTime.IsZero() {
		t.Fatalf("LastCheckTime = %v before first probe, want zero", stats.LastCheckTime)
	}
	if got := checker.LastCheckTime(); !got.IsZero() {
		t.Fatalf("checker LastCheckTime = %v before first probe, want zero", got)
	}
}

type blockingProbeNode struct {
	started chan struct{}
	release chan struct{}
}

func (n *blockingProbeNode) ID() string      { return "blocking" }
func (n *blockingProbeNode) Address() string { return "blocking" }
func (n *blockingProbeNode) Close() error    { return nil }
func (n *blockingProbeNode) IsHealthy(context.Context) bool {
	close(n.started)
	<-n.release
	return true
}

func TestDefaultHealthCheckerLastCheckTimeWaitsForProbeCompletion(t *testing.T) {
	checker := NewDefaultHealthChecker(DefaultHealthCheckerConfig())
	node := &blockingProbeNode{started: make(chan struct{}), release: make(chan struct{})}
	checker.AddNode(node)

	done := make(chan struct{})
	go func() {
		checker.Check(context.Background(), node)
		close(done)
	}()
	select {
	case <-node.started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	if stats := checker.GetNodeStats(node.ID()); stats == nil || !stats.LastCheckTime.IsZero() {
		t.Fatalf("LastCheckTime changed before probe completion: %#v", stats)
	}
	close(node.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not complete")
	}
	if stats := checker.GetNodeStats(node.ID()); stats == nil || stats.LastCheckTime.IsZero() {
		t.Fatalf("LastCheckTime was not recorded after probe completion: %#v", stats)
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

	listenerContext, cancelListener := context.WithTimeout(context.Background(), time.Second)
	if !listener.WaitForEvents(1, listenerContext) {
		cancelListener()
		t.Fatal("health listener did not receive the unhealthy event")
	}
	cancelListener()

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

	listenerContext, cancelListener = context.WithTimeout(context.Background(), time.Second)
	defer cancelListener()
	if !listener.WaitForEvents(1, listenerContext) {
		t.Fatal("health listener did not receive the recovery event")
	}

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

	listenerContext, cancelListener := context.WithTimeout(context.Background(), time.Second)
	defer cancelListener()
	if !listener.WaitForEvents(1, listenerContext) {
		t.Fatal("health listener did not receive the node2 event")
	}

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

	listenerContext, cancelListener := context.WithTimeout(context.Background(), time.Second)
	defer cancelListener()
	if !listener1.WaitForEvents(2, listenerContext) || !listener2.WaitForEvents(2, listenerContext) {
		t.Fatal("health listeners did not receive both events")
	}

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
func fastHealthConfig() HealthCheckerConfig {
	config := DefaultHealthCheckerConfig()
	config.Interval = 5 * time.Millisecond
	config.Timeout = 5 * time.Millisecond
	config.FailureThreshold = 1
	config.RecoveryThreshold = 1
	return config
}

func TestHealthCheckerStopAndRestartWaitsForOldRun(t *testing.T) {
	checker := NewDefaultHealthChecker(fastHealthConfig())
	checker.maxWorkers = 2
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker.StartMonitoring(parent)
	first := checker.run
	if first == nil {
		t.Fatal("monitoring did not start")
	}
	checker.StopMonitoring()
	select {
	case <-first.done:
	default:
		t.Fatal("StopMonitoring returned before all workers exited")
	}

	checker.StartMonitoring(parent)
	second := checker.run
	if second == nil || second == first || second.queue == first.queue {
		t.Fatal("restart must create an independent run and work queue")
	}
	checker.StopMonitoring()
	select {
	case <-second.done:
	default:
		t.Fatal("second run still has active workers")
	}
}

func TestHealthCheckerConfigRestartUsesOriginalContext(t *testing.T) {
	config := fastHealthConfig()
	checker := NewDefaultHealthChecker(config)
	checker.maxWorkers = 2
	parent, cancel := context.WithCancel(context.Background())
	defer checker.StopMonitoring()

	checker.StartMonitoring(parent)
	first := checker.run
	config.Interval *= 2
	if err := checker.UpdateConfig(config); err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}
	select {
	case <-first.done:
	default:
		t.Fatal("config restart did not wait for the old workers")
	}
	second := checker.run
	if second == nil || second == first {
		t.Fatal("interval change did not create a new run")
	}

	cancel()
	select {
	case <-second.done:
	case <-time.After(time.Second):
		t.Fatal("config-restarted workers ignored the original context")
	}
	config.Interval *= 2
	if err := checker.UpdateConfig(config); err != nil {
		t.Fatalf("UpdateConfig after parent cancellation failed: %v", err)
	}
	if checker.run != nil {
		t.Fatal("config update restarted monitoring after parent cancellation")
	}
}

func TestHealthCheckerEnableAfterDisabledStartUsesOriginalContext(t *testing.T) {
	config := fastHealthConfig()
	config.Enabled = false
	checker := NewDefaultHealthChecker(config)
	checker.maxWorkers = 2
	parent, cancel := context.WithCancel(context.Background())
	defer checker.StopMonitoring()

	checker.StartMonitoring(parent)
	if checker.run != nil {
		t.Fatal("disabled checker started workers")
	}
	config.Enabled = true
	if err := checker.UpdateConfig(config); err != nil {
		t.Fatalf("enabling checker failed: %v", err)
	}
	run := checker.run
	if run == nil {
		t.Fatal("enabling checker did not start monitoring")
	}
	cancel()
	select {
	case <-run.done:
	case <-time.After(time.Second):
		t.Fatal("enabled workers ignored the original context")
	}
}

func TestHealthCheckerCanceledStartDoesNotClaimLifecycle(t *testing.T) {
	checker := NewDefaultHealthChecker(fastHealthConfig())
	checker.maxWorkers = 2
	canceledCtx, cancelCanceledCtx := context.WithCancel(context.Background())
	cancelCanceledCtx()

	checker.StartMonitoring(canceledCtx)
	if checker.parentCtx != nil || checker.run != nil {
		t.Fatal("a canceled context must not claim the checker lifecycle")
	}

	validCtx, cancelValidCtx := context.WithCancel(context.Background())
	defer cancelValidCtx()
	defer checker.StopMonitoring()
	checker.StartMonitoring(validCtx)
	if checker.run == nil {
		t.Fatal("a rejected canceled start must not prevent a later valid start")
	}
}

func TestHealthListenerCanStopMonitoring(t *testing.T) {
	config := fastHealthConfig()
	checker := NewDefaultHealthChecker(config)
	checker.maxWorkers = 1
	node := NewMockNode("node-1", "127.0.0.1:6379")
	node.SetHealthy(false)
	checker.AddNode(node)

	stopped := make(chan struct{})
	checker.AddHealthListener(func(_ string, healthy bool) {
		if !healthy {
			checker.StopMonitoring()
			close(stopped)
		}
	})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	checker.StartMonitoring(parent)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("listener deadlocked while stopping its checker")
	}
}

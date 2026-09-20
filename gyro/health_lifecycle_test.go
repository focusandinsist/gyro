package gyro

import (
	"context"
	"testing"
	"time"
)

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

func TestHealthPoolTracksNodesAfterEnablingChecker(t *testing.T) {
	config := fastHealthConfig()
	config.Enabled = false
	locator, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatalf("NewConsistentLocator failed: %v", err)
	}
	node := NewMockNode("node-1", "127.0.0.1:6379")
	if err := locator.AddNode(node); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}
	pool := NewHealthAwarePoolWithChecker(locator, NewDefaultHealthChecker(config))
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer pool.Close()
	pool.StartHealthMonitoring(parent)

	node.SetHealthy(false)
	config.Enabled = true
	if err := pool.UpdateHealthCheckerConfig(config); err != nil {
		t.Fatalf("enabling checker failed: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for pool.IsNodeHealthy(node.ID()) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if pool.IsNodeHealthy(node.ID()) {
		t.Fatal("pool did not receive health events after enabling checker")
	}
}

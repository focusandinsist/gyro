package gyro

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type controllableHealthChecker struct {
	mu       sync.Mutex
	listener HealthListener
	config   HealthCheckerConfig
}

func (c *controllableHealthChecker) Check(context.Context, Node) error { return nil }

func (c *controllableHealthChecker) AddNode(Node) {}

func (c *controllableHealthChecker) RemoveNode(string) {}

func (c *controllableHealthChecker) StartMonitoring(context.Context) {}

func (c *controllableHealthChecker) StopMonitoring() {}

func (c *controllableHealthChecker) IsNodeHealthy(string) bool { return true }

func (c *controllableHealthChecker) AddHealthListener(listener HealthListener) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listener = listener
}

func (c *controllableHealthChecker) UpdateConfig(config HealthCheckerConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config = config
	return nil
}

func (c *controllableHealthChecker) GetConfig() HealthCheckerConfig {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config
}

func (c *controllableHealthChecker) IsEnabled() bool { return true }

func (c *controllableHealthChecker) Emit(nodeID string, healthy bool) {
	c.mu.Lock()
	listener := c.listener
	c.mu.Unlock()
	if listener != nil {
		listener(nodeID, healthy)
	}
}

func TestHealthAwarePoolCloseRejectsLateCallbacks(t *testing.T) {
	locator, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatalf("failed to create locator: %v", err)
	}

	checker := &controllableHealthChecker{config: DefaultHealthCheckerConfig()}
	pool := NewHealthAwarePoolWithChecker(locator, checker)
	pool.StartHealthMonitoring(context.Background())

	if err := pool.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	// The checker may still deliver a callback after Close because its listener
	// API is asynchronous. This must be ignored after the pool is torn down.
	checker.Emit("late-node", false)

	if err := pool.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}

type blockingGetAllLocator struct {
	Locator
	started chan struct{}
	release chan struct{}
}

func (l *blockingGetAllLocator) GetAllNodes() []Node {
	select {
	case <-l.started:
	default:
		close(l.started)
	}
	<-l.release
	return l.Locator.GetAllNodes()
}

func TestHealthAwarePoolReplaceLocatorSerializesWithClose(t *testing.T) {
	base, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	oldNode := NewMockNode("old", "old")
	if err := base.AddNodeContext(context.Background(), oldNode); err != nil {
		t.Fatal(err)
	}
	pool := NewHealthAwarePoolWithChecker(base, &controllableHealthChecker{config: DefaultHealthCheckerConfig()})

	blocking := &blockingGetAllLocator{
		Locator: pool.currentLocator(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	pool.snapshot.Store(&poolSnapshot{locator: blocking, healthyNodes: pool.GetHealthStatus()})

	replacement, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	newNode := NewMockNode("new", "new")
	if err := replacement.AddNodeContext(context.Background(), newNode); err != nil {
		t.Fatal(err)
	}

	replaceDone := make(chan error, 1)
	go func() { replaceDone <- pool.ReplaceLocator(replacement) }()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("ReplaceLocator did not start reading the old locator")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- pool.Close() }()
	close(blocking.release)

	if err := <-replaceDone; err != nil {
		t.Fatalf("ReplaceLocator failed: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if newNode.IsHealthy(context.Background()) {
		t.Fatal("replacement node remained open after concurrent pool Close")
	}
}

func TestHealthAwarePoolSmallClusterFailover(t *testing.T) {
	t.Run("0_nodes", func(t *testing.T) {
		locator, err := NewConsistentLocator(DefaultLocatorConfig())
		if err != nil {
			t.Fatalf("NewConsistentLocator failed: %v", err)
		}
		pool := NewHealthAwarePoolWithChecker(locator, &controllableHealthChecker{config: DefaultHealthCheckerConfig()})
		if _, err := pool.Get(context.Background(), "small-cluster-key"); err == nil {
			t.Fatal("Get with no nodes succeeded, want no-nodes error")
		}
	})

	for _, nodeCount := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d_nodes", nodeCount), func(t *testing.T) {
			locator, err := NewConsistentLocator(DefaultLocatorConfig())
			if err != nil {
				t.Fatalf("NewConsistentLocator failed: %v", err)
			}
			for i := 1; i <= nodeCount; i++ {
				node := NewMockNode(fmt.Sprintf("node-%d", i), fmt.Sprintf("127.0.0.1:%d", 6378+i))
				if err := locator.AddNodeContext(context.Background(), node); err != nil {
					t.Fatalf("AddNode failed: %v", err)
				}
			}

			checker := &controllableHealthChecker{config: DefaultHealthCheckerConfig()}
			pool := NewHealthAwarePoolWithChecker(locator, checker)
			pool.StartHealthMonitoring(context.Background())
			defer pool.Close()

			const key = "small-cluster-key"
			primary, err := locator.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Get primary failed: %v", err)
			}
			checker.Emit(primary.ID(), false)

			got, err := pool.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Get with %d nodes returned an error: %v", nodeCount, err)
			}
			if nodeCount == 1 && got.ID() != primary.ID() {
				t.Fatalf("single-node fallback returned %q, want primary %q", got.ID(), primary.ID())
			}
			if nodeCount == 2 && got.ID() == primary.ID() {
				t.Fatalf("two-node failover returned unhealthy primary %q", primary.ID())
			}
		})
	}
}

func TestHealthAwarePoolRemoveNodeClearsHealthSnapshot(t *testing.T) {
	locator, err := NewConsistentLocator(DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	node := NewMockNode("remove-me", "remove-me")
	if err := locator.AddNodeContext(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	pool := NewHealthAwarePoolWithChecker(locator, &controllableHealthChecker{config: DefaultHealthCheckerConfig()})
	if err := pool.RemoveNodeContext(context.Background(), node.ID()); err != nil {
		t.Fatal(err)
	}
	status := pool.GetHealthStatus()
	if _, exists := status[node.ID()]; exists {
		t.Fatalf("removed node remained in health snapshot: %#v", status)
	}
	if stats := pool.GetStats(); stats.TotalNodes != 0 {
		t.Fatalf("removed node remained in pool stats: %#v", stats)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
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
	if err := locator.AddNodeContext(context.Background(), node); err != nil {
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
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for pool.IsNodeHealthy(node.ID()) && time.Now().Before(deadline) {
		<-ticker.C
	}
	if pool.IsNodeHealthy(node.ID()) {
		t.Fatal("pool did not receive health events after enabling checker")
	}
}

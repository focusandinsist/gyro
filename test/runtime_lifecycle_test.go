package test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientpkg "gyro/client"
	gyrohealth "gyro/health"
	"gyro/discovery/static"
	"gyro/gyro"
	"gyro/internal/health"
	"gyro/internal/policy"
	"gyro/internal/resource"
	"gyro/internal/routing"
)

type trackedNode struct {
	id, address string
	closes      atomic.Int32
}

func (n *trackedNode) ID() string                     { return n.id }
func (n *trackedNode) Address() string                { return n.address }
func (n *trackedNode) IsHealthy(context.Context) bool { return true }
func (n *trackedNode) Close() error                   { n.closes.Add(1); return nil }

type trackedFactory struct {
	mu    sync.Mutex
	nodes []*trackedNode
}

func (f *trackedFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	node := &trackedNode{id: info.ID, address: info.Address}
	f.mu.Lock()
	f.nodes = append(f.nodes, node)
	f.mu.Unlock()
	return node, nil
}

func (f *trackedFactory) WithConnectionConfig(gyro.ConnectionConfig) (gyro.NodeFactory, error) {
	return f, nil
}

func (f *trackedFactory) snapshot() []*trackedNode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*trackedNode(nil), f.nodes...)
}

func TestRoutedLocatorClosesRemovedAndRemainingNodesOnce(t *testing.T) {
	locator, err := routing.NewLocator(gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	first := &trackedNode{id: "first", address: "first"}
	second := &trackedNode{id: "second", address: "second"}
	if err := locator.AddNodeContext(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := locator.AddNodeContext(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := locator.Get(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
	primary, err := locator.Get(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	replicas, err := locator.GetReplicas(context.Background(), "key", 2)
	if err != nil || len(replicas) != 2 || replicas[0].ID() != primary.ID() {
		t.Fatalf("replica order does not start at primary: %v, %v", replicas, err)
	}
	if err := locator.RemoveNodeContext(context.Background(), first.ID()); err != nil {
		t.Fatal(err)
	}
	if got := first.closes.Load(); got != 1 {
		t.Fatalf("removed node close count = %d, want 1", got)
	}
	if err := locator.Close(); err != nil {
		t.Fatal(err)
	}
	if err := locator.Close(); err != nil {
		t.Fatal(err)
	}
	if got := first.closes.Load(); got != 1 {
		t.Fatalf("removed node closed again: %d", got)
	}
	if got := second.closes.Load(); got != 1 {
		t.Fatalf("remaining node close count = %d, want 1", got)
	}
}

func TestDynamicClientClosesPreparedNodesAndCanRestart(t *testing.T) {
	factory := &trackedFactory{}
	config := clientpkg.DefaultConfig()
	config.HealthChecker.Enabled = false
	client, err := clientpkg.NewClient("service", static.New([]string{"node-a"}), clientpkg.NewConfigManager(config), factory, passiveChecker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := factory.snapshot()[0].closes.Load(); got != 1 {
		t.Fatalf("prepared node close count = %d", got)
	}
	for i := 0; i < 2; i++ {
		if err := client.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := client.GetNodeForKey(context.Background(), "key"); err != nil {
			t.Fatal(err)
		}
		if err := client.Stop(); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range factory.snapshot() {
		if got := node.closes.Load(); got != 1 {
			t.Fatalf("node %s closed %d times", node.id, got)
		}
	}
}

func TestDynamicClientConfigReplacementClosesOldNode(t *testing.T) {
	factory := &trackedFactory{}
	config := clientpkg.DefaultConfig()
	config.HealthChecker.Enabled = false
	manager := clientpkg.NewConfigManager(config)
	client, err := clientpkg.NewClient("service", static.New([]string{"node-a"}), manager, factory, passiveChecker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated := manager.GetConfig()
	updated.Connection.ConnectTimeout++
	if err := manager.UpdateConfig(updated); err != nil {
		t.Fatal(err)
	}
	nodes := factory.snapshot()
	if len(nodes) != 2 {
		t.Fatalf("created nodes = %d, want 2", len(nodes))
	}
	if got := nodes[0].closes.Load(); got != 1 {
		t.Fatalf("replaced node close count = %d", got)
	}
	if got := nodes[1].closes.Load(); got != 0 {
		t.Fatalf("active node closed early: %d", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := nodes[1].closes.Load(); got != 1 {
		t.Fatalf("active node close count = %d", got)
	}
}

type passiveChecker struct{}

func (passiveChecker) Check(context.Context, gyro.Node) error { return nil }
func (passiveChecker) AddNode(gyro.Node)                      {}
func (passiveChecker) RemoveNode(string)                      {}
func (passiveChecker) StartMonitoring(context.Context)        {}
func (passiveChecker) StopMonitoring()                        {}
func (passiveChecker) IsNodeHealthy(string) bool              { return true }
func (passiveChecker) AddHealthListener(gyro.HealthListener)  {}

type callbackChecker struct{ listener gyro.HealthListener }

func (*callbackChecker) Check(context.Context, gyro.Node) error           { return nil }
func (*callbackChecker) AddNode(gyro.Node)                                {}
func (*callbackChecker) RemoveNode(string)                                {}
func (*callbackChecker) StartMonitoring(context.Context)                  {}
func (*callbackChecker) StopMonitoring()                                  {}
func (*callbackChecker) IsNodeHealthy(string) bool                        { return true }
func (c *callbackChecker) AddHealthListener(listener gyro.HealthListener) { c.listener = listener }

func TestHealthObserverIgnoresLateCallbacksAfterRemoval(t *testing.T) {
	checker := &callbackChecker{}
	observer := health.NewObserver(checker, []gyro.Node{&trackedNode{id: "old"}})
	observer.Start(context.Background())
	observer.RemoveNode("old")
	checker.listener("old", false)
	if _, exists := observer.Snapshot()["old"]; exists {
		t.Fatal("removed node reappeared after late callback")
	}
	observer.Close()
	checker.listener("old", true)
	if _, exists := observer.Snapshot()["old"]; exists {
		t.Fatal("closed observer accepted late callback")
	}
}

type countingChecker struct {
	callbackChecker
	starts, stops, listeners int
}

func (c *countingChecker) StartMonitoring(context.Context) { c.starts++ }
func (c *countingChecker) StopMonitoring()                 { c.stops++ }
func (c *countingChecker) AddHealthListener(listener gyro.HealthListener) {
	c.listeners++
	c.callbackChecker.AddHealthListener(listener)
}

func TestHealthObserverMonitoringRestartsWithoutDuplicateListener(t *testing.T) {
	checker := &countingChecker{}
	observer := health.NewObserver(checker, []gyro.Node{&trackedNode{id: "node"}})
	observer.Start(context.Background())
	observer.Stop()
	observer.Start(context.Background())
	observer.Close()
	if checker.starts != 2 || checker.listeners != 1 || checker.stops != 2 {
		t.Fatalf("monitor lifecycle: starts=%d listeners=%d stops=%d", checker.starts, checker.listeners, checker.stops)
	}
}

type trackedResource struct{ *trackedNode }

func (r trackedResource) MemberID() string { return r.ID() }

func TestOwnedResourcePoolDefersCloseUntilLeaseRelease(t *testing.T) {
	pool := resource.NewOwnedPool()
	node := &trackedNode{id: "node"}
	if err := pool.Add(trackedResource{node}); err != nil {
		t.Fatal(err)
	}
	lease, err := pool.Acquire(context.Background(), node.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Remove(node.ID()); err != nil {
		t.Fatal(err)
	}
	if got := node.closes.Load(); got != 0 {
		t.Fatalf("borrowed node closed before release: %d", got)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := node.closes.Load(); got != 1 {
		t.Fatalf("node close count after release = %d", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if got := node.closes.Load(); got != 1 {
		t.Fatalf("node closed twice: %d", got)
	}
}

func TestRoutedPoolReplacementWaitsForBorrowedNode(t *testing.T) {
	base, err := routing.NewLocator(gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := &trackedNode{id: "old", address: "old"}
	if err := base.AddNodeContext(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	pool := routing.NewHealthAwarePoolWithChecker(base, passiveChecker{})
	lease, err := pool.BorrowNodeForKey(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	next, err := routing.NewLocator(gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	current := &trackedNode{id: "current", address: "current"}
	if err := next.AddNodeContext(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if err := pool.ReplaceLocator(next); err != nil {
		t.Fatal(err)
	}
	if got := old.closes.Load(); got != 0 {
		t.Fatalf("borrowed old node closed during replacement: %d", got)
	}
	if lease.Node().ID() != old.ID() {
		t.Fatalf("borrowed node changed to %s", lease.Node().ID())
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := old.closes.Load(); got != 1 {
		t.Fatalf("old node close count = %d", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if got := current.closes.Load(); got != 1 {
		t.Fatalf("current node close count = %d", got)
	}
}

type blockingPolicy struct {
	entered chan struct{}
	resume  chan struct{}
}

func (p blockingPolicy) Decide(ctx context.Context, request gyro.RouteRequest, snapshot gyro.TopologySnapshot, candidates gyro.CandidateSet, view gyro.HealthView) (gyro.RouteDecision, error) {
	close(p.entered)
	<-p.resume
	return (policy.PrimaryOnly{}).Decide(ctx, request, snapshot, candidates, view)
}

func TestRoutedPoolReplacementWaitsForRouteLease(t *testing.T) {
	base, err := routing.NewLocator(gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := &trackedNode{id: "old", address: "old"}
	if err := base.AddNodeContext(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	gate := blockingPolicy{entered: make(chan struct{}), resume: make(chan struct{})}
	pool := routing.NewHealthAwarePoolWithCheckerAndPolicy(base, passiveChecker{}, gate)
	borrowed := make(chan *routing.NodeLease, 1)
	borrowErr := make(chan error, 1)
	go func() {
		lease, err := pool.BorrowNodeForKey(context.Background(), "key")
		borrowed <- lease
		borrowErr <- err
	}()
	<-gate.entered
	next, err := routing.NewLocator(gyro.DefaultLocatorConfig())
	if err != nil {
		close(gate.resume)
		t.Fatal(err)
	}
	current := &trackedNode{id: "current", address: "current"}
	if err := next.AddNodeContext(context.Background(), current); err != nil {
		close(gate.resume)
		t.Fatal(err)
	}
	replaced := make(chan error, 1)
	go func() { replaced <- pool.ReplaceLocator(next) }()
	select {
	case err := <-replaced:
		close(gate.resume)
		t.Fatalf("replacement completed before route acquired a lease: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gate.resume)
	lease := <-borrowed
	if err := <-borrowErr; err != nil {
		t.Fatal(err)
	}
	if lease.Node() != old {
		t.Fatal("route did not borrow the original node")
	}
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	if got := old.closes.Load(); got != 0 {
		t.Fatalf("borrowed node closed before release: %d", got)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := old.closes.Load(); got != 1 {
		t.Fatalf("old node close count = %d", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

type probeNode struct {
	trackedNode
	healthy atomic.Bool
}

func (n *probeNode) IsHealthy(context.Context) bool { return n.healthy.Load() }

func TestHealthCheckerStopsAndRestartsWithoutOldWorkers(t *testing.T) {
	config := gyrohealth.DefaultConfig()
	config.Interval = 5 * time.Millisecond
	config.Timeout = 50 * time.Millisecond
	config.FailureThreshold = 1
	config.RecoveryThreshold = 1
	checker := gyrohealth.NewChecker(config)
	node := &probeNode{trackedNode: trackedNode{id: "node"}}
	node.healthy.Store(true)
	checker.AddNode(node)
	checker.StartMonitoring(context.Background())
	waitForHealthStatus(t, checker, node.ID(), gyro.Healthy)
	node.healthy.Store(false)
	waitForHealthStatus(t, checker, node.ID(), gyro.Unhealthy)
	checker.StopMonitoring()
	node.healthy.Store(true)
	time.Sleep(3 * config.Interval)
	if got := checker.Status(node.ID()); got != gyro.Unhealthy {
		t.Fatalf("stopped checker changed status to %v", got)
	}
	checker.StartMonitoring(context.Background())
	defer checker.StopMonitoring()
	waitForHealthStatus(t, checker, node.ID(), gyro.Healthy)
}

func waitForHealthStatus(t *testing.T, view gyro.HealthView, id string, want gyro.HealthStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if view.Status(id) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("health status = %v, want %v", view.Status(id), want)
}

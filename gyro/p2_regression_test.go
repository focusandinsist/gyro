package gyro

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConfigManagerSnapshotsAreIsolatedAndFailedUpdateIsNotPublished(t *testing.T) {
	config := DefaultClientConfig()
	manager := NewConfigManager(config)
	config.Connection.MaxIdleConns = 99
	if got := manager.GetConfig().Connection.MaxIdleConns; got == 99 {
		t.Fatal("constructor retained caller-owned config")
	}
	manager.AddConfigWatcher(func(oldConfig, newConfig *ClientConfig) error {
		oldConfig.Connection.MaxIdleConns = 77
		newConfig.Connection.MaxIdleConns = 88
		return errors.New("reject")
	})
	update := manager.GetConfig()
	update.Connection.MaxIdleConns = 42
	if err := manager.UpdateConfig(update); err == nil {
		t.Fatal("expected watcher failure")
	}
	if got := manager.GetConfig().Connection.MaxIdleConns; got != DefaultConnectionConfig().MaxIdleConns {
		t.Fatalf("failed update was published: %d", got)
	}
}

type blockingHealthNode struct {
	id      string
	started chan struct{}
	release chan struct{}
}

func (n *blockingHealthNode) ID() string      { return n.id }
func (n *blockingHealthNode) Address() string { return n.id }
func (n *blockingHealthNode) Close() error    { return nil }
func (n *blockingHealthNode) IsHealthy(ctx context.Context) bool {
	select {
	case <-n.started:
	default:
		close(n.started)
	}
	select {
	case <-n.release:
		return false
	case <-ctx.Done():
		return false
	}
}

func TestHealthCheckDoesNotHoldLockDuringProbeOrCommitStaleResult(t *testing.T) {
	config := DefaultHealthCheckerConfig()
	config.Timeout = time.Second
	config.FailureThreshold = 1
	checker := NewDefaultHealthChecker(config)
	oldNode := &blockingHealthNode{id: "node", started: make(chan struct{}), release: make(chan struct{})}
	checker.AddNode(oldNode)
	done := make(chan struct{})
	go func() { _ = checker.Check(context.Background(), oldNode); close(done) }()
	select {
	case <-oldNode.started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	newNode := NewMockNode("node", "new")
	checker.RemoveNode("node")
	checker.AddNode(newNode)
	close(oldNode.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not finish")
	}
	stats := checker.GetNodeStats("node")
	if stats == nil || stats.TotalChecks != 0 {
		t.Fatalf("stale probe committed to replacement node: %+v", stats)
	}
}

func TestNodeNeedsUpdateIncludesMetadataAndWeight(t *testing.T) {
	client := &Client{nodeInfos: map[string]NodeInfo{
		"node": {ID: "node", Address: "addr", Metadata: map[string]string{"zone": "a"}, Weight: 1},
	}}
	node := NewMockNode("node", "addr")
	if client.nodeNeedsUpdate(node, NodeInfo{ID: "node", Address: "addr", Metadata: map[string]string{"zone": "b"}, Weight: 1}) == false {
		t.Fatal("metadata change was ignored")
	}
	if client.nodeNeedsUpdate(node, NodeInfo{ID: "node", Address: "addr", Metadata: map[string]string{"zone": "a"}, Weight: 2}) == false {
		t.Fatal("weight change was ignored")
	}
	if client.nodeNeedsUpdate(node, NodeInfo{ID: "node", Address: "addr", Metadata: map[string]string{"zone": "a"}, Weight: 1}) {
		t.Fatal("unchanged node info requires an unnecessary update")
	}
}

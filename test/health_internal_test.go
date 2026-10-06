package test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gyro"
	"gyro/internal/health"
)

type internalHealthNode struct {
	healthy atomic.Bool
}

func (n *internalHealthNode) ID() string                     { return "internal-health-node" }
func (n *internalHealthNode) Address() string                { return "internal-health-node" }
func (n *internalHealthNode) IsHealthy(context.Context) bool { return n.healthy.Load() }
func (*internalHealthNode) Close() error                     { return nil }

func TestInternalHealthCheckerPublishesThresholdedSnapshot(t *testing.T) {
	config := gyro.DefaultHealthCheckerConfig()
	config.Enabled = false
	config.Timeout = time.Second
	config.FailureThreshold = 2
	config.RecoveryThreshold = 1
	checker := health.NewDefaultHealthChecker(config)
	node := &internalHealthNode{}
	node.healthy.Store(true)
	checker.AddNode(node)
	if got := checker.Status(node.ID()); got != gyro.Unknown {
		t.Fatalf("initial status = %v, want Unknown", got)
	}
	node.healthy.Store(false)
	if err := checker.Check(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if got := checker.Status(node.ID()); got != gyro.Healthy {
		t.Fatalf("status after one failure = %v, want Healthy", got)
	}
	if err := checker.Check(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if got := checker.Status(node.ID()); got != gyro.Unhealthy {
		t.Fatalf("status after threshold = %v, want Unhealthy", got)
	}
	if snapshot := checker.Snapshot(); snapshot[node.ID()] != gyro.Unhealthy {
		t.Fatalf("snapshot = %#v, want unhealthy node", snapshot)
	}
}

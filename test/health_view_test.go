package test

import (
	"context"
	"testing"

	"gyro/gyro"
	"gyro/health"
)

type healthViewNode struct{}

func (healthViewNode) ID() string                     { return "node-a" }
func (healthViewNode) Address() string                { return "node-a" }
func (healthViewNode) IsHealthy(context.Context) bool { return true }
func (healthViewNode) Close() error                   { return nil }

func TestDefaultHealthCheckerPublishesUnknownUntilFirstProbeAndCleansRemovedNode(t *testing.T) {
	checker := health.NewChecker(gyro.DefaultHealthCheckerConfig())
	node := healthViewNode{}
	checker.AddNode(node)
	if got := checker.Status("node-a"); got != gyro.Unknown {
		t.Fatalf("initial status = %v, want Unknown", got)
	}
	if got := checker.Status("missing"); got != gyro.Unknown {
		t.Fatalf("missing status = %v, want Unknown", got)
	}
	if err := checker.Check(context.Background(), node); err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if got := checker.Status("node-a"); got != gyro.Healthy {
		t.Fatalf("post-probe status = %v, want Healthy", got)
	}
	view := checker.Snapshot()
	view["node-a"] = gyro.Unhealthy
	if got := checker.Status("node-a"); got != gyro.Healthy {
		t.Fatalf("mutating Snapshot changed checker status to %v", got)
	}
	checker.RemoveNode("node-a")
	if got := checker.Status("node-a"); got != gyro.Unknown {
		t.Fatalf("removed status = %v, want Unknown", got)
	}
}

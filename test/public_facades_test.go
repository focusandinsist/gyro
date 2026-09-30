package test

import (
	"context"
	"testing"

	gyrohealth "gyro/health"
	"gyro/discovery/static"
)

func TestPublicRuntimeFacadesExposeInternalImplementations(t *testing.T) {
	discovery := static.New([]string{"node-a:1", "node-b:1"})
	snapshot, err := discovery.Discover(context.Background(), "default")
	if err != nil {
		t.Fatalf("static discovery failed: %v", err)
	}
	if len(snapshot.Members) != 2 {
		t.Fatalf("members = %d, want 2", len(snapshot.Members))
	}

	checker := gyrohealth.NewChecker(gyrohealth.DefaultConfig())
	if checker == nil {
		t.Fatal("health facade returned nil checker")
	}
	_ = checker.StopMonitoring
}

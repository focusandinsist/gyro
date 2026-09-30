package test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gyro/gyro"
	"gyro/internal/selector"
)

func TestRendezvousSelectorIsDeterministicAndOrderIndependent(t *testing.T) {
	sel := selector.NewRendezvousSelector("production-v1")
	request := gyro.RouteRequest{Key: "tenant-42"}
	left, err := sel.Select(context.Background(), request, selectorSnapshot("node-a", "node-b", "node-c"))
	if err != nil {
		t.Fatalf("first Select failed: %v", err)
	}
	right, err := selector.NewRendezvousSelector("production-v1").Select(context.Background(), request, selectorSnapshot("node-c", "node-a", "node-b"))
	if err != nil {
		t.Fatalf("second Select failed: %v", err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("selection depends on member order: left=%#v right=%#v", left, right)
	}
	if len(left.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want 3", len(left.Candidates))
	}
}

func TestRendezvousSelectorMemberChangesOnlyProduceCandidatesFromCurrentTopology(t *testing.T) {
	sel := selector.NewRendezvousSelector("production-v1")
	before, err := sel.Select(context.Background(), gyro.RouteRequest{Key: "tenant-42"}, selectorSnapshot("a", "b", "c"))
	if err != nil {
		t.Fatalf("before Select failed: %v", err)
	}
	after, err := sel.Select(context.Background(), gyro.RouteRequest{Key: "tenant-42"}, selectorSnapshot("a", "b", "c", "d"))
	if err != nil {
		t.Fatalf("after Select failed: %v", err)
	}
	if len(before.Candidates) != 3 || len(after.Candidates) != 4 {
		t.Fatalf("candidate counts = %d/%d", len(before.Candidates), len(after.Candidates))
	}
	for _, candidate := range after.Candidates {
		if candidate.MemberID == "" {
			t.Fatal("selector returned an empty member ID")
		}
	}
}

func TestRendezvousSelectorValidatesRequestAndCancellation(t *testing.T) {
	sel := selector.NewRendezvousSelector("")
	if _, err := sel.Select(context.Background(), gyro.RouteRequest{}, selectorSnapshot("a")); !errors.Is(err, gyro.ErrInvalidRequest) {
		t.Fatalf("empty key error = %v, want ErrInvalidRequest", err)
	}
	if _, err := sel.Select(context.Background(), gyro.RouteRequest{Key: "key"}, selectorSnapshot()); !errors.Is(err, gyro.ErrNoMembers) {
		t.Fatalf("empty topology error = %v, want ErrNoMembers", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sel.Select(ctx, gyro.RouteRequest{Key: "key"}, selectorSnapshot("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Select error = %v, want context.Canceled", err)
	}
}

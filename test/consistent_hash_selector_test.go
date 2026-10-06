package test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gyro"
)

func selectorSnapshot(ids ...string) gyro.TopologySnapshot {
	members := make([]gyro.Member, len(ids))
	for i, id := range ids {
		members[i] = gyro.Member{ID: id}
	}
	return gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "selector-test", Generation: 1, Token: "1"},
		Members:  members,
	}
}

func TestConsistentHashSelectorIsDeterministicAndOrderIndependent(t *testing.T) {
	config := gyro.DefaultLocatorConfig()
	first, err := gyro.NewConsistentHashSelector(config)
	if err != nil {
		t.Fatalf("NewConsistentHashSelector failed: %v", err)
	}
	second, err := gyro.NewConsistentHashSelector(config)
	if err != nil {
		t.Fatalf("second selector failed: %v", err)
	}
	request := gyro.RouteRequest{Key: "routing-regression-key"}
	left, err := first.Select(context.Background(), request, selectorSnapshot("node-a", "node-b", "node-c"))
	if err != nil {
		t.Fatalf("first Select failed: %v", err)
	}
	right, err := second.Select(context.Background(), request, selectorSnapshot("node-c", "node-a", "node-b"))
	if err != nil {
		t.Fatalf("second Select failed: %v", err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("selection depends on member order or selector instance: left=%#v right=%#v", left, right)
	}
	if got := candidateIDs(left); !reflect.DeepEqual(got, []string{"node-b", "node-c", "node-a"}) {
		t.Fatalf("routing regression sample = %#v, want [node-b node-c node-a]", got)
	}
}

func TestConsistentHashSelectorValidatesInputAndPropagatesCancellation(t *testing.T) {
	selector, err := gyro.NewConsistentHashSelector(gyro.DefaultLocatorConfig())
	if err != nil {
		t.Fatalf("NewConsistentHashSelector failed: %v", err)
	}
	if _, err := selector.Select(context.Background(), gyro.RouteRequest{Key: "key"}, selectorSnapshot()); !errors.Is(err, gyro.ErrNoMembers) {
		t.Fatalf("empty topology error = %v, want ErrNoMembers", err)
	}
	if _, err := selector.Select(context.Background(), gyro.RouteRequest{}, selectorSnapshot("node-a")); !errors.Is(err, gyro.ErrInvalidRequest) {
		t.Fatalf("empty key error = %v, want ErrInvalidRequest", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := selector.Select(canceled, gyro.RouteRequest{Key: "key"}, selectorSnapshot("node-a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled selection error = %v, want context.Canceled", err)
	}
}

func candidateIDs(selection gyro.CandidateSet) []string {
	ids := make([]string, len(selection.Candidates))
	for i, candidate := range selection.Candidates {
		ids[i] = candidate.MemberID
	}
	return ids
}

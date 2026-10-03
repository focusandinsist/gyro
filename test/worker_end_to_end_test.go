package test

import (
	"context"
	"errors"
	"testing"

	"gyro"
	"gyro/internal/policy"
	"gyro/internal/resource"
	"gyro/internal/selector"
	"gyro/internal/topology"
)

type workerResource struct {
	id     string
	closed bool
}

func (r *workerResource) MemberID() string { return r.id }
func (r *workerResource) Close() error     { r.closed = true; return nil }

type workerFactory struct{ resources []*workerResource }

func (f *workerFactory) Create(_ context.Context, member gyro.Member) (gyro.Resource, error) {
	resource := &workerResource{id: member.ID}
	f.resources = append(f.resources, resource)
	return resource, nil
}

type workerHealth map[string]gyro.HealthStatus

func (h workerHealth) Status(id string) gyro.HealthStatus { return h[id] }
func (h workerHealth) Snapshot() map[string]gyro.HealthStatus {
	copy := make(map[string]gyro.HealthStatus, len(h))
	for id, status := range h {
		copy[id] = status
	}
	return copy
}

func workerSnapshot(source string, generation uint64, ids ...string) gyro.TopologySnapshot {
	members := make([]gyro.Member, len(ids))
	for i, id := range ids {
		members[i] = gyro.Member{ID: id, Endpoints: []gyro.Endpoint{{Address: id + ":8080"}}}
	}
	return gyro.TopologySnapshot{Revision: gyro.Revision{Source: source, Generation: generation, Token: string(rune(generation))}, Members: members}
}

func TestWorkerRoutingEndToEndUsesTopologySelectorPolicyAndResources(t *testing.T) {
	ctx := context.Background()
	sel := selector.NewRendezvousSelector("worker-v1")
	snapshot := workerSnapshot("workers", 1, "worker-a", "worker-b", "worker-c")
	other, err := sel.Select(ctx, gyro.RouteRequest{Key: "task-42"}, snapshot)
	if err != nil {
		t.Fatalf("first selector failed: %v", err)
	}
	second, err := selector.NewRendezvousSelector("worker-v1").Select(ctx, gyro.RouteRequest{Key: "task-42"}, snapshot)
	if err != nil {
		t.Fatalf("second selector failed: %v", err)
	}
	if other.Candidates[0] != second.Candidates[0] {
		t.Fatalf("independent workers disagreed: %#v vs %#v", other, second)
	}

	factory := &workerFactory{}
	resources, err := resource.NewResourcePool(factory)
	if err != nil {
		t.Fatalf("resource pool failed: %v", err)
	}
	if err := resources.Replace(ctx, snapshot.Members); err != nil {
		t.Fatalf("resource prepare failed: %v", err)
	}
	handle, err := resources.Acquire(ctx, other.Candidates[0].MemberID)
	if err != nil {
		t.Fatalf("resource acquire failed: %v", err)
	}
	if handle.Resource().MemberID() != other.Candidates[0].MemberID {
		t.Fatal("resource identity did not match route")
	}
	if err := handle.Release(); err != nil {
		t.Fatalf("resource release failed: %v", err)
	}
	if err := resources.Close(); err != nil {
		t.Fatalf("resource close failed: %v", err)
	}
	for _, resource := range factory.resources {
		if !resource.closed {
			t.Fatalf("resource %s was not closed", resource.id)
		}
	}

	selection := gyro.CandidateSet{Revision: snapshot.Revision, Candidates: []gyro.Candidate{{MemberID: "worker-a"}, {MemberID: "worker-b"}}}
	if _, err := (policy.PrimaryOnly{}).Decide(ctx, gyro.RouteRequest{Key: "task-42"}, snapshot, selection, workerHealth{"worker-a": gyro.Unhealthy, "worker-b": gyro.Healthy}); !errors.Is(err, gyro.ErrFailoverNotAllowed) {
		t.Fatalf("PrimaryOnly error = %v, want ErrFailoverNotAllowed", err)
	}
	decision, err := (policy.HealthyCandidate{}).Decide(ctx, gyro.RouteRequest{Key: "task-42"}, snapshot, selection, workerHealth{"worker-a": gyro.Unhealthy, "worker-b": gyro.Healthy})
	if err != nil || decision.Primary.ID != "worker-b" {
		t.Fatalf("HealthyCandidate decision = %#v, error=%v", decision, err)
	}

	store := topology.NewStore()
	if err := store.Publish(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(ctx, workerSnapshot("workers", 0, "old-worker")); !errors.Is(err, gyro.ErrStaleRevision) {
		t.Fatalf("old event error = %v, want ErrStaleRevision", err)
	}
}

func TestWorkerMembershipChangeMigratesOnlyPartOfKeys(t *testing.T) {
	selector := selector.NewRendezvousSelector("worker-v1")
	before := workerSnapshot("workers", 1, "a", "b", "c")
	after := workerSnapshot("workers", 2, "a", "b", "c", "d")
	unchanged, changed := 0, 0
	for i := 0; i < 100; i++ {
		key := "task-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		left, err := selector.Select(context.Background(), gyro.RouteRequest{Key: key}, before)
		if err != nil {
			t.Fatal(err)
		}
		right, err := selector.Select(context.Background(), gyro.RouteRequest{Key: key}, after)
		if err != nil {
			t.Fatal(err)
		}
		if left.Candidates[0].MemberID == right.Candidates[0].MemberID {
			unchanged++
		} else {
			changed++
		}
	}
	if unchanged == 0 || changed == 0 {
		t.Fatalf("membership change did not show partial migration: unchanged=%d changed=%d", unchanged, changed)
	}
}

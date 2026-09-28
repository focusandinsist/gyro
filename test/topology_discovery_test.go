package test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
)

func TestStaticDiscoveryPublishesVersionedSnapshotsAndCoalescesUpdates(t *testing.T) {
	discovery := gyro.NewStaticServiceDiscovery([]string{"node-a", "node-b"})
	ctx := context.Background()
	stream, err := discovery.Watch(ctx, "default")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer stream.Close()

	first, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("initial Next failed: %v", err)
	}
	if first.Revision.Source == "" || first.Revision.Generation == 0 || len(first.Members) != 2 {
		t.Fatalf("invalid initial snapshot: %#v", first)
	}

	discovery.UpdateNodes("default", []string{"node-b", "node-a"})
	second, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("reordered Next failed: %v", err)
	}
	if second.Revision.Source != first.Revision.Source || second.Revision.Generation <= first.Revision.Generation {
		t.Fatalf("revision did not advance within source: first=%#v second=%#v", first.Revision, second.Revision)
	}
	diff := gyro.DiffTopologySnapshots(first, second)
	if len(diff.Added) != 0 || len(diff.Removed) != 0 || len(diff.Updated) != 0 {
		t.Fatalf("member reorder produced a topology change: %#v", diff)
	}

	discovery.UpdateNodes("default", []string{"node-c"})
	third, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("updated Next failed: %v", err)
	}
	diff = gyro.DiffTopologySnapshots(second, third)
	if len(diff.Added) != 1 || diff.Added[0].ID != "node-c" || len(diff.Removed) != 2 {
		t.Fatalf("unexpected topology diff: %#v", diff)
	}
}

func TestStaticDiscoveryStreamCloseStopsNextAndIsIdempotent(t *testing.T) {
	discovery := gyro.NewStaticServiceDiscovery(nil)
	stream, err := discovery.Watch(context.Background(), "default")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("initial Next failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close error = %v, want io.EOF", err)
	}
}

func TestServiceTopologySourceScopesServiceDiscovery(t *testing.T) {
	discovery := gyro.NewStaticServiceDiscovery(nil)
	discovery.SetNodes("orders", []gyro.NodeInfo{{ID: "orders-1", Address: "orders:1"}})
	source, err := gyro.NewServiceTopologySource(discovery, "orders")
	if err != nil {
		t.Fatalf("NewServiceTopologySource failed: %v", err)
	}
	snapshot, err := source.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(snapshot.Members) != 1 || snapshot.Members[0].ID != "orders-1" {
		t.Fatalf("source returned wrong service: %#v", snapshot)
	}
}

func TestTopologyDiffReturnsDetachedMembers(t *testing.T) {
	previous := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 1},
		Members:  []gyro.Member{{ID: "member", Endpoints: []gyro.Endpoint{{Address: "old", Attributes: map[string]string{"zone": "a"}}}}},
	}
	current := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 2},
		Members:  []gyro.Member{{ID: "member", Endpoints: []gyro.Endpoint{{Address: "new", Attributes: map[string]string{"zone": "b"}}}}},
	}
	diff := gyro.DiffTopologySnapshots(previous, current)
	if len(diff.Updated) != 1 || diff.Updated[0].Endpoints[0].Address != "new" {
		t.Fatalf("unexpected update diff: %#v", diff)
	}
	diff.Updated[0].Endpoints[0].Attributes["zone"] = "mutated"
	if current.Members[0].Endpoints[0].Attributes["zone"] != "b" {
		t.Fatal("diff exposed current snapshot state")
	}
}

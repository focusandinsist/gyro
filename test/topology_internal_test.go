package test

import (
	"context"
	"errors"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/topology"
)

func TestInternalTopologyStoreRejectsStaleSnapshots(t *testing.T) {
	store := topology.NewStore()
	current := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "source", Generation: 2, Token: "v2"}, Members: []gyro.Member{{ID: "a", Endpoints: []gyro.Endpoint{{Address: "a:1"}}}}}
	if err := store.Publish(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	stale := current
	stale.Revision.Generation = 1
	if err := store.Publish(context.Background(), stale); !errors.Is(err, gyro.ErrStaleRevision) {
		t.Fatalf("error = %v, want ErrStaleRevision", err)
	}
}

func TestInternalTopologyDiffDoesNotExposeInputMembers(t *testing.T) {
	previous := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "source", Generation: 1}, Members: []gyro.Member{{ID: "a", Endpoints: []gyro.Endpoint{{Address: "a:1"}}}}}
	current := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "source", Generation: 2}, Members: []gyro.Member{{ID: "a", Endpoints: []gyro.Endpoint{{Address: "a:2"}}}}}
	diff := topology.Diff(previous, current)
	if len(diff.Updated) != 1 {
		t.Fatalf("updated = %#v, want one member", diff.Updated)
	}
	diff.Updated[0].Endpoints[0].Address = "mutated"
	if current.Members[0].Endpoints[0].Address != "a:2" {
		t.Fatal("diff exposed the input member")
	}
}

func TestInternalStaticDiscoveryPublishesCompleteSnapshots(t *testing.T) {
	discovery := topology.NewStaticDiscovery([]string{"node-a"})
	stream, err := discovery.Watch(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Next(context.Background())
	if err != nil || len(first.Members) != 1 {
		t.Fatalf("first snapshot = %#v, err=%v", first, err)
	}
	if err := discovery.UpdateNodes("default", []string{"node-b"}); err != nil {
		t.Fatal(err)
	}
	second, err := stream.Next(context.Background())
	if err != nil || second.Members[0].ID != "node-b" {
		t.Fatalf("second snapshot = %#v, err=%v", second, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

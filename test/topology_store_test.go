package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
)

func TestTopologyStorePublishesAndNormalizesCompleteSnapshots(t *testing.T) {
	store := gyro.NewTopologyStore()
	input := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-a", Generation: 1, Token: "v1"},
		Members: []gyro.Member{
			{ID: "member-b", Endpoints: []gyro.Endpoint{{Address: "b:2"}, {Address: "b:1"}}},
			{ID: "member-a", Endpoints: []gyro.Endpoint{{Address: "a:1"}}},
		},
	}

	if err := store.Publish(context.Background(), input); err != nil {
		t.Fatalf("first Publish failed: %v", err)
	}
	got, ok := store.Snapshot()
	if !ok {
		t.Fatal("Snapshot reported no published value")
	}
	if got.Members[0].ID != "member-a" || got.Members[1].ID != "member-b" {
		t.Fatalf("members were not normalized by ID: %#v", got.Members)
	}
	if got.Members[1].Endpoints[0].Address != "b:1" {
		t.Fatalf("endpoints were not normalized by address: %#v", got.Members[1].Endpoints)
	}

	if err := store.Publish(context.Background(), input); err != nil {
		t.Fatalf("identical Publish should be idempotent: %v", err)
	}
	if _, ok := store.Snapshot(); !ok {
		t.Fatal("idempotent Publish removed the active snapshot")
	}
}

func TestTopologyStoreRevisionRules(t *testing.T) {
	store := gyro.NewTopologyStore()
	base := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "source-a", Generation: 2, Token: "v2"}}
	if err := store.Publish(context.Background(), base); err != nil {
		t.Fatalf("base Publish failed: %v", err)
	}

	stale := base
	stale.Revision.Generation = 1
	if err := store.Publish(context.Background(), stale); !errors.Is(err, gyro.ErrStaleRevision) {
		t.Fatalf("stale Publish error = %v, want ErrStaleRevision", err)
	}

	conflict := base
	conflict.Revision.Token = "different"
	if err := store.Publish(context.Background(), conflict); !errors.Is(err, gyro.ErrRevisionConflict) {
		t.Fatalf("conflicting Publish error = %v, want ErrRevisionConflict", err)
	}

	otherSource := base
	otherSource.Revision.Source = "source-b"
	if err := store.Publish(context.Background(), otherSource); !errors.Is(err, gyro.ErrIncomparableRevision) {
		t.Fatalf("cross-source Publish error = %v, want ErrIncomparableRevision", err)
	}
}

func TestTopologyStoreResetSourceReplacesActiveSource(t *testing.T) {
	store := gyro.NewTopologyStore()
	if err := store.Publish(context.Background(), gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-a", Generation: 99, Token: "old"},
	}); err != nil {
		t.Fatalf("initial Publish failed: %v", err)
	}

	reset := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-b", Generation: 1, Token: "new"},
		Members:  []gyro.Member{{ID: "new-member", Endpoints: []gyro.Endpoint{{Address: "new:1"}}}},
	}
	if err := store.ResetSource(context.Background(), reset); err != nil {
		t.Fatalf("ResetSource failed: %v", err)
	}
	got, ok := store.Snapshot()
	if !ok || got.Revision.Source != "source-b" || got.Members[0].ID != "new-member" {
		t.Fatalf("ResetSource did not replace active source: %#v", got)
	}
	if err := store.Publish(context.Background(), gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-a", Generation: 100, Token: "late"},
	}); !errors.Is(err, gyro.ErrIncomparableRevision) {
		t.Fatalf("old-source Publish error = %v, want ErrIncomparableRevision", err)
	}
}

func TestTopologyStoreSameSourceResetCannotRollBack(t *testing.T) {
	store := gyro.NewTopologyStore()
	current := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "source", Generation: 2, Token: "v2"}}
	if err := store.Publish(context.Background(), current); err != nil {
		t.Fatalf("initial Publish failed: %v", err)
	}
	if err := store.ResetSource(context.Background(), gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 1, Token: "v1"},
	}); !errors.Is(err, gyro.ErrStaleRevision) {
		t.Fatalf("same-source rollback error = %v, want ErrStaleRevision", err)
	}
	got, _ := store.Snapshot()
	if got.Revision != current.Revision {
		t.Fatalf("same-source rollback changed revision: %#v", got.Revision)
	}
}

func TestTopologyStoreRejectsInvalidSnapshots(t *testing.T) {
	cases := []gyro.TopologySnapshot{
		{Revision: gyro.Revision{Generation: 1}},
		{Revision: gyro.Revision{Source: "source", Generation: 1}, Members: []gyro.Member{{ID: ""}}},
		{Revision: gyro.Revision{Source: "source", Generation: 1}, Members: []gyro.Member{{ID: "same"}, {ID: "same"}}},
		{Revision: gyro.Revision{Source: "source", Generation: 1}, Members: []gyro.Member{{ID: "member", Endpoints: []gyro.Endpoint{{Address: ""}}}}},
		{Revision: gyro.Revision{Source: "source", Generation: 1}, Members: []gyro.Member{{ID: "member", Endpoints: []gyro.Endpoint{{Address: "same"}, {Address: "same"}}}}},
	}
	for i, snapshot := range cases {
		store := gyro.NewTopologyStore()
		if err := store.Publish(context.Background(), snapshot); !errors.Is(err, gyro.ErrInvalidSnapshot) {
			t.Errorf("case %d error = %v, want ErrInvalidSnapshot", i, err)
		}
		if _, ok := store.Snapshot(); ok {
			t.Errorf("case %d published an invalid snapshot", i)
		}
	}
}

func TestTopologyStoreTreatsEndpointAttributesAsPartOfIdentity(t *testing.T) {
	store := gyro.NewTopologyStore()
	snapshot := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 1},
		Members: []gyro.Member{{
			ID: "member",
			Endpoints: []gyro.Endpoint{
				{Address: "same", Attributes: map[string]string{"tls": "internal"}},
				{Address: "same", Attributes: map[string]string{"tls": "external"}},
			},
		}},
	}
	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("distinct endpoint attributes should be accepted: %v", err)
	}
	got, _ := store.Snapshot()
	if len(got.Members[0].Endpoints) != 2 || got.Members[0].Endpoints[0].Attributes["tls"] != "external" {
		t.Fatalf("endpoint attributes were not canonically ordered: %#v", got.Members[0].Endpoints)
	}
}

func TestTopologyStoreSnapshotsAreDeeplyIsolated(t *testing.T) {
	store := gyro.NewTopologyStore()
	input := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: 1, Token: "v1"},
		Members: []gyro.Member{{
			ID:         "member",
			Endpoints:  []gyro.Endpoint{{Address: "member:1", Attributes: map[string]string{"zone": "a"}}},
			Attributes: map[string]string{"role": "primary"},
		}},
	}
	if err := store.Publish(context.Background(), input); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	input.Members[0].ID = "caller-mutated"
	input.Members[0].Attributes["role"] = "caller-mutated"
	input.Members[0].Endpoints[0].Attributes["zone"] = "caller-mutated"

	got, _ := store.Snapshot()
	got.Members[0].ID = "returned-mutated"
	got.Members[0].Attributes["role"] = "returned-mutated"
	got.Members[0].Endpoints[0].Attributes["zone"] = "returned-mutated"

	again, _ := store.Snapshot()
	if again.Members[0].ID != "member" || again.Members[0].Attributes["role"] != "primary" || again.Members[0].Endpoints[0].Attributes["zone"] != "a" {
		t.Fatalf("store state was exposed through a caller mutation: %#v", again)
	}
}

func TestTopologyStoreConcurrentSnapshotsAreWholeValues(t *testing.T) {
	store := gyro.NewTopologyStore()
	if err := store.Publish(context.Background(), topologyForGeneration(0)); err != nil {
		t.Fatalf("initial Publish failed: %v", err)
	}

	const generations = 100
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for generation := uint64(1); generation < generations; generation++ {
			if err := store.Publish(context.Background(), topologyForGeneration(generation)); err != nil {
				t.Errorf("Publish generation %d failed: %v", generation, err)
				return
			}
		}
	}()

	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 500; j++ {
				snapshot, ok := store.Snapshot()
				if !ok || len(snapshot.Members) != 2 {
					t.Errorf("read incomplete snapshot: ok=%v snapshot=%#v", ok, snapshot)
					return
				}
				if snapshot.Members[0].Attributes["generation"] != snapshot.Members[1].Attributes["generation"] {
					t.Errorf("read mixed generations: %#v", snapshot)
					return
				}
			}
		}()
	}
	writers.Wait()
	readers.Wait()
}

func topologyForGeneration(generation uint64) gyro.TopologySnapshot {
	value := fmt.Sprintf("%d", generation)
	return gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source", Generation: generation, Token: value},
		Members: []gyro.Member{
			{ID: "a", Attributes: map[string]string{"generation": value}},
			{ID: "b", Attributes: map[string]string{"generation": value}},
		},
	}
}

package gyro

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStaticServiceDiscoveryMutationsNotifyWatchers(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := discovery.Watch(ctx, "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	assertNodeSnapshot(t, updates, nil)

	node1 := NodeInfo{ID: "node-1", Address: "127.0.0.1:8001"}
	node1Updated := NodeInfo{ID: "node-1", Address: "127.0.0.1:9001"}
	node2 := NodeInfo{ID: "node-2", Address: "127.0.0.1:8002"}

	if err := discovery.Register(context.Background(), "orders", node1); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertNodeSnapshot(t, updates, []NodeInfo{node1})

	if err := discovery.Register(context.Background(), "orders", node1Updated); err != nil {
		t.Fatalf("Register update failed: %v", err)
	}
	assertNodeSnapshot(t, updates, []NodeInfo{node1Updated})

	discovery.SetNodes("orders", []NodeInfo{node1Updated, node2})
	assertNodeSnapshot(t, updates, []NodeInfo{node1Updated, node2})

	if err := discovery.Unregister(context.Background(), "orders", node1.ID); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	assertNodeSnapshot(t, updates, []NodeInfo{node2})

	discovery.UpdateNodes("orders", []string{"127.0.0.1:7001"})
	assertNodeSnapshot(t, updates, []NodeInfo{{ID: "127.0.0.1:7001", Address: "127.0.0.1:7001"}})
}

func TestStaticServiceDiscoverySlowWatcherEventuallyReceivesLatestSnapshot(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := discovery.Watch(ctx, "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	assertNodeSnapshot(t, updates, nil)

	for i := 0; i < 20; i++ {
		discovery.SetNodes("orders", []NodeInfo{{
			ID:      "node-1",
			Address: fmt.Sprintf("127.0.0.1:%d", 8000+i),
		}})
	}

	deadline := time.After(time.Second)
	for {
		select {
		case snapshot := <-updates:
			if len(snapshot) == 1 && snapshot[0].Address == "127.0.0.1:8019" {
				return
			}
		case <-deadline:
			t.Fatal("slow watcher never received the latest service snapshot")
		}
	}
}

func TestStaticServiceDiscoverySnapshotsAreIsolated(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	input := []NodeInfo{{
		ID:       "node-1",
		Address:  "127.0.0.1:8001",
		Metadata: map[string]string{"zone": "a"},
	}}
	discovery.SetNodes("orders", input)
	input[0].Address = "caller-mutated"
	input[0].Metadata["zone"] = "caller-mutated"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := discovery.Watch(ctx, "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	snapshot := <-updates
	if snapshot[0].Address != "127.0.0.1:8001" || snapshot[0].Metadata["zone"] != "a" {
		t.Fatalf("stored snapshot was changed through caller input: %#v", snapshot)
	}
	snapshot[0].Address = "watcher-mutated"
	snapshot[0].Metadata["zone"] = "watcher-mutated"

	discovered, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}
	if discovered[0].Address != "127.0.0.1:8001" || discovered[0].Metadata["zone"] != "a" {
		t.Fatalf("internal state was changed through watcher snapshot: %#v", discovered)
	}
}

func TestStaticServiceDiscoveryConcurrentDiscoverDoesNotOverwriteMutation(t *testing.T) {
	for i := 0; i < 1000; i++ {
		discovery := NewStaticServiceDiscovery([]string{"127.0.0.1:7000"})
		explicit := []NodeInfo{{ID: "explicit", Address: "127.0.0.1:8000"}}
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			_, _ = discovery.Discover(context.Background(), "orders")
		}()
		go func() {
			defer workers.Done()
			<-start
			discovery.SetNodes("orders", explicit)
		}()
		close(start)
		workers.Wait()

		got, err := discovery.Discover(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Discover failed: %v", err)
		}
		if len(got) != 1 || got[0].ID != explicit[0].ID || got[0].Address != explicit[0].Address {
			t.Fatalf("iteration %d: Discover overwrote explicit topology with stale default: %#v", i, got)
		}
	}
}

func TestStaticServiceDiscoveryAddressIDsSurviveReordering(t *testing.T) {
	addresses := []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
	discovery := NewStaticServiceDiscovery(addresses)
	before, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("initial Discover failed: %v", err)
	}
	beforeIDs := make(map[string]string, len(before))
	for _, node := range before {
		beforeIDs[node.Address] = node.ID
	}

	discovery.UpdateNodes("orders", []string{addresses[2], addresses[0], addresses[1]})
	after, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Discover after reorder failed: %v", err)
	}
	for _, node := range after {
		if node.ID != beforeIDs[node.Address] {
			t.Fatalf("address %q changed ID from %q to %q after reorder", node.Address, beforeIDs[node.Address], node.ID)
		}
	}
}

func assertNodeSnapshot(t *testing.T, updates <-chan []NodeInfo, want []NodeInfo) {
	t.Helper()
	select {
	case got := <-updates:
		if len(got) != len(want) {
			t.Fatalf("snapshot length = %d, want %d: %#v", len(got), len(want), got)
		}
		for i := range want {
			if got[i].ID != want[i].ID || got[i].Address != want[i].Address {
				t.Fatalf("snapshot[%d] = %#v, want %#v", i, got[i], want[i])
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for service discovery update")
	}
}

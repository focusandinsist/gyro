package gyro

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestStaticServiceDiscoveryMutationsPublishCompleteSnapshots(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := discovery.Watch(ctx, "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	assertTopologySnapshot(t, stream, "orders", nil)

	node1 := NodeInfo{ID: "node-1", Address: "127.0.0.1:8001"}
	node1Updated := NodeInfo{ID: "node-1", Address: "127.0.0.1:9001"}
	node2 := NodeInfo{ID: "node-2", Address: "127.0.0.1:8002"}

	if err := discovery.Register(context.Background(), "orders", node1); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertTopologySnapshot(t, stream, "orders", []NodeInfo{node1})

	if err := discovery.Register(context.Background(), "orders", node1Updated); err != nil {
		t.Fatalf("Register update failed: %v", err)
	}
	assertTopologySnapshot(t, stream, "orders", []NodeInfo{node1Updated})

	discovery.SetNodes("orders", []NodeInfo{node1Updated, node2})
	assertTopologySnapshot(t, stream, "orders", []NodeInfo{node1Updated, node2})

	if err := discovery.Unregister(context.Background(), "orders", node1.ID); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	assertTopologySnapshot(t, stream, "orders", []NodeInfo{node2})
}

func TestStaticServiceDiscoverySlowWatcherReceivesLatestSnapshot(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := discovery.Watch(ctx, "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	assertTopologySnapshot(t, stream, "orders", nil)

	for i := 0; i < 20; i++ {
		discovery.SetNodes("orders", []NodeInfo{{
			ID:      "node-1",
			Address: fmt.Sprintf("127.0.0.1:%d", 8000+i),
		}})
	}

	deadline := time.After(time.Second)
	for {
		snapshot, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if len(snapshot.Members) == 1 && snapshot.Members[0].Endpoints[0].Address == "127.0.0.1:8019" {
			return
		}
		select {
		case <-deadline:
			t.Fatal("slow watcher never received the latest service snapshot")
		default:
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

	snapshot, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Discover failed: %v", err)
	}
	if snapshot.Members[0].Endpoints[0].Address != "127.0.0.1:8001" || snapshot.Members[0].Attributes["zone"] != "a" {
		t.Fatalf("stored snapshot was changed through caller input: %#v", snapshot)
	}
	snapshot.Members[0].Endpoints[0].Address = "watcher-mutated"
	snapshot.Members[0].Attributes["zone"] = "watcher-mutated"

	again, err := discovery.Discover(context.Background(), "orders")
	if err != nil {
		t.Fatalf("second Discover failed: %v", err)
	}
	if again.Members[0].Endpoints[0].Address != "127.0.0.1:8001" || again.Members[0].Attributes["zone"] != "a" {
		t.Fatalf("internal state was changed through returned snapshot: %#v", again)
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
			t.Fatalf("iteration %d: Discover failed: %v", i, err)
		}
		if len(got.Members) != 1 || got.Members[0].ID != explicit[0].ID || got.Members[0].Endpoints[0].Address != explicit[0].Address {
			t.Fatalf("iteration %d: Discover overwrote explicit topology: %#v", i, got)
		}
	}
}

func TestStaticServiceDiscoveryAddressIDsSurviveReordering(t *testing.T) {
	addresses := []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
	discovery := NewStaticServiceDiscovery(addresses)
	before, err := discovery.Discover(context.Background(), "default")
	if err != nil {
		t.Fatalf("initial Discover failed: %v", err)
	}
	beforeIDs := make(map[string]string, len(before.Members))
	for _, member := range before.Members {
		beforeIDs[member.Endpoints[0].Address] = member.ID
	}

	discovery.UpdateNodes("default", []string{addresses[2], addresses[0], addresses[1]})
	after, err := discovery.Discover(context.Background(), "default")
	if err != nil {
		t.Fatalf("Discover after reorder failed: %v", err)
	}
	for _, member := range after.Members {
		if member.ID != beforeIDs[member.Endpoints[0].Address] {
			t.Fatalf("address %q changed ID from %q to %q after reorder", member.Endpoints[0].Address, beforeIDs[member.Endpoints[0].Address], member.ID)
		}
	}
}

func TestStaticServiceDiscoveryWatchCloseIsIdempotent(t *testing.T) {
	discovery := NewStaticServiceDiscovery(nil)
	stream, err := discovery.Watch(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("initial Next failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("Next after Close error = %v, want io.EOF", err)
	}
}

func assertTopologySnapshot(t *testing.T, stream TopologyStream, serviceName string, want []NodeInfo) {
	t.Helper()
	snapshot, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("%s Next failed: %v", serviceName, err)
	}
	if len(snapshot.Members) != len(want) {
		t.Fatalf("%s snapshot length = %d, want %d: %#v", serviceName, len(snapshot.Members), len(want), snapshot)
	}
	for i, node := range want {
		member := snapshot.Members[i]
		if member.ID != node.ID || len(member.Endpoints) != 1 || member.Endpoints[0].Address != node.Address {
			t.Fatalf("%s snapshot[%d] = %#v, want %#v", serviceName, i, member, node)
		}
	}
}

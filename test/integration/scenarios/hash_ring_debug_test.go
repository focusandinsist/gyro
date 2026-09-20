package scenarios

import (
	"context"
	"fmt"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

func TestHashRingDebug(t *testing.T) {
	t.Run("InternalRingState", func(t *testing.T) {
		testInternalRingState(t)
	})

	t.Run("SingleKeyTracking", func(t *testing.T) {
		testSingleKeyTracking(t)
	})

	t.Run("AlternativeLibraryComparison", func(t *testing.T) {
		testAlternativeLibraryComparison(t)
	})
}

func testInternalRingState(t *testing.T) {
	t.Log("=== Testing Internal Ring State ===")

	config := consistent.Config{
		Hasher:            consistent.NewDefaultHasher(),
		PartitionCount:    23, // Use smaller number for easier debugging
		ReplicationFactor: 3,
		Load:              1.0,
	}

	ring, err := consistent.New(config)
	if err != nil {
		t.Fatalf("Failed to create ring: %v", err)
	}

	ctx := context.Background()

	// Add nodes one by one and check state
	nodes := []string{"A", "B", "C", "D"}
	testKey := "test-key-123"

	for _, node := range nodes {
		t.Logf("--- Adding node %s ---", node)

		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}

		// Check members
		members := ring.GetMembers(ctx)
		t.Logf("Members: %v", members)

		// Check where our test key goes
		nodeID, err := ring.LocateKey(ctx, []byte(testKey))
		if err != nil {
			t.Fatalf("Failed to locate key: %v", err)
		}
		t.Logf("Key '%s' -> %s", testKey, nodeID)

		// Test a few more keys
		for j := 0; j < 5; j++ {
			key := fmt.Sprintf("key-%d", j)
			nodeID, err := ring.LocateKey(ctx, []byte(key))
			if err != nil {
				t.Fatalf("Failed to locate key %s: %v", key, err)
			}
			t.Logf("  '%s' -> %s", key, nodeID)
		}

		t.Logf("")
	}
}

func testSingleKeyTracking(t *testing.T) {
	t.Log("=== Testing Single Key Tracking ===")

	config := consistent.Config{
		Hasher:            consistent.NewDefaultHasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring, err := consistent.New(config)
	if err != nil {
		t.Fatalf("Failed to create ring: %v", err)
	}

	ctx := context.Background()

	// Track specific keys through node additions
	trackingKeys := []string{
		"user-001",
		"order-123",
		"session-abc",
		"product-xyz",
		"cache-key-1",
	}

	// Add initial nodes
	initialNodes := []string{"node-1", "node-2", "node-3"}
	for _, node := range initialNodes {
		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}
	}

	// Record initial locations
	t.Log("=== Initial locations (3 nodes) ===")
	initialLocations := make(map[string]string)
	for _, key := range trackingKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		initialLocations[key] = nodeID
		t.Logf("'%s' -> %s", key, nodeID)
	}

	// Add new node
	newNode := "node-4"
	if err := ring.Add(ctx, newNode); err != nil {
		t.Fatalf("Failed to add %s: %v", newNode, err)
	}

	// Check new locations
	t.Log("=== After adding node-4 ===")
	changes := 0
	for _, key := range trackingKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}

		oldNode := initialLocations[key]
		if oldNode != nodeID {
			t.Logf("'%s' MOVED: %s -> %s", key, oldNode, nodeID)
			changes++
		} else {
			t.Logf("'%s' STAYED: %s", key, nodeID)
		}
	}

	t.Logf("Total changes: %d/%d keys", changes, len(trackingKeys))

	if changes == 0 {
		t.Error("❌ No keys moved - this indicates the library bug")
	}
}

func testAlternativeLibraryComparison(t *testing.T) {
	t.Log("=== Alternative Library Comparison ===")

	// This test shows what SHOULD happen with a correct consistent hash implementation
	// We'll implement a simple consistent hash to compare

	t.Log("Expected behavior with correct consistent hashing:")
	t.Log("- Adding 1 node to 3 nodes should move ~25% of keys")
	t.Log("- Keys should be distributed roughly evenly")
	t.Log("- Only keys that need to move should move (minimal disruption)")

	// Simple demonstration of expected behavior
	testKeys := []string{"key-1", "key-2", "key-3", "key-4", "key-5", "key-6", "key-7", "key-8"}

	t.Log("\nSimulated correct behavior:")
	t.Log("3 nodes: A=33%, B=33%, C=33%")
	t.Log("4 nodes: A=25%, B=25%, C=25%, D=25%")
	t.Log("Expected moves: ~25% of keys should move to new node D")

	// Test the actual library behavior
	config := consistent.Config{
		Hasher:            consistent.NewDefaultHasher(),
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	ring, err := consistent.New(config)
	if err != nil {
		t.Fatalf("Failed to create ring: %v", err)
	}

	ctx := context.Background()

	// Add 3 nodes
	for _, node := range []string{"A", "B", "C"} {
		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}
	}

	// Check distribution with 3 nodes
	dist3 := make(map[string]int)
	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		dist3[nodeID]++
	}

	t.Log("\nActual library behavior with 3 nodes:")
	for node, count := range dist3 {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Add 4th node
	if err := ring.Add(ctx, "D"); err != nil {
		t.Fatalf("Failed to add D: %v", err)
	}

	// Check distribution with 4 nodes
	dist4 := make(map[string]int)
	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		dist4[nodeID]++
	}

	t.Log("\nActual library behavior with 4 nodes:")
	for node, count := range dist4 {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Check if new node got any keys
	newNodeKeys := dist4["D"]
	if newNodeKeys == 0 {
		t.Error("❌ CONFIRMED BUG: New node 'D' received 0 keys")
		t.Error("This violates the fundamental principle of consistent hashing")
	} else {
		t.Logf("✅ New node 'D' received %d keys", newNodeKeys)
	}
}

// Additional test to check if the issue is with specific node names or patterns
func TestNodeNamingPatterns(t *testing.T) {
	patterns := []struct {
		name  string
		nodes []string
	}{
		{"Letters", []string{"A", "B", "C", "D"}},
		{"Numbers", []string{"1", "2", "3", "4"}},
		{"IPs", []string{"192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4"}},
		{"Hostnames", []string{"host1", "host2", "host3", "host4"}},
		{"UUIDs", []string{"550e8400-e29b-41d4", "6ba7b810-9dad-11d1", "6ba7b811-9dad-11d1", "6ba7b812-9dad-11d1"}},
	}

	for _, pattern := range patterns {
		t.Run(pattern.name, func(t *testing.T) {
			config := consistent.Config{
				Hasher:            consistent.NewDefaultHasher(),
				PartitionCount:    271,
				ReplicationFactor: 20,
				Load:              1.25,
			}

			ring, err := consistent.New(config)
			if err != nil {
				t.Fatalf("Failed to create ring: %v", err)
			}

			ctx := context.Background()

			// Add first 3 nodes
			for i := 0; i < 3; i++ {
				if err := ring.Add(ctx, pattern.nodes[i]); err != nil {
					t.Fatalf("Failed to add %s: %v", pattern.nodes[i], err)
				}
			}

			// Test with 10 keys
			testKeys := make([]string, 10)
			for i := 0; i < 10; i++ {
				testKeys[i] = fmt.Sprintf("test-key-%d", i)
			}

			// Record initial distribution
			initialDist := make(map[string]string)
			for _, key := range testKeys {
				nodeID, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					t.Fatalf("Failed to locate key %s: %v", key, err)
				}
				initialDist[key] = nodeID
			}

			// Add 4th node
			if err := ring.Add(ctx, pattern.nodes[3]); err != nil {
				t.Fatalf("Failed to add %s: %v", pattern.nodes[3], err)
			}

			// Record new distribution
			newDist := make(map[string]string)
			for _, key := range testKeys {
				nodeID, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					t.Fatalf("Failed to locate key %s: %v", key, err)
				}
				newDist[key] = nodeID
			}

			// Count moves
			moves := 0
			movedToNew := 0
			for _, key := range testKeys {
				if initialDist[key] != newDist[key] {
					moves++
					if newDist[key] == pattern.nodes[3] {
						movedToNew++
					}
				}
			}

			t.Logf("Pattern %s: %d moves, %d to new node '%s'",
				pattern.name, moves, movedToNew, pattern.nodes[3])

			if moves == 0 {
				t.Logf("❌ No moves with pattern %s", pattern.name)
			}
		})
	}
}

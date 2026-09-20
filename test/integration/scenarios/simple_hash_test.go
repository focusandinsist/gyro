package scenarios

import (
	"context"
	"fmt"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

func TestSimpleHashLibrary(t *testing.T) {
	t.Log("=== Testing Updated Consistent Hash Library ===")

	// Create ring with simple config
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

	// Add 3 initial nodes
	nodes := []string{"node-1", "node-2", "node-3"}
	for _, node := range nodes {
		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}
	}

	// Test with 20 keys
	testKeys := make([]string, 20)
	for i := 0; i < 20; i++ {
		testKeys[i] = fmt.Sprintf("test-key-%02d", i)
	}

	// Record initial distribution
	t.Log("=== Initial Distribution (3 nodes) ===")
	initialDist := make(map[string]string)
	distribution := make(map[string]int)

	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		initialDist[key] = nodeID
		distribution[nodeID]++
		t.Logf("'%s' -> %s", key, nodeID)
	}

	t.Log("=== Distribution Summary (3 nodes) ===")
	for node, count := range distribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("%s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Check load distribution before adding new node
	t.Log("=== Load Distribution BEFORE adding new node ===")
	loadDistBefore := ring.LoadDistribution(ctx)
	for node, load := range loadDistBefore {
		t.Logf("  %s: %.1f partitions", node, load)
	}

	// Add 4th node
	newNode := "node-4"
	if err := ring.Add(ctx, newNode); err != nil {
		t.Fatalf("Failed to add %s: %v", newNode, err)
	}

	// Check load distribution after adding new node
	t.Log("=== Load Distribution AFTER adding new node ===")
	loadDistAfter := ring.LoadDistribution(ctx)
	for node, load := range loadDistAfter {
		t.Logf("  %s: %.1f partitions", node, load)
	}

	// Check members
	members := ring.GetMembers(ctx)
	t.Logf("Ring members after adding %s: %v", newNode, members)

	// Record new distribution
	t.Log("=== New Distribution (4 nodes) ===")
	newDist := make(map[string]string)
	newDistribution := make(map[string]int)

	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		newDist[key] = nodeID
		newDistribution[nodeID]++

		oldNode := initialDist[key]
		if oldNode != nodeID {
			t.Logf("'%s' MOVED: %s -> %s", key, oldNode, nodeID)
		} else {
			t.Logf("'%s' STAYED: %s", key, nodeID)
		}
	}

	t.Log("=== Distribution Summary (4 nodes) ===")
	for node, count := range newDistribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("%s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Analyze changes
	moves := 0
	movedToNew := 0
	for _, key := range testKeys {
		if initialDist[key] != newDist[key] {
			moves++
			if newDist[key] == newNode {
				movedToNew++
			}
		}
	}

	movePercentage := float64(moves) / float64(len(testKeys)) * 100
	newNodePercentage := float64(movedToNew) / float64(len(testKeys)) * 100

	t.Log("=== Change Analysis ===")
	t.Logf("Total keys: %d", len(testKeys))
	t.Logf("Moved keys: %d (%.1f%%)", moves, movePercentage)
	t.Logf("Keys moved to new node: %d (%.1f%%)", movedToNew, newNodePercentage)

	// Check if new node got any keys
	newNodeKeys := newDistribution[newNode]
	if newNodeKeys == 0 {
		t.Error("❌ STILL BROKEN: New node received 0 keys")
		t.Error("The consistent hashing library still has the same bug")
	} else {
		t.Logf("✅ SUCCESS: New node received %d keys", newNodeKeys)
		t.Logf("✅ Library appears to be working correctly now!")
	}

	// Expected behavior check
	if moves == 0 {
		t.Error("❌ No keys moved when adding a new node - this violates consistent hashing principles")
	} else if movePercentage < 10.0 {
		t.Logf("⚠️  Low movement: %.1f%% (expected ~25%% for 3->4 nodes)", movePercentage)
	} else if movePercentage > 40.0 {
		t.Logf("⚠️  High movement: %.1f%% (expected ~25%% for 3->4 nodes)", movePercentage)
	} else {
		t.Logf("✅ Good movement: %.1f%% (within expected range)", movePercentage)
	}
}

package scenarios

import (
	"context"
	"fmt"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

func TestDebugLocalHashLibrary(t *testing.T) {
	t.Log("=== Debugging Local Consistent Hash Library ===")

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

	// Check load distribution after 3 nodes
	t.Log("=== Load Distribution (3 nodes) ===")
	loadDist := ring.LoadDistribution(ctx)
	for node, load := range loadDist {
		t.Logf("Node %s: %.1f partitions", node, load)
	}

	avgLoad, _ := ring.AverageLoad(ctx)
	t.Logf("Average load: %.1f", avgLoad)

	// Test with 20 keys
	testKeys := make([]string, 20)
	for i := 0; i < 20; i++ {
		testKeys[i] = fmt.Sprintf("test-key-%02d", i)
	}

	// Record initial distribution
	t.Log("=== Initial Key Distribution (3 nodes) ===")
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

	t.Log("=== Key Distribution Summary (3 nodes) ===")
	for node, count := range distribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("%s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Add 4th node
	newNode := "node-4"
	t.Logf("=== Adding new node: %s ===", newNode)

	// Check load before adding
	t.Log("Load distribution BEFORE adding new node:")
	loadDistBefore := ring.LoadDistribution(ctx)
	for node, load := range loadDistBefore {
		t.Logf("  %s: %.1f partitions", node, load)
	}

	if err := ring.Add(ctx, newNode); err != nil {
		t.Fatalf("Failed to add %s: %v", newNode, err)
	}

	// Check load after adding
	t.Log("Load distribution AFTER adding new node:")
	loadDistAfter := ring.LoadDistribution(ctx)
	for node, load := range loadDistAfter {
		t.Logf("  %s: %.1f partitions", node, load)
	}

	newAvgLoad, _ := ring.AverageLoad(ctx)
	t.Logf("New average load: %.1f", newAvgLoad)

	// Check members
	members := ring.GetMembers(ctx)
	t.Logf("Ring members after adding %s: %v", newNode, members)

	// Record new distribution
	t.Log("=== New Key Distribution (4 nodes) ===")
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

	t.Log("=== Key Distribution Summary (4 nodes) ===")
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
	newNodeLoad := loadDistAfter[newNode]

	t.Logf("New node '%s' statistics:", newNode)
	t.Logf("  Keys: %d", newNodeKeys)
	t.Logf("  Load (partitions): %.1f", newNodeLoad)

	if newNodeKeys == 0 && newNodeLoad == 0 {
		t.Error("❌ CRITICAL: New node received 0 keys AND 0 partitions")
		t.Error("This indicates the rebalancing algorithm is not working at all")
	} else if newNodeKeys == 0 && newNodeLoad > 0 {
		t.Error("❌ PARTIAL ISSUE: New node has partitions but no keys")
		t.Error("This suggests partition assignment works but key routing doesn't")
	} else if newNodeKeys > 0 && newNodeLoad == 0 {
		t.Error("❌ INCONSISTENT: New node has keys but no partitions")
		t.Error("This is impossible and indicates a serious bug")
	} else {
		t.Logf("✅ SUCCESS: New node received %d keys and %.1f partitions", newNodeKeys, newNodeLoad)
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

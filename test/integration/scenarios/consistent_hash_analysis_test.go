package scenarios

import (
	"context"
	"fmt"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

func TestConsistentHashAnalysis(t *testing.T) {
	t.Run("BasicLibraryBehavior", func(t *testing.T) {
		testBasicLibraryBehavior(t)
	})

	t.Run("DifferentConfigurations", func(t *testing.T) {
		testDifferentConfigurations(t)
	})

	t.Run("NodeOrderEffect", func(t *testing.T) {
		testNodeOrderEffect(t)
	})

	t.Run("KeyDistributionAnalysis", func(t *testing.T) {
		testKeyDistributionAnalysis(t)
	})
}

func testBasicLibraryBehavior(t *testing.T) {
	t.Log("=== Testing Basic Library Behavior ===")

	// Create ring with default config
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

	// Add initial nodes
	nodes := []string{"node-A", "node-B", "node-C"}
	for _, node := range nodes {
		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}
	}

	// Test with a variety of keys
	testKeys := generateTestKeys(100)

	// Record initial distribution
	initialDistribution := make(map[string]string)
	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		initialDistribution[key] = nodeID
	}

	// Print initial distribution
	printDistribution(t, "Initial (3 nodes)", initialDistribution, testKeys)

	// Add a new node
	newNode := "node-D"
	if err := ring.Add(ctx, newNode); err != nil {
		t.Fatalf("Failed to add %s: %v", newNode, err)
	}

	// Check members
	members := ring.GetMembers(ctx)
	t.Logf("Ring members after adding %s: %v", newNode, members)

	// Record new distribution
	newDistribution := make(map[string]string)
	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		newDistribution[key] = nodeID
	}

	// Print new distribution
	printDistribution(t, "After adding node-D", newDistribution, testKeys)

	// Analyze changes
	analyzeChanges(t, initialDistribution, newDistribution, testKeys)
}

func testDifferentConfigurations(t *testing.T) {
	t.Log("=== Testing Different Configurations ===")

	configs := []struct {
		name              string
		partitionCount    int
		replicationFactor int
		load              float64
	}{
		{"Small", 23, 3, 1.0},
		{"Medium", 127, 10, 1.1},
		{"Large", 271, 20, 1.25},
		{"XLarge", 541, 40, 1.5},
	}

	testKeys := generateTestKeys(50)

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			config := consistent.Config{
				Hasher:            consistent.NewDefaultHasher(),
				PartitionCount:    cfg.partitionCount,
				ReplicationFactor: cfg.replicationFactor,
				Load:              cfg.load,
			}

			ring, err := consistent.New(config)
			if err != nil {
				t.Fatalf("Failed to create ring: %v", err)
			}

			ctx := context.Background()

			// Add initial nodes
			nodes := []string{"A", "B", "C"}
			for _, node := range nodes {
				if err := ring.Add(ctx, node); err != nil {
					t.Fatalf("Failed to add %s: %v", node, err)
				}
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

			// Add new node
			if err := ring.Add(ctx, "D"); err != nil {
				t.Fatalf("Failed to add D: %v", err)
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
			for _, key := range testKeys {
				if initialDist[key] != newDist[key] {
					moves++
				}
			}

			movePercentage := float64(moves) / float64(len(testKeys)) * 100
			t.Logf("Config %s: %d/%d keys moved (%.1f%%)", cfg.name, moves, len(testKeys), movePercentage)
		})
	}
}

func testNodeOrderEffect(t *testing.T) {
	t.Log("=== Testing Node Order Effect ===")

	testKeys := generateTestKeys(50)

	// Test different node addition orders
	orders := [][]string{
		{"A", "B", "C", "D"},
		{"D", "C", "B", "A"},
		{"B", "D", "A", "C"},
	}

	for i, order := range orders {
		t.Run(fmt.Sprintf("Order%d", i+1), func(t *testing.T) {
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
			for j := 0; j < 3; j++ {
				if err := ring.Add(ctx, order[j]); err != nil {
					t.Fatalf("Failed to add %s: %v", order[j], err)
				}
			}

			// Record distribution with 3 nodes
			dist3 := make(map[string]string)
			for _, key := range testKeys {
				nodeID, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					t.Fatalf("Failed to locate key %s: %v", key, err)
				}
				dist3[key] = nodeID
			}

			// Add 4th node
			if err := ring.Add(ctx, order[3]); err != nil {
				t.Fatalf("Failed to add %s: %v", order[3], err)
			}

			// Record distribution with 4 nodes
			dist4 := make(map[string]string)
			for _, key := range testKeys {
				nodeID, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					t.Fatalf("Failed to locate key %s: %v", key, err)
				}
				dist4[key] = nodeID
			}

			// Count moves
			moves := 0
			for _, key := range testKeys {
				if dist3[key] != dist4[key] {
					moves++
				}
			}

			movePercentage := float64(moves) / float64(len(testKeys)) * 100
			t.Logf("Order %v: %d/%d keys moved (%.1f%%)", order, moves, len(testKeys), movePercentage)
		})
	}
}

func testKeyDistributionAnalysis(t *testing.T) {
	t.Log("=== Testing Key Distribution Analysis ===")

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

	// Add nodes one by one and observe distribution
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	testKeys := generateTestKeys(1000)

	for i, node := range nodes {
		if err := ring.Add(ctx, node); err != nil {
			t.Fatalf("Failed to add %s: %v", node, err)
		}

		// Record distribution
		distribution := make(map[string]int)
		for _, key := range testKeys {
			nodeID, err := ring.LocateKey(ctx, []byte(key))
			if err != nil {
				t.Fatalf("Failed to locate key %s: %v", key, err)
			}
			distribution[nodeID]++
		}

		t.Logf("=== After adding %s (%d nodes total) ===", node, i+1)
		for nodeID, count := range distribution {
			percentage := float64(count) / float64(len(testKeys)) * 100
			t.Logf("  %s: %d keys (%.1f%%)", nodeID, count, percentage)
		}
	}
}

// Helper functions

func generateTestKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	return keys
}

func printDistribution(t *testing.T, title string, distribution map[string]string, keys []string) {
	counts := make(map[string]int)
	for _, key := range keys {
		node := distribution[key]
		counts[node]++
	}

	t.Logf("=== %s ===", title)
	for node, count := range counts {
		percentage := float64(count) / float64(len(keys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}
}

func analyzeChanges(t *testing.T, initial, new map[string]string, keys []string) {
	moves := 0
	movedToNew := 0

	for _, key := range keys {
		oldNode := initial[key]
		newNode := new[key]

		if oldNode != newNode {
			moves++
			if newNode == "node-D" {
				movedToNew++
			}
		}
	}

	movePercentage := float64(moves) / float64(len(keys)) * 100
	newNodePercentage := float64(movedToNew) / float64(len(keys)) * 100

	t.Logf("=== Change Analysis ===")
	t.Logf("Total keys: %d", len(keys))
	t.Logf("Moved keys: %d (%.1f%%)", moves, movePercentage)
	t.Logf("Keys moved to new node: %d (%.1f%%)", movedToNew, newNodePercentage)

	if moves == 0 {
		t.Errorf("❌ PROBLEM: No keys moved when adding a new node!")
	} else {
		t.Logf("✅ Keys moved as expected")
	}
}

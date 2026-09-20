package scenarios

import (
	"context"
	"fmt"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

func TestConsistentHashLibrary(t *testing.T) {
	// Test the consistent hash library directly with simpler config
	config := consistent.Config{
		Hasher:            consistent.NewDefaultHasher(),
		PartitionCount:    23,  // Smaller partition count
		ReplicationFactor: 3,   // Smaller replication factor
		Load:              1.0, // Default load
	}

	ring, err := consistent.New(config)
	if err != nil {
		t.Fatalf("Failed to create consistent hash ring: %v", err)
	}

	// Add initial nodes
	ctx := context.Background()
	err = ring.Add(ctx, "node-1")
	if err != nil {
		t.Fatalf("Failed to add node-1: %v", err)
	}
	err = ring.Add(ctx, "node-2")
	if err != nil {
		t.Fatalf("Failed to add node-2: %v", err)
	}
	err = ring.Add(ctx, "node-3")
	if err != nil {
		t.Fatalf("Failed to add node-3: %v", err)
	}

	// Test many keys with 3 nodes
	testKeys := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		testKeys = append(testKeys, fmt.Sprintf("key-%04d", i))
	}

	t.Log("=== Initial routing with 3 nodes ===")
	initialRouting := make(map[string]string)
	for i, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		initialRouting[key] = nodeID
		if i < 10 { // Only log first 10 keys
			t.Logf("Key '%s' -> Node '%s'", key, nodeID)
		}
	}

	// Add a new node
	t.Log("=== Adding node-4 ===")
	err = ring.Add(ctx, "node-4")
	if err != nil {
		t.Fatalf("Failed to add node-4: %v", err)
	}

	// Check if the ring has all 4 nodes
	members := ring.GetMembers(ctx)
	t.Logf("Ring members after adding node-4: %v", members)
	if len(members) != 4 {
		t.Errorf("Expected 4 members in ring, got %d", len(members))
	}

	// Test the same keys with 4 nodes
	t.Log("=== New routing with 4 nodes ===")
	movedCount := 0
	movedKeys := make([]string, 0)
	for _, key := range testKeys {
		nodeID, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}

		oldNode := initialRouting[key]
		if oldNode != nodeID {
			if len(movedKeys) < 10 { // Only log first 10 moved keys
				t.Logf("Key '%s' moved: %s -> %s", key, oldNode, nodeID)
			}
			movedKeys = append(movedKeys, key)
			movedCount++
		}
	}

	t.Logf("=== Summary ===")
	t.Logf("Total keys: %d", len(testKeys))
	t.Logf("Moved keys: %d", movedCount)
	t.Logf("Stable keys: %d", len(testKeys)-movedCount)

	if movedCount == 0 {
		t.Errorf("Expected some keys to move when adding a new node, but none moved")
	}
}

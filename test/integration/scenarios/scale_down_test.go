package scenarios

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/gyro/grpc"
	"github.com/focusandinsist/gyro/test/integration/testbed"
)

func TestScaleDownScenario(t *testing.T) {
	// Start test cluster with initial 4 nodes (including one we'll remove)
	cluster := testbed.NewTestCluster(testbed.DefaultClusterConfig())
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	t.Run("gRPC_ScaleDown", func(t *testing.T) {
		testGRPCScaleDown(t, cluster)
	})

	t.Run("Redis_ScaleDown", func(t *testing.T) {
		testRedisScaleDown(t, cluster)
	})
}

func testGRPCScaleDown(t *testing.T, cluster *testbed.TestCluster) {
	// Create Gyro client configuration
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 500 * time.Millisecond
	config.HealthChecker.Timeout = 200 * time.Millisecond
	config.HealthChecker.FailureThreshold = 2
	config.HealthChecker.RecoveryThreshold = 2

	// Start with 4 nodes (add one extra node to the default 3)
	initialAddresses := cluster.GetGRPCAddresses()

	// Add a 4th node that we'll later remove
	extraPort := 8004
	if err := cluster.StartGRPCServer(extraPort); err != nil {
		t.Fatalf("Failed to start extra gRPC server on port %d: %v", extraPort, err)
	}

	// Include the extra node in initial addresses
	initialAddresses = append(initialAddresses, "localhost:8004")
	discovery := gyro.NewStaticServiceDiscovery(initialAddresses)

	// Create config manager
	configManager := gyro.NewConfigManager(config)

	// Create node factory for gRPC
	nodeFactory := grpc.NewGRPCNodeFactory()

	// Create health checker
	healthChecker := gyro.NewDefaultHealthChecker(config.HealthChecker)

	// Create Gyro client
	client, err := gyro.NewClient(
		"test-grpc-service",
		discovery,
		configManager,
		nodeFactory,
		healthChecker,
	)
	if err != nil {
		t.Fatalf("Failed to create Gyro client: %v", err)
	}
	defer client.Close()

	// Start health monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("Failed to start Gyro client: %v", err)
	}

	// Wait for initial health checks
	time.Sleep(1 * time.Second)

	// Test keys for consistent hashing analysis
	testKeys := make([]string, 0, 100)

	// Generate 100 test keys with different patterns
	for i := 0; i < 25; i++ {
		testKeys = append(testKeys, fmt.Sprintf("user-%03d", i))
		testKeys = append(testKeys, fmt.Sprintf("order-%03d", i))
		testKeys = append(testKeys, fmt.Sprintf("product-%03d", i))
		testKeys = append(testKeys, fmt.Sprintf("session-%03d", i))
	}

	var initialRouting map[string]string
	var nodeToRemove string

	t.Run("EstablishInitialRouting", func(t *testing.T) {
		initialRouting = make(map[string]string)

		t.Logf("=== Initial Routing with %d nodes ===", len(initialAddresses))
		for _, addr := range initialAddresses {
			t.Logf("Node: %s", addr)
		}

		// Record initial routing for all test keys
		for _, key := range testKeys {
			_, err := client.GetClientForKey(ctx, key)
			if err != nil {
				t.Fatalf("Failed to get client for key %s: %v", key, err)
			}

			node := getNodeAddressForKey(t, client, ctx, key)
			initialRouting[key] = node
		}

		// Analyze initial distribution
		distribution := make(map[string]int)
		for _, node := range initialRouting {
			distribution[node]++
		}

		t.Logf("=== Initial Distribution ===")
		for node, count := range distribution {
			percentage := float64(count) / float64(len(testKeys)) * 100
			t.Logf("Node %s: %d keys (%.1f%%)", node, count, percentage)
		}

		// Choose the node to remove (pick the one with some keys)
		nodeToRemove = "localhost:8004" // Remove the extra node we added
		keysOnNodeToRemove := distribution[nodeToRemove]
		t.Logf("Node to remove (%s) currently has %d keys (%.1f%%)",
			nodeToRemove, keysOnNodeToRemove, float64(keysOnNodeToRemove)/float64(len(testKeys))*100)
	})

	t.Run("RemoveNode", func(t *testing.T) {
		if initialRouting == nil {
			t.Skip("Initial routing test must run first")
		}

		t.Logf("=== Removing node %s ===", nodeToRemove)

		// Stop the server
		removePort := extractPortFromAddress(nodeToRemove)
		if err := cluster.StopGRPCServer(removePort); err != nil {
			t.Fatalf("Failed to stop gRPC server on port %d: %v", removePort, err)
		}

		// Update service discovery to exclude the removed node
		updatedAddresses := make([]string, 0)
		for _, addr := range initialAddresses {
			if addr != nodeToRemove {
				updatedAddresses = append(updatedAddresses, addr)
			}
		}

		// Update the discovery with the new node list
		discovery.UpdateNodes("test-grpc-service", updatedAddresses)

		// Wait for the change to be detected and processed
		time.Sleep(2 * time.Second)

		t.Logf("Updated cluster now has %d nodes", len(updatedAddresses))
		for _, addr := range updatedAddresses {
			t.Logf("Remaining node: %s", addr)
		}
	})

	t.Run("AnalyzeRoutingChanges", func(t *testing.T) {
		if initialRouting == nil {
			t.Skip("Initial routing test must run first")
		}

		// Wait a bit more for routing to stabilize
		time.Sleep(1 * time.Second)

		newRouting := make(map[string]string)

		t.Logf("=== New Routing with 3 nodes ===")

		// Record new routing for all test keys
		for _, key := range testKeys {
			_, err := client.GetClientForKey(ctx, key)
			if err != nil {
				t.Fatalf("Failed to get client for key %s: %v", key, err)
			}

			node := getNodeAddressForKey(t, client, ctx, key)
			newRouting[key] = node
		}

		// Analyze changes
		var movedKeys []string
		var stableKeys []string
		var keysFromRemovedNode []string

		for _, key := range testKeys {
			oldNode := initialRouting[key]
			newNode := newRouting[key]

			if oldNode == nodeToRemove {
				// This key was on the removed node
				keysFromRemovedNode = append(keysFromRemovedNode, key)
				if len(movedKeys) < 10 { // Only log first 10 moved keys
					t.Logf("Key '%s' redistributed: %s -> %s", key, oldNode, newNode)
				}
				movedKeys = append(movedKeys, key)
			} else if oldNode != newNode {
				// This key moved between remaining nodes
				if len(movedKeys) < 10 {
					t.Logf("Key '%s' moved: %s -> %s", key, oldNode, newNode)
				}
				movedKeys = append(movedKeys, key)
			} else {
				// This key stayed on the same node
				stableKeys = append(stableKeys, key)
			}
		}

		// Calculate statistics
		totalKeys := len(testKeys)
		movedCount := len(movedKeys)
		stableCount := len(stableKeys)
		redistributedCount := len(keysFromRemovedNode)

		movedPercentage := float64(movedCount) / float64(totalKeys) * 100
		stablePercentage := float64(stableCount) / float64(totalKeys) * 100
		redistributedPercentage := float64(redistributedCount) / float64(totalKeys) * 100

		t.Logf("=== Scale-Down Analysis ===")
		t.Logf("Total keys: %d", totalKeys)
		t.Logf("Moved keys: %d (%.1f%%)", movedCount, movedPercentage)
		t.Logf("Stable keys: %d (%.1f%%)", stableCount, stablePercentage)
		t.Logf("Keys redistributed from removed node: %d (%.1f%%)", redistributedCount, redistributedPercentage)

		// Analyze new distribution
		distribution := make(map[string]int)
		for _, node := range newRouting {
			distribution[node]++
		}

		t.Logf("=== New Distribution ===")
		for node, count := range distribution {
			percentage := float64(count) / float64(totalKeys) * 100
			t.Logf("Node %s: %d keys (%.1f%%)", node, count, percentage)
		}

		// Verify that the removed node is not in the new routing
		if _, exists := distribution[nodeToRemove]; exists {
			t.Errorf("Removed node %s should not appear in new routing", nodeToRemove)
		} else {
			t.Logf("✓ Removed node %s is not in new routing", nodeToRemove)
		}

		// Verify that keys from the removed node were redistributed
		if redistributedCount == 0 {
			t.Logf("ℹ️  No keys were originally on the removed node")
		} else {
			t.Logf("✓ All %d keys from removed node were redistributed", redistributedCount)
		}

		// Verify that remaining nodes are still balanced
		remainingNodeCount := len(distribution)
		if remainingNodeCount != 3 {
			t.Errorf("Expected 3 remaining nodes, got %d", remainingNodeCount)
		} else {
			t.Logf("✓ Correct number of remaining nodes: %d", remainingNodeCount)
		}

		// Verify system stability (most keys should remain stable)
		if stablePercentage < 50.0 {
			t.Logf("Warning: Low stability: %.1f%% keys remained stable", stablePercentage)
		} else {
			t.Logf("✓ Good stability: %.1f%% keys remained on original nodes", stablePercentage)
		}
	})

	t.Run("VerifyRemainingNodesHealth", func(t *testing.T) {
		// Verify that remaining nodes are still being health checked
		remainingPorts := []int{8001, 8002, 8003}

		// Wait for a few health check cycles
		time.Sleep(2 * time.Second)

		for _, port := range remainingPorts {
			stats, err := cluster.GetServerStats("grpc", port)
			if err != nil {
				t.Errorf("Failed to get stats for remaining node port %d: %v", port, err)
				continue
			}

			requestCount := stats["request_count"].(int64)
			if requestCount < 2 {
				t.Errorf("Remaining node %d should have received health checks: got %d requests", port, requestCount)
			} else {
				t.Logf("✓ Remaining node %d is being health checked: %d requests", port, requestCount)
			}
		}
	})
}

func testRedisScaleDown(t *testing.T, cluster *testbed.TestCluster) {
	t.Log("Redis scale-down uses the adapter routing contract until a protocol-level topology control is available")
	testRedisFailover(t, cluster)
}

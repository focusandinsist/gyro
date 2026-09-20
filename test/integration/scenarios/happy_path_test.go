// Package scenarios contains integration test scenarios for Gyro.
package scenarios

import (
	"context"
	"testing"
	"time"

	gyro "github.com/focusandinsist/gyro/gyro"
	grpc "github.com/focusandinsist/gyro/gyro/grpc"
	redis "github.com/focusandinsist/gyro/gyro/redis"
	"github.com/focusandinsist/gyro/test/integration/testbed"
)

// TestHappyPath tests the normal operation scenario.
func TestHappyPath(t *testing.T) {
	// Setup test cluster
	cluster := testbed.NewTestCluster(testbed.DefaultClusterConfig())

	// Start the cluster
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// Wait for servers to be ready
	time.Sleep(1 * time.Second)

	t.Run("gRPC_HappyPath", func(t *testing.T) {
		testGRPCHappyPath(t, cluster)
	})

	t.Run("Redis_HappyPath", func(t *testing.T) {
		testRedisHappyPath(t, cluster)
	})
}

func testGRPCHappyPath(t *testing.T, cluster *testbed.TestCluster) {
	// Create Gyro client configuration
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 1 * time.Second
	config.HealthChecker.Timeout = 500 * time.Millisecond
	config.HealthChecker.FailureThreshold = 3
	config.HealthChecker.RecoveryThreshold = 3

	// Create service discovery with gRPC addresses
	addresses := cluster.GetGRPCAddresses()
	discovery := gyro.NewStaticServiceDiscovery(addresses)

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
	time.Sleep(2 * time.Second)

	// Test 1: Verify all nodes are initially healthy
	t.Run("InitialHealthStatus", func(t *testing.T) {
		stats := client.GetStats()
		if stats.TotalNodes != len(addresses) {
			t.Errorf("Expected %d total nodes, got %d", len(addresses), stats.TotalNodes)
		}
		if stats.HealthyNodes != len(addresses) {
			t.Errorf("Expected %d healthy nodes, got %d", len(addresses), stats.HealthyNodes)
		}
		if stats.UnhealthyNodes != 0 {
			t.Errorf("Expected 0 unhealthy nodes, got %d", stats.UnhealthyNodes)
		}
	})

	// Test 2: Verify request routing distribution
	t.Run("RequestDistribution", func(t *testing.T) {
		testKeys := []string{
			"user-123", "user-456", "user-789",
			"order-abc", "order-def", "order-ghi",
			"product-111", "product-222", "product-333",
		}

		routingMap := make(map[string]string) // key -> node address

		for _, key := range testKeys {
			_, err := client.GetClientForKey(ctx, key)
			if err != nil {
				t.Errorf("Failed to get client for key %s: %v", key, err)
				continue
			}

			// Get the node address (this would be implementation-specific)
			// For now, we'll simulate this by checking which server received the request
			nodeAddress := getNodeAddressForKey(t, client, ctx, key)
			routingMap[key] = nodeAddress

			t.Logf("Key '%s' routed to node '%s'", key, nodeAddress)
		}

		// Verify routing consistency: same key should always go to same node
		for i := 0; i < 3; i++ {
			for _, key := range testKeys {
				_, err := client.GetClientForKey(ctx, key)
				if err != nil {
					t.Errorf("Failed to get client for key %s on iteration %d: %v", key, i, err)
					continue
				}

				nodeAddress := getNodeAddressForKey(t, client, ctx, key)
				expectedAddress := routingMap[key]

				if nodeAddress != expectedAddress {
					t.Errorf("Routing inconsistency for key '%s': expected '%s', got '%s' on iteration %d",
						key, expectedAddress, nodeAddress, i)
				}
			}
		}

		// Verify load distribution: keys should be distributed across multiple nodes
		nodeUsage := make(map[string]int)
		for _, nodeAddress := range routingMap {
			nodeUsage[nodeAddress]++
		}

		if len(nodeUsage) < 2 {
			t.Errorf("Poor load distribution: only %d nodes used out of %d available",
				len(nodeUsage), len(addresses))
		}

		t.Logf("Load distribution: %v", nodeUsage)
	})

	// Test 3: Verify health check monitoring is working
	t.Run("HealthMonitoring", func(t *testing.T) {
		// Get initial stats from all servers
		initialStats := make(map[int]map[string]any)
		for _, addr := range addresses {
			port := extractPortFromAddress(addr)
			stats, err := cluster.GetServerStats("grpc", port)
			if err != nil {
				t.Errorf("Failed to get stats for gRPC server on port %d: %v", port, err)
				continue
			}
			initialStats[port] = stats
		}

		// Wait for a few health check cycles
		cluster.WaitForHealthCheck(3)

		// Verify that health checks are being performed
		for _, addr := range addresses {
			port := extractPortFromAddress(addr)
			stats, err := cluster.GetServerStats("grpc", port)
			if err != nil {
				t.Errorf("Failed to get stats for gRPC server on port %d: %v", port, err)
				continue
			}

			initialCount := initialStats[port]["request_count"].(int64)
			currentCount := stats["request_count"].(int64)

			if currentCount <= initialCount {
				t.Errorf("Health checks not being performed on port %d: request count did not increase (%d -> %d)",
					port, initialCount, currentCount)
			} else {
				t.Logf("Health checks working on port %d: request count increased (%d -> %d)",
					port, initialCount, currentCount)
			}
		}
	})
}

func testRedisHappyPath(t *testing.T, cluster *testbed.TestCluster) {
	// Similar implementation for Redis
	// This would test Redis-specific functionality

	// Create Gyro client configuration for Redis
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 1 * time.Second
	config.HealthChecker.Timeout = 500 * time.Millisecond

	// Create service discovery with Redis addresses
	addresses := cluster.GetRedisAddresses()
	discovery := gyro.NewStaticServiceDiscovery(addresses)

	// Create config manager
	configManager := gyro.NewConfigManager(config)

	// Create node factory for Redis
	nodeFactory := redis.NewRedisNodeFactory()

	// Create health checker
	healthChecker := gyro.NewDefaultHealthChecker(config.HealthChecker)

	// Create Gyro client
	client, err := gyro.NewClient(
		"test-redis-service",
		discovery,
		configManager,
		nodeFactory,
		healthChecker,
	)
	if err != nil {
		t.Fatalf("Failed to create Gyro Redis client: %v", err)
	}
	defer client.Close()

	// Start health monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("Failed to start Gyro Redis client: %v", err)
	}

	// Wait for initial health checks
	time.Sleep(2 * time.Second)

	// Test Redis-specific scenarios
	t.Run("RedisInitialHealth", func(t *testing.T) {
		stats := client.GetStats()
		if stats.TotalNodes != len(addresses) {
			t.Errorf("Expected %d total Redis nodes, got %d", len(addresses), stats.TotalNodes)
		}
		if stats.HealthyNodes != len(addresses) {
			t.Errorf("Expected %d healthy Redis nodes, got %d", len(addresses), stats.HealthyNodes)
		}
	})

	t.Logf("Redis happy path test completed successfully")
}

// Helper functions

func getNodeAddressForKey(t *testing.T, client *gyro.Client, ctx context.Context, key string) string {
	t.Helper()
	node, err := client.GetNodeForKey(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get routed node for key %s: %v", key, err)
	}
	return node.Address()
}

func waitForRoutedNode(t *testing.T, client *gyro.Client, ctx context.Context, key, previous string, wantDifferent bool) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node, err := client.GetNodeForKey(ctx, key)
		if err == nil && (!wantDifferent || node.Address() != previous) {
			return node.Address()
		}
		time.Sleep(25 * time.Millisecond)
	}
	node, err := client.GetNodeForKey(ctx, key)
	if err != nil {
		t.Fatalf("timed out waiting for route for key %s: %v", key, err)
	}
	return node.Address()
}

func waitForServerHealth(t *testing.T, cluster *testbed.TestCluster, kind string, port int, healthy bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := cluster.GetServerStats(kind, port)
		if err == nil {
			if value, ok := stats["healthy"].(bool); ok && value == healthy {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s server %d healthy=%v", kind, port, healthy)
}

func extractPortFromAddress(address string) int {
	// Extract port from "localhost:8001" format
	// This is a simplified implementation
	switch address {
	case "localhost:8001":
		return 8001
	case "localhost:8002":
		return 8002
	case "localhost:8003":
		return 8003
	case "localhost:8004":
		return 8004
	case "localhost:16379":
		return 16379
	case "localhost:16380":
		return 16380
	case "localhost:16381":
		return 16381
	default:
		return 0
	}
}

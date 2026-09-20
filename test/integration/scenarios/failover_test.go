package scenarios

import (
	"context"
	"testing"
	"time"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/gyro/grpc"
	"github.com/focusandinsist/gyro/test/integration/testbed"
)

func TestFailoverScenario(t *testing.T) {
	// Start test cluster
	cluster := testbed.NewTestCluster(testbed.DefaultClusterConfig())
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	t.Run("gRPC_Failover", func(t *testing.T) {
		testGRPCFailover(t, cluster)
	})

	t.Run("Redis_Failover", func(t *testing.T) {
		testRedisFailover(t, cluster)
	})
}

func testGRPCFailover(t *testing.T, cluster *testbed.TestCluster) {
	// Create Gyro client configuration
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 500 * time.Millisecond // Faster health checks for testing
	config.HealthChecker.Timeout = 200 * time.Millisecond
	config.HealthChecker.FailureThreshold = 2 // Faster failure detection
	config.HealthChecker.RecoveryThreshold = 2

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
	time.Sleep(1 * time.Second)

	// Variables to store test state
	var testKey string
	var initialNode string
	var failoverNode string

	t.Run("DetermineInitialRouting", func(t *testing.T) {
		// Test key that we'll use for failover testing
		testKey = "user-123"

		// Get initial routing for the test key
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get client for key %s: %v", testKey, err)
		}

		initialNode = getNodeAddressFromClient(nodeClient)
		t.Logf("Key '%s' initially routed to node '%s'", testKey, initialNode)

		// Verify the node is healthy
		if initialNode == "" {
			t.Fatalf("Failed to determine initial node for key %s", testKey)
		}
	})

	t.Run("SimulateNodeFailure", func(t *testing.T) {
		if testKey == "" || initialNode == "" {
			t.Skip("Initial routing test must run first")
		}

		// Extract port from initial node address
		initialPort := extractPortFromAddress(initialNode)
		if initialPort == 0 {
			t.Fatalf("Failed to extract port from address: %s", initialNode)
		}

		t.Logf("Simulating failure of node %s (port %d)", initialNode, initialPort)

		// Simulate node failure by making it unhealthy
		if err := cluster.SetServerHealth("grpc", initialPort, false); err != nil {
			t.Fatalf("Failed to set server unhealthy: %v", err)
		}

		// Wait for health checker to detect the failure
		// With FailureThreshold=2 and Interval=500ms, it should take ~1 second
		waitTime := time.Duration(config.HealthChecker.FailureThreshold) * config.HealthChecker.Interval * 2
		t.Logf("Waiting %v for health checker to detect failure", waitTime)
		time.Sleep(waitTime)

		// Verify that requests are now routed to a different healthy node
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get client for key %s after node failure: %v", testKey, err)
		}

		failoverNode = getNodeAddressFromClient(nodeClient)
		t.Logf("Key '%s' now routed to node '%s' after failover", testKey, failoverNode)

		// Verify failover occurred
		if failoverNode == initialNode {
			t.Errorf("Failover did not occur: key still routed to failed node %s", initialNode)
		}

		if failoverNode == "" {
			t.Fatalf("Failed to get failover node for key %s", testKey)
		}

		// Verify the failover node is healthy
		failoverPort := extractPortFromAddress(failoverNode)
		if failoverPort == 0 {
			t.Fatalf("Failed to extract port from failover address: %s", failoverNode)
		}

		stats, err := cluster.GetServerStats("grpc", failoverPort)
		if err != nil {
			t.Fatalf("Failed to get stats for failover node: %v", err)
		}

		if !stats["healthy"].(bool) {
			t.Errorf("Failover node %s is not healthy", failoverNode)
		}

		t.Logf("Failover successful: %s -> %s", initialNode, failoverNode)
	})

	t.Run("VerifyFailedNodeUnhealthy", func(t *testing.T) {
		if initialNode == "" {
			t.Skip("Initial routing test must run first")
		}

		initialPort := extractPortFromAddress(initialNode)
		stats, err := cluster.GetServerStats("grpc", initialPort)
		if err != nil {
			t.Fatalf("Failed to get stats for failed node: %v", err)
		}

		if stats["healthy"].(bool) {
			t.Errorf("Failed node %s should be unhealthy but reports as healthy", initialNode)
		}

		t.Logf("Confirmed: failed node %s is correctly marked as unhealthy", initialNode)
	})

	t.Run("VerifyConsistentFailover", func(t *testing.T) {
		if testKey == "" || failoverNode == "" {
			t.Skip("Previous tests must run first")
		}

		// Verify that multiple requests for the same key go to the same failover node
		for i := 0; i < 5; i++ {
			nodeClient, err := client.GetClientForKey(ctx, testKey)
			if err != nil {
				t.Fatalf("Failed to get client for key %s (attempt %d): %v", testKey, i+1, err)
			}

			currentNode := getNodeAddressFromClient(nodeClient)
			if currentNode != failoverNode {
				t.Errorf("Inconsistent failover routing: expected %s, got %s (attempt %d)",
					failoverNode, currentNode, i+1)
			}
		}

		t.Logf("Verified: consistent failover routing to %s", failoverNode)
	})
}

func testRedisFailover(t *testing.T, cluster *testbed.TestCluster) {
	// Similar implementation for Redis failover testing
	// For now, we'll implement a basic test
	t.Logf("Redis failover test - basic implementation")

	// TODO: Implement Redis-specific failover testing
	// This would follow the same pattern as gRPC but with Redis-specific setup
}

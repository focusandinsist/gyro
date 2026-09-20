package scenarios

import (
	"context"
	"testing"
	"time"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/gyro/grpc"
	"github.com/focusandinsist/gyro/test/integration/testbed"
)

func TestRecoveryScenario(t *testing.T) {
	// Start test cluster
	cluster := testbed.NewTestCluster(testbed.DefaultClusterConfig())
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	t.Run("gRPC_Recovery", func(t *testing.T) {
		testGRPCRecovery(t, cluster)
	})

	t.Run("Redis_Recovery", func(t *testing.T) {
		testRedisRecovery(t, cluster)
	})
}

func testGRPCRecovery(t *testing.T, cluster *testbed.TestCluster) {
	// Create Gyro client configuration
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 500 * time.Millisecond // Faster health checks for testing
	config.HealthChecker.Timeout = 200 * time.Millisecond
	config.HealthChecker.FailureThreshold = 2  // Faster failure detection
	config.HealthChecker.RecoveryThreshold = 2 // Faster recovery detection

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
	var originalNode string
	var failoverNode string

	t.Run("EstablishInitialState", func(t *testing.T) {
		// Test key that we'll use for recovery testing
		testKey = "recovery-test-123"

		// Get initial routing for the test key
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get client for key %s: %v", testKey, err)
		}

		originalNode = getNodeAddressFromClient(nodeClient)
		t.Logf("Key '%s' initially routed to node '%s'", testKey, originalNode)

		// Verify the node is healthy
		if originalNode == "" {
			t.Fatalf("Failed to determine original node for key %s", testKey)
		}
	})

	t.Run("SimulateFailureAndFailover", func(t *testing.T) {
		if testKey == "" || originalNode == "" {
			t.Skip("Initial state test must run first")
		}

		// Extract port from original node address
		originalPort := extractPortFromAddress(originalNode)
		if originalPort == 0 {
			t.Fatalf("Failed to extract port from address: %s", originalNode)
		}

		t.Logf("Simulating failure of original node %s (port %d)", originalNode, originalPort)

		// Simulate node failure by making it unhealthy
		if err := cluster.SetServerHealth("grpc", originalPort, false); err != nil {
			t.Fatalf("Failed to set server unhealthy: %v", err)
		}

		// Wait for health checker to detect the failure
		waitTime := time.Duration(config.HealthChecker.FailureThreshold) * config.HealthChecker.Interval * 2
		t.Logf("Waiting %v for health checker to detect failure", waitTime)
		time.Sleep(waitTime)

		// Verify that requests are now routed to a different healthy node
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get client for key %s after node failure: %v", testKey, err)
		}

		failoverNode = getNodeAddressFromClient(nodeClient)
		t.Logf("Key '%s' now routed to failover node '%s'", testKey, failoverNode)

		// Verify failover occurred
		if failoverNode == originalNode {
			t.Errorf("Failover did not occur: key still routed to failed node %s", originalNode)
		}

		if failoverNode == "" {
			t.Fatalf("Failed to get failover node for key %s", testKey)
		}

		t.Logf("Failover completed: %s -> %s", originalNode, failoverNode)
	})

	t.Run("SimulateRecovery", func(t *testing.T) {
		if testKey == "" || originalNode == "" || failoverNode == "" {
			t.Skip("Previous tests must run first")
		}

		// Extract port from original node address
		originalPort := extractPortFromAddress(originalNode)
		if originalPort == 0 {
			t.Fatalf("Failed to extract port from address: %s", originalNode)
		}

		t.Logf("Simulating recovery of original node %s (port %d)", originalNode, originalPort)

		// Simulate node recovery by making it healthy again
		if err := cluster.SetServerHealth("grpc", originalPort, true); err != nil {
			t.Fatalf("Failed to set server healthy: %v", err)
		}

		// Wait for health checker to detect the recovery
		waitTime := time.Duration(config.HealthChecker.RecoveryThreshold) * config.HealthChecker.Interval * 2
		t.Logf("Waiting %v for health checker to detect recovery", waitTime)
		time.Sleep(waitTime)

		// Verify that the original node is now healthy
		stats, err := cluster.GetServerStats("grpc", originalPort)
		if err != nil {
			t.Fatalf("Failed to get stats for recovered node: %v", err)
		}

		if !stats["healthy"].(bool) {
			t.Errorf("Recovered node %s should be healthy but reports as unhealthy", originalNode)
		}

		t.Logf("Confirmed: original node %s has recovered and is healthy", originalNode)
	})

	t.Run("VerifyStickyRouting", func(t *testing.T) {
		if testKey == "" || originalNode == "" || failoverNode == "" {
			t.Skip("Previous tests must run first")
		}

		// After recovery, verify that requests return to the original node (sticky routing)
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Fatalf("Failed to get client for key %s after recovery: %v", testKey, err)
		}

		currentNode := getNodeAddressFromClient(nodeClient)
		t.Logf("Key '%s' now routed to node '%s' after recovery", testKey, currentNode)

		// Verify sticky routing: should return to original node
		if currentNode != originalNode {
			t.Errorf("Sticky routing failed: expected %s, got %s", originalNode, currentNode)
		} else {
			t.Logf("Sticky routing successful: %s -> %s -> %s", originalNode, failoverNode, currentNode)
		}
	})

	t.Run("VerifyConsistentRecovery", func(t *testing.T) {
		if testKey == "" || originalNode == "" {
			t.Skip("Previous tests must run first")
		}

		// Verify that multiple requests for the same key consistently go to the original node
		for i := 0; i < 5; i++ {
			nodeClient, err := client.GetClientForKey(ctx, testKey)
			if err != nil {
				t.Fatalf("Failed to get client for key %s (attempt %d): %v", testKey, i+1, err)
			}

			currentNode := getNodeAddressFromClient(nodeClient)
			if currentNode != originalNode {
				t.Errorf("Inconsistent recovery routing: expected %s, got %s (attempt %d)",
					originalNode, currentNode, i+1)
			}
		}

		t.Logf("Verified: consistent recovery routing to original node %s", originalNode)
	})
}

func testRedisRecovery(t *testing.T, cluster *testbed.TestCluster) {
	// Similar implementation for Redis recovery testing
	// For now, we'll implement a basic test
	t.Logf("Redis recovery test - basic implementation")

	// TODO: Implement Redis-specific recovery testing
	// This would follow the same pattern as gRPC but with Redis-specific setup
}

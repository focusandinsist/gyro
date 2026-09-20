package scenarios

import (
	"context"
	"testing"
	"time"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/gyro/grpc"
	"github.com/focusandinsist/gyro/test/integration/testbed"
)

func TestDebugHealthChecker(t *testing.T) {
	// Start test cluster
	cluster := testbed.NewTestCluster(testbed.DefaultClusterConfig())
	if err := cluster.Start(); err != nil {
		t.Fatalf("Failed to start test cluster: %v", err)
	}
	defer cluster.Stop()

	// Create Gyro client configuration
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Interval = 1 * time.Second // Slower for easier debugging
	config.HealthChecker.Timeout = 500 * time.Millisecond
	config.HealthChecker.FailureThreshold = 2
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
	time.Sleep(2 * time.Second)

	t.Log("=== Initial Health Check Stats ===")
	for _, addr := range addresses {
		port := extractPortFromAddress(addr)
		stats, err := cluster.GetServerStats("grpc", port)
		if err != nil {
			t.Errorf("Failed to get stats for port %d: %v", port, err)
			continue
		}
		t.Logf("Port %d: healthy=%v, request_count=%v", port, stats["healthy"], stats["request_count"])
	}

	// Simulate failure of one node
	testPort := 8003
	t.Logf("=== Simulating failure of port %d ===", testPort)
	if err := cluster.SetServerHealth("grpc", testPort, false); err != nil {
		t.Fatalf("Failed to set server unhealthy: %v", err)
	}

	// Wait for failure detection
	time.Sleep(3 * time.Second)

	t.Log("=== Health Check Stats After Failure ===")
	for _, addr := range addresses {
		port := extractPortFromAddress(addr)
		stats, err := cluster.GetServerStats("grpc", port)
		if err != nil {
			t.Errorf("Failed to get stats for port %d: %v", port, err)
			continue
		}
		t.Logf("Port %d: healthy=%v, request_count=%v", port, stats["healthy"], stats["request_count"])
	}

	// Simulate recovery
	t.Logf("=== Simulating recovery of port %d ===", testPort)
	if err := cluster.SetServerHealth("grpc", testPort, true); err != nil {
		t.Fatalf("Failed to set server healthy: %v", err)
	}

	// Wait for recovery detection
	time.Sleep(3 * time.Second)

	t.Log("=== Health Check Stats After Recovery ===")
	for _, addr := range addresses {
		port := extractPortFromAddress(addr)
		stats, err := cluster.GetServerStats("grpc", port)
		if err != nil {
			t.Errorf("Failed to get stats for port %d: %v", port, err)
			continue
		}
		t.Logf("Port %d: healthy=%v, request_count=%v", port, stats["healthy"], stats["request_count"])
	}

	// Test routing behavior
	testKey := "debug-test-key"
	t.Logf("=== Testing routing for key '%s' ===", testKey)

	for i := 0; i < 3; i++ {
		nodeClient, err := client.GetClientForKey(ctx, testKey)
		if err != nil {
			t.Errorf("Failed to get client for key %s (attempt %d): %v", testKey, i+1, err)
			continue
		}

		currentNode := getNodeAddressFromClient(nodeClient)
		t.Logf("Attempt %d: Key '%s' routed to node '%s'", i+1, testKey, currentNode)

		time.Sleep(500 * time.Millisecond)
	}
}

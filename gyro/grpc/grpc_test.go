package grpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gyro/gyro"
)

type testGRPCNativeClient struct {
	address string
}

type testGRPCConnection struct {
	native  *testGRPCNativeClient
	healthy atomic.Bool
	closed  atomic.Bool
}

func newTestGRPCConnection(address string) *testGRPCConnection {
	connection := &testGRPCConnection{native: &testGRPCNativeClient{address: address}}
	connection.healthy.Store(true)
	return connection
}

func (c *testGRPCConnection) Ping(context.Context) error {
	if c.closed.Load() || !c.healthy.Load() {
		return fmt.Errorf("connection is unhealthy")
	}
	return nil
}

func (c *testGRPCConnection) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *testGRPCConnection) IsConnected() bool {
	return !c.closed.Load() && c.healthy.Load()
}

func (c *testGRPCConnection) GetState() string { return "READY" }

func (c *testGRPCConnection) GetNativeClient() any { return c.native }

func TestGRPCConvenienceClientUsesHealthAwareFailover(t *testing.T) {
	config := DefaultGRPCClientConfig()
	config.HealthChecker.Interval = 5 * time.Millisecond
	config.HealthChecker.Timeout = 5 * time.Millisecond
	config.HealthChecker.FailureThreshold = 1
	config.HealthChecker.RecoveryThreshold = 1

	connections := make(map[string]*testGRPCConnection)
	factory := &GRPCNodeFactory{
		config: config,
		newConnection: func(address string, _ gyro.ConnectionConfig) (GRPCConnection, error) {
			connection := newTestGRPCConnection(address)
			connections[address] = connection
			return connection, nil
		},
	}

	client, err := newGRPCClient(
		[]string{"grpc-1.test", "grpc-2.test", "grpc-3.test"},
		config,
		factory,
		nil,
	)
	if err != nil {
		t.Fatalf("NewGRPCClient failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pool, ok := client.locator.(*gyro.HealthAwarePool)
	if !ok {
		t.Fatalf("convenience client locator is %T, want *gyro.HealthAwarePool", client.locator)
	}

	key := findGRPCKeyForNode(t, client.locator, "grpc-1")
	connections["grpc-1.test"].healthy.Store(false)
	waitForGRPCNodeHealth(t, pool, "grpc-1", false)

	native, err := client.GetClientForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("GetClientForKey failed after primary became unhealthy: %v", err)
	}
	fallback, ok := native.(*testGRPCNativeClient)
	if !ok {
		t.Fatalf("native client is %T, want *testGRPCNativeClient", native)
	}
	if fallback.address == "grpc-1.test" {
		t.Fatal("health-aware routing returned the unhealthy primary")
	}

	connections["grpc-1.test"].healthy.Store(true)
	waitForGRPCNodeHealth(t, pool, "grpc-1", true)
	native, err = client.GetClientForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("GetClientForKey failed after primary recovered: %v", err)
	}
	if native.(*testGRPCNativeClient).address != "grpc-1.test" {
		t.Fatal("health-aware routing did not return to the recovered primary")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}
	for address, connection := range connections {
		if !connection.closed.Load() {
			t.Fatalf("connection %s was not closed", address)
		}
	}
}

func TestGRPCConvenienceClientRejectsInvalidHealthConfig(t *testing.T) {
	config := DefaultGRPCClientConfig()
	config.HealthChecker.Interval = 0

	client, err := NewGRPCClient([]string{"grpc-1.test"}, config)
	if err == nil {
		client.Close()
		t.Fatal("expected invalid health checker config to fail")
	}
}

func findGRPCKeyForNode(t *testing.T, locator gyro.Locator, nodeID string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("grpc-key-%d", i)
		node, err := locator.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("locate key %q: %v", key, err)
		}
		if node.ID() == nodeID {
			return key
		}
	}
	t.Fatalf("could not find a key for node %s", nodeID)
	return ""
}

func waitForGRPCNodeHealth(t *testing.T, pool *gyro.HealthAwarePool, nodeID string, healthy bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pool.IsNodeHealthy(nodeID) == healthy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s health did not become %v", nodeID, healthy)
}

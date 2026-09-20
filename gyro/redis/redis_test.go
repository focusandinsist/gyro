package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/focusandinsist/gyro/gyro"
)

type testRedisNativeClient struct {
	address string
}

type testRedisConnection struct {
	native  *testRedisNativeClient
	healthy atomic.Bool
	closed  atomic.Bool
}

func newTestRedisConnection(address string) *testRedisConnection {
	connection := &testRedisConnection{native: &testRedisNativeClient{address: address}}
	connection.healthy.Store(true)
	return connection
}

func (c *testRedisConnection) Ping(context.Context) error {
	if c.closed.Load() || !c.healthy.Load() {
		return fmt.Errorf("connection is unhealthy")
	}
	return nil
}

func (c *testRedisConnection) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *testRedisConnection) IsConnected() bool {
	return !c.closed.Load() && c.healthy.Load()
}

func (c *testRedisConnection) GetNativeClient() any { return c.native }

func TestRedisConvenienceClientUsesHealthAwareFailover(t *testing.T) {
	config := DefaultRedisClientConfig()
	config.HealthChecker.Interval = 5 * time.Millisecond
	config.HealthChecker.Timeout = 5 * time.Millisecond
	config.HealthChecker.FailureThreshold = 1
	config.HealthChecker.RecoveryThreshold = 1

	connections := make(map[string]*testRedisConnection)
	factory := &RedisNodeFactory{
		config: config,
		newConnection: func(address string, _ gyro.ConnectionConfig) (RedisConnection, error) {
			connection := newTestRedisConnection(address)
			connections[address] = connection
			return connection, nil
		},
	}

	client, err := newRedisClient(
		[]string{"redis-1.test", "redis-2.test", "redis-3.test"},
		config,
		factory,
		nil,
	)
	if err != nil {
		t.Fatalf("NewRedisClient failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pool, ok := client.locator.(*gyro.HealthAwarePool)
	if !ok {
		t.Fatalf("convenience client locator is %T, want *gyro.HealthAwarePool", client.locator)
	}

	key := findRedisKeyForNode(t, client.locator, "redis-1")
	connections["redis-1.test"].healthy.Store(false)
	waitForRedisNodeHealth(t, pool, "redis-1", false)

	native, err := client.GetClientForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("GetClientForKey failed after primary became unhealthy: %v", err)
	}
	fallback, ok := native.(*testRedisNativeClient)
	if !ok {
		t.Fatalf("native client is %T, want *testRedisNativeClient", native)
	}
	if fallback.address == "redis-1.test" {
		t.Fatal("health-aware routing returned the unhealthy primary")
	}

	connections["redis-1.test"].healthy.Store(true)
	waitForRedisNodeHealth(t, pool, "redis-1", true)
	native, err = client.GetClientForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("GetClientForKey failed after primary recovered: %v", err)
	}
	if native.(*testRedisNativeClient).address != "redis-1.test" {
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

func TestRedisConvenienceClientRejectsInvalidHealthConfig(t *testing.T) {
	config := DefaultRedisClientConfig()
	config.HealthChecker.Interval = 0

	client, err := NewRedisClient([]string{"redis-1.test"}, config)
	if err == nil {
		client.Close()
		t.Fatal("expected invalid health checker config to fail")
	}
}

func TestRedisNodeDoesNotGateNativeClientOnSingleFailedProbe(t *testing.T) {
	connection := newTestRedisConnection("redis.test")
	node := NewRedisNode("redis-1", "redis.test", connection)
	connection.healthy.Store(false)
	if node.IsHealthy(context.Background()) {
		t.Fatal("probe should report the connection unhealthy")
	}
	if node.GetNativeClient() != connection.native {
		t.Fatal("probe result must not override the health pool's threshold decision")
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if node.GetNativeClient() != nil {
		t.Fatal("closed node must not expose its native client")
	}
}

func findRedisKeyForNode(t *testing.T, locator gyro.Locator, nodeID string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("redis-key-%d", i)
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

func waitForRedisNodeHealth(t *testing.T, pool *gyro.HealthAwarePool, nodeID string, healthy bool) {
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

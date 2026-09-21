package gyro

import (
	"context"
	"testing"
)

type readOnlyDiscovery struct{}

func (readOnlyDiscovery) Discover(context.Context, string) ([]NodeInfo, error) {
	return []NodeInfo{{ID: "node-1", Address: "node-1"}}, nil
}

func (readOnlyDiscovery) Watch(context.Context, string) (<-chan []NodeInfo, error) {
	updates := make(chan []NodeInfo)
	close(updates)
	return updates, nil
}

type minimalHealthChecker struct{}

func (minimalHealthChecker) Check(context.Context, Node) error { return nil }
func (minimalHealthChecker) AddNode(Node)                      {}
func (minimalHealthChecker) RemoveNode(string)                 {}
func (minimalHealthChecker) StartMonitoring(context.Context)   {}
func (minimalHealthChecker) StopMonitoring()                   {}
func (minimalHealthChecker) IsNodeHealthy(string) bool         { return true }
func (minimalHealthChecker) AddHealthListener(HealthListener)  {}

var _ ServiceDiscovery = readOnlyDiscovery{}
var _ HealthChecker = minimalHealthChecker{}

func TestClientAcceptsReadOnlyDiscoveryAndMinimalHealthChecker(t *testing.T) {
	config := DefaultClientConfig()
	config.HealthChecker.Enabled = false
	client, err := NewClient(
		"orders",
		readOnlyDiscovery{},
		NewConfigManager(config),
		NewMockNodeFactory(),
		minimalHealthChecker{},
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestStaticDiscoverySeparatesRegistrationCapability(t *testing.T) {
	var _ ServiceRegistrar = (*StaticServiceDiscovery)(nil)
	var _ ServiceDiscovery = (*StaticServiceDiscovery)(nil)
}

package gyro_test

import (
	"context"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
)

type readOnlyDiscovery struct{}

func (readOnlyDiscovery) Discover(context.Context, string) ([]gyro.NodeInfo, error) {
	return []gyro.NodeInfo{{ID: "node-1", Address: "node-1"}}, nil
}

func (readOnlyDiscovery) Watch(context.Context, string) (<-chan []gyro.NodeInfo, error) {
	updates := make(chan []gyro.NodeInfo)
	close(updates)
	return updates, nil
}

type minimalHealthChecker struct{}

func (minimalHealthChecker) Check(context.Context, gyro.Node) error { return nil }
func (minimalHealthChecker) AddNode(gyro.Node)                      {}
func (minimalHealthChecker) RemoveNode(string)                      {}
func (minimalHealthChecker) StartMonitoring(context.Context)        {}
func (minimalHealthChecker) StopMonitoring()                        {}
func (minimalHealthChecker) IsNodeHealthy(string) bool              { return true }
func (minimalHealthChecker) AddHealthListener(gyro.HealthListener)  {}

type contractNode struct{}

func (contractNode) ID() string                     { return "node-1" }
func (contractNode) Address() string                { return "node-1" }
func (contractNode) IsHealthy(context.Context) bool { return true }
func (contractNode) Close() error                   { return nil }

type contractNodeFactory struct{}

func (contractNodeFactory) CreateNode(gyro.NodeInfo) (gyro.Node, error) {
	return contractNode{}, nil
}

var _ gyro.ServiceDiscovery = readOnlyDiscovery{}
var _ gyro.HealthChecker = minimalHealthChecker{}
var _ gyro.NodeFactory = contractNodeFactory{}

func TestClientAcceptsReadOnlyDiscoveryAndMinimalHealthChecker(t *testing.T) {
	config := gyro.DefaultClientConfig()
	config.HealthChecker.Enabled = false
	client, err := gyro.NewClient(
		"orders",
		readOnlyDiscovery{},
		gyro.NewConfigManager(config),
		contractNodeFactory{},
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
	var _ gyro.ServiceRegistrar = (*gyro.StaticServiceDiscovery)(nil)
	var _ gyro.ServiceDiscovery = (*gyro.StaticServiceDiscovery)(nil)
}

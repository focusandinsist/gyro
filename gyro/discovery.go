package gyro

import (
	"context"
	"fmt"
)

type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type TopologyStream interface {
	Next(context.Context) (TopologySnapshot, error)
	Close() error
}

type TopologySource interface {
	Snapshot(context.Context) (TopologySnapshot, error)
	Watch(context.Context) (TopologyStream, error)
}

type ServiceDiscovery interface {
	Discover(context.Context, string) (TopologySnapshot, error)
	Watch(context.Context, string) (TopologyStream, error)
}

func NewServiceTopologySource(discovery ServiceDiscovery, serviceName string) (TopologySource, error) {
	if discovery == nil {
		return nil, fmt.Errorf("service discovery cannot be nil")
	}
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}
	return serviceTopologySource{discovery: discovery, serviceName: serviceName}, nil
}

type serviceTopologySource struct {
	discovery   ServiceDiscovery
	serviceName string
}

func (s serviceTopologySource) Snapshot(ctx context.Context) (TopologySnapshot, error) {
	return s.discovery.Discover(ctx, s.serviceName)
}

func (s serviceTopologySource) Watch(ctx context.Context) (TopologyStream, error) {
	return s.discovery.Watch(ctx, s.serviceName)
}

type ServiceRegistrar interface {
	Register(context.Context, string, NodeInfo) error
	Unregister(context.Context, string, string) error
}

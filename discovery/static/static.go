// Package static provides the reference in-memory service discovery source.
// It is intended for fixed deployments, examples, and tests.
package static

import (
	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/topology"
)

type ServiceDiscovery = topology.StaticDiscovery

func New(addresses []string) *ServiceDiscovery {
	return topology.NewStaticDiscovery(addresses)
}

var _ gyro.ServiceDiscovery = (*ServiceDiscovery)(nil)

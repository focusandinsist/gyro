package client

import (
	"sort"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/health"
	"github.com/focusandinsist/gyro/internal/topology"
)

type ServiceDiscovery = gyro.ServiceDiscovery
type NodeFactory = gyro.NodeFactory
type ConnectionConfigurableNodeFactory = gyro.ConnectionConfigurableNodeFactory
type HealthChecker = gyro.HealthChecker
type ConfigurableHealthChecker = gyro.ConfigurableHealthChecker
type HealthCheckerConfig = gyro.HealthCheckerConfig
type Node = gyro.Node
type NodeInfo = gyro.NodeInfo
type Locator = gyro.Locator
type LocatorConfig = gyro.LocatorConfig
type TopologyStore = gyro.TopologyStore
type TopologySnapshot = gyro.TopologySnapshot
type TopologyStream = gyro.TopologyStream
type HealthAwarePoolStats = gyro.HealthAwarePoolStats

var (
	DefaultLocatorConfig        = gyro.DefaultLocatorConfig
	DefaultHealthCheckerConfig  = gyro.DefaultHealthCheckerConfig
	ValidateHealthCheckerConfig = health.ValidateHealthCheckerConfig
	ErrInvalidSnapshot          = gyro.ErrInvalidSnapshot
	ErrStaleRevision            = gyro.ErrStaleRevision
	ErrRevisionConflict         = gyro.ErrRevisionConflict
	ErrIncomparableRevision     = gyro.ErrIncomparableRevision
	ErrNilContext               = gyro.ErrNilContext
)

func NewTopologyStore() TopologyStore { return topology.NewStore() }

type healthAwarePool = health.HealthAwarePool

func newHealthAwarePool(locator Locator, checker HealthChecker) *healthAwarePool {
	return health.NewHealthAwarePoolWithChecker(locator, checker)
}

func cloneNodeInfo(node NodeInfo) NodeInfo {
	result := node
	result.Metadata = cloneStringMap(node.Metadata)
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func sortNodeInfosByID(nodes []NodeInfo) {
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
}

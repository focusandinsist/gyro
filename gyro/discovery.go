package gyro

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// NodeInfo contains information about a service node.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ServiceDiscovery provides the read-side service discovery capabilities used
// by Client. Registration is optional and is exposed separately through
// ServiceRegistrar.
type ServiceDiscovery interface {
	Discover(ctx context.Context, serviceName string) ([]NodeInfo, error)
	Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error)
}

// ServiceRegistrar provides optional service registration capabilities.
type ServiceRegistrar interface {
	Register(ctx context.Context, serviceName string, node NodeInfo) error
	Unregister(ctx context.Context, serviceName string, nodeID string) error
}

// StaticServiceDiscovery is a fixed, in-memory ServiceDiscovery backed by an
// address list, useful for tests or clusters that don't change at runtime.
type StaticServiceDiscovery struct {
	mu       sync.RWMutex
	services map[string][]NodeInfo
	watchers map[string][]chan []NodeInfo
}

// NewStaticServiceDiscovery creates a new static service discovery.
func NewStaticServiceDiscovery(addresses []string) *StaticServiceDiscovery {
	ssd := &StaticServiceDiscovery{services: make(map[string][]NodeInfo), watchers: make(map[string][]chan []NodeInfo)}
	if len(addresses) > 0 {
		ssd.services["default"] = nodeInfosFromAddresses(addresses)
	}
	return ssd
}

// UpdateNodes updates the node list for a service.
func (ssd *StaticServiceDiscovery) UpdateNodes(serviceName string, addresses []string) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	nodes := nodeInfosFromAddresses(addresses)
	ssd.services[serviceName] = nodes
	ssd.publishLocked(serviceName)
	if serviceName != "default" && len(ssd.services) == 1 {
		ssd.services["default"] = cloneNodeInfos(nodes)
		ssd.publishLocked("default")
	}
}

// Discover discovers available service nodes.
func (ssd *StaticServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]NodeInfo, error) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	nodes, exists := ssd.services[serviceName]
	if !exists {
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			result := cloneNodeInfos(defaultNodes)
			ssd.services[serviceName] = cloneNodeInfos(result)
			return result, nil
		}
		return []NodeInfo{}, nil
	}
	return cloneNodeInfos(nodes), nil
}

// Watch watches for changes in service nodes.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	ch := make(chan []NodeInfo, 1)
	ssd.mu.Lock()
	if ssd.watchers[serviceName] == nil {
		ssd.watchers[serviceName] = make([]chan []NodeInfo, 0)
	}
	ssd.watchers[serviceName] = append(ssd.watchers[serviceName], ch)
	nodes, exists := ssd.services[serviceName]
	if !exists {
		if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
			nodes = cloneNodeInfos(defaultNodes)
			ssd.services[serviceName] = nodes
		}
	}
	ch <- cloneNodeInfos(nodes)
	ssd.mu.Unlock()
	go func() {
		defer func() {
			ssd.mu.Lock()
			if watchers, exists := ssd.watchers[serviceName]; exists {
				for i, watcher := range watchers {
					if watcher == ch {
						ssd.watchers[serviceName] = append(watchers[:i], watchers[i+1:]...)
						close(ch)
						break
					}
				}
			}
			ssd.mu.Unlock()
		}()
		<-ctx.Done()
	}()
	return ch, nil
}

// Register registers a service node.
func (ssd *StaticServiceDiscovery) Register(ctx context.Context, serviceName string, node NodeInfo) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	if ssd.services[serviceName] == nil {
		ssd.services[serviceName] = make([]NodeInfo, 0)
	}
	for i, existing := range ssd.services[serviceName] {
		if existing.ID == node.ID {
			ssd.services[serviceName][i] = cloneNodeInfo(node)
			ssd.publishLocked(serviceName)
			return nil
		}
	}
	ssd.services[serviceName] = append(ssd.services[serviceName], cloneNodeInfo(node))
	ssd.publishLocked(serviceName)
	return nil
}

// Unregister unregisters a service node.
func (ssd *StaticServiceDiscovery) Unregister(ctx context.Context, serviceName string, nodeID string) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	nodes, exists := ssd.services[serviceName]
	if !exists {
		return fmt.Errorf("service %s not found", serviceName)
	}
	for i, node := range nodes {
		if node.ID == nodeID {
			nodes[i] = nodes[len(nodes)-1]
			ssd.services[serviceName] = nodes[:len(nodes)-1]
			ssd.publishLocked(serviceName)
			return nil
		}
	}
	return fmt.Errorf("node %s not found in service %s", nodeID, serviceName)
}

// SetNodes replaces all nodes for a service.
func (ssd *StaticServiceDiscovery) SetNodes(serviceName string, nodes []NodeInfo) {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	ssd.services[serviceName] = cloneNodeInfos(nodes)
	ssd.publishLocked(serviceName)
}

func (ssd *StaticServiceDiscovery) publishLocked(serviceName string) {
	for _, watcher := range ssd.watchers[serviceName] {
		snapshot := cloneNodeInfos(ssd.services[serviceName])
		select {
		case watcher <- snapshot:
		default:
			select {
			case <-watcher:
			default:
			}
			watcher <- snapshot
		}
	}
}

func cloneNodeInfos(nodes []NodeInfo) []NodeInfo {
	result := make([]NodeInfo, len(nodes))
	for i, node := range nodes {
		result[i] = cloneNodeInfo(node)
	}
	return result
}

func cloneNodeInfo(node NodeInfo) NodeInfo {
	result := node
	if node.Metadata != nil {
		result.Metadata = make(map[string]string, len(node.Metadata))
		for key, value := range node.Metadata {
			result.Metadata[key] = value
		}
	}
	return result
}

func nodeInfosFromAddresses(addresses []string) []NodeInfo {
	nodes := make([]NodeInfo, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		nodes = append(nodes, NodeInfo{ID: address, Address: address})
	}
	return nodes
}

func sortNodeInfosByID(nodes []NodeInfo) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
}

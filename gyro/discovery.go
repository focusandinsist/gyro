package gyro

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// NodeInfo is the transitional resource-factory view of a topology member.
// The topology APIs use Member and Endpoint; NodeInfo remains until the later
// resource-pool migration removes NodeFactory.
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// TopologyStream delivers complete snapshots and never deltas. Next is
// serialized by the caller; Close may run concurrently with Next.
type TopologyStream interface {
	Next(ctx context.Context) (TopologySnapshot, error)
	Close() error
}

// TopologySource is scoped to one service or discovery query.
type TopologySource interface {
	Snapshot(ctx context.Context) (TopologySnapshot, error)
	Watch(ctx context.Context) (TopologyStream, error)
}

// ServiceDiscovery provides service-scoped topology snapshots used by Client.
// Registration is optional and is exposed separately through ServiceRegistrar.
type ServiceDiscovery interface {
	Discover(ctx context.Context, serviceName string) (TopologySnapshot, error)
	Watch(ctx context.Context, serviceName string) (TopologyStream, error)
}

// NewServiceTopologySource scopes a service discovery implementation to one
// service and exposes the frozen TopologySource contract.
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

// ServiceRegistrar provides optional service registration capabilities.
type ServiceRegistrar interface {
	Register(ctx context.Context, serviceName string, node NodeInfo) error
	Unregister(ctx context.Context, serviceName string, nodeID string) error
}

// StaticServiceDiscovery is an in-memory complete-snapshot source useful for
// tests and fixed deployments. Each service has a stable source epoch and a
// local monotonic generation.
type StaticServiceDiscovery struct {
	mu          sync.RWMutex
	services    map[string][]NodeInfo
	generations map[string]uint64
	sources     map[string]string
	watchers    map[string][]*staticTopologyStream
	instanceID  uint64
}

var staticDiscoverySequence atomic.Uint64

// NewStaticServiceDiscovery creates a static source initialized with the
// default service. An empty address list still produces a valid empty snapshot.
func NewStaticServiceDiscovery(addresses []string) *StaticServiceDiscovery {
	ssd := &StaticServiceDiscovery{
		services:    make(map[string][]NodeInfo),
		generations: make(map[string]uint64),
		sources:     make(map[string]string),
		watchers:    make(map[string][]*staticTopologyStream),
		instanceID:  staticDiscoverySequence.Add(1),
	}
	ssd.services["default"] = nodeInfosFromAddresses(addresses)
	ssd.generations["default"] = 1
	ssd.sources["default"] = ssd.sourceLocked("default")
	return ssd
}

// UpdateNodes replaces a service's address list and publishes a new snapshot.
func (ssd *StaticServiceDiscovery) UpdateNodes(serviceName string, addresses []string) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	if serviceName == "" {
		return fmt.Errorf("service name cannot be empty")
	}
	hadOnlyDefault := serviceName != "default" && len(ssd.services) == 1
	ssd.ensureServiceLocked(serviceName)
	ssd.services[serviceName] = nodeInfosFromAddresses(addresses)
	ssd.bumpGenerationLocked(serviceName)
	ssd.publishLocked(serviceName)
	if hadOnlyDefault {
		ssd.services["default"] = cloneNodeInfos(ssd.services[serviceName])
		ssd.bumpGenerationLocked("default")
		ssd.publishLocked("default")
	}
	return nil
}

// Discover returns the current complete topology snapshot for a service.
func (ssd *StaticServiceDiscovery) Discover(ctx context.Context, serviceName string) (TopologySnapshot, error) {
	if err := contextErr(ctx); err != nil {
		return TopologySnapshot{}, err
	}
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	ssd.ensureServiceLocked(serviceName)
	return ssd.snapshotLocked(serviceName)
}

// Watch returns a stream whose first event is the current complete snapshot.
func (ssd *StaticServiceDiscovery) Watch(ctx context.Context, serviceName string) (TopologyStream, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	stream := &staticTopologyStream{
		owner:       ssd,
		serviceName: serviceName,
		updates:     make(chan TopologySnapshot, 1),
		done:        make(chan struct{}),
	}

	ssd.mu.Lock()
	ssd.ensureServiceLocked(serviceName)
	snapshot, err := ssd.snapshotLocked(serviceName)
	if err != nil {
		ssd.mu.Unlock()
		return nil, err
	}
	ssd.watchers[serviceName] = append(ssd.watchers[serviceName], stream)
	stream.updates <- snapshot
	ssd.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-stream.done:
		}
	}()
	return stream, nil
}

// Register registers or updates a service node.
func (ssd *StaticServiceDiscovery) Register(ctx context.Context, serviceName string, node NodeInfo) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	ssd.ensureServiceLocked(serviceName)
	if node.ID == "" || node.Address == "" {
		return ErrInvalidSnapshot
	}
	for i, existing := range ssd.services[serviceName] {
		if existing.ID == node.ID {
			ssd.services[serviceName][i] = cloneNodeInfo(node)
			ssd.bumpGenerationLocked(serviceName)
			ssd.publishLocked(serviceName)
			return nil
		}
	}
	ssd.services[serviceName] = append(ssd.services[serviceName], cloneNodeInfo(node))
	ssd.bumpGenerationLocked(serviceName)
	ssd.publishLocked(serviceName)
	return nil
}

// Unregister removes a service node.
func (ssd *StaticServiceDiscovery) Unregister(ctx context.Context, serviceName string, nodeID string) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	nodes, exists := ssd.services[serviceName]
	if !exists {
		return fmt.Errorf("service %s not found", serviceName)
	}
	for i, node := range nodes {
		if node.ID == nodeID {
			ssd.services[serviceName] = append(nodes[:i], nodes[i+1:]...)
			ssd.bumpGenerationLocked(serviceName)
			ssd.publishLocked(serviceName)
			return nil
		}
	}
	return fmt.Errorf("node %s not found in service %s", nodeID, serviceName)
}

// SetNodes replaces all nodes for a service.
func (ssd *StaticServiceDiscovery) SetNodes(serviceName string, nodes []NodeInfo) error {
	ssd.mu.Lock()
	defer ssd.mu.Unlock()
	if serviceName == "" {
		return fmt.Errorf("service name cannot be empty")
	}
	ssd.ensureServiceLocked(serviceName)
	candidate := topologySnapshotFromNodeInfos(ssd.sources[serviceName], ssd.generations[serviceName]+1, nodes)
	if _, err := normalizeTopologySnapshot(candidate); err != nil {
		return err
	}
	ssd.services[serviceName] = cloneNodeInfos(nodes)
	ssd.bumpGenerationLocked(serviceName)
	ssd.publishLocked(serviceName)
	return nil
}

func (ssd *StaticServiceDiscovery) publishLocked(serviceName string) {
	snapshot, err := ssd.snapshotLocked(serviceName)
	if err != nil {
		return
	}
	for _, watcher := range ssd.watchers[serviceName] {
		if watcher.closed {
			continue
		}
		select {
		case watcher.updates <- snapshot:
		default:
			select {
			case <-watcher.updates:
			default:
			}
			watcher.updates <- snapshot
		}
	}
}

func (ssd *StaticServiceDiscovery) ensureServiceLocked(serviceName string) {
	if _, exists := ssd.services[serviceName]; !exists {
		if serviceName != "default" {
			if defaultNodes, hasDefault := ssd.services["default"]; hasDefault {
				ssd.services[serviceName] = cloneNodeInfos(defaultNodes)
			} else {
				ssd.services[serviceName] = []NodeInfo{}
			}
		} else {
			ssd.services[serviceName] = []NodeInfo{}
		}
	}
	if ssd.generations[serviceName] == 0 {
		ssd.generations[serviceName] = 1
	}
	if ssd.sources[serviceName] == "" {
		ssd.sources[serviceName] = ssd.sourceLocked(serviceName)
	}
}

func (ssd *StaticServiceDiscovery) sourceLocked(serviceName string) string {
	return fmt.Sprintf("static-%d:%s", ssd.instanceID, serviceName)
}

func (ssd *StaticServiceDiscovery) bumpGenerationLocked(serviceName string) {
	ssd.generations[serviceName]++
}

func (ssd *StaticServiceDiscovery) snapshotLocked(serviceName string) (TopologySnapshot, error) {
	return normalizeTopologySnapshot(topologySnapshotFromNodeInfos(
		ssd.sources[serviceName],
		ssd.generations[serviceName],
		ssd.services[serviceName],
	))
}

type staticTopologyStream struct {
	owner       *StaticServiceDiscovery
	serviceName string
	updates     chan TopologySnapshot
	done        chan struct{}
	closed      bool
}

func (s *staticTopologyStream) Next(ctx context.Context) (TopologySnapshot, error) {
	if err := contextErr(ctx); err != nil {
		return TopologySnapshot{}, err
	}
	select {
	case <-s.done:
		return TopologySnapshot{}, io.EOF
	default:
	}
	select {
	case <-ctx.Done():
		return TopologySnapshot{}, ctx.Err()
	case <-s.done:
		return TopologySnapshot{}, io.EOF
	case snapshot, ok := <-s.updates:
		if !ok {
			return TopologySnapshot{}, io.EOF
		}
		select {
		case <-s.done:
			return TopologySnapshot{}, io.EOF
		default:
		}
		return cloneTopologySnapshot(snapshot), nil
	}
}

func (s *staticTopologyStream) Close() error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	watchers := s.owner.watchers[s.serviceName]
	for i, watcher := range watchers {
		if watcher == s {
			s.owner.watchers[s.serviceName] = append(watchers[:i], watchers[i+1:]...)
			break
		}
	}
	close(s.updates)
	close(s.done)
	return nil
}

func topologySnapshotFromNodeInfos(source string, generation uint64, nodes []NodeInfo) TopologySnapshot {
	members := make([]Member, len(nodes))
	for i, node := range nodes {
		members[i] = Member{
			ID:         node.ID,
			Endpoints:  []Endpoint{{Address: node.Address}},
			Attributes: cloneStringMap(node.Metadata),
		}
	}
	return TopologySnapshot{
		Revision: Revision{Source: source, Generation: generation, Token: fmt.Sprintf("%d", generation)},
		Members:  members,
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
	result.Metadata = cloneStringMap(node.Metadata)
	return result
}

func nodeInfosFromAddresses(addresses []string) []NodeInfo {
	nodes := make([]NodeInfo, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
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

package topology

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"gyro/gyro"
)

// StaticDiscovery is an in-memory complete-snapshot discovery source for
// tests, examples, and fixed deployments.
type StaticDiscovery struct {
	mu          sync.Mutex
	services    map[string][]gyro.NodeInfo
	generations map[string]uint64
	sources     map[string]string
	watchers    map[string]map[*staticStream]struct{}
	instanceID  uint64
}

var staticSequence atomic.Uint64

var _ gyro.ServiceDiscovery = (*StaticDiscovery)(nil)
var _ gyro.ServiceRegistrar = (*StaticDiscovery)(nil)

func NewStaticDiscovery(addresses []string) *StaticDiscovery {
	discovery := &StaticDiscovery{
		services:    make(map[string][]gyro.NodeInfo),
		generations: make(map[string]uint64),
		sources:     make(map[string]string),
		watchers:    make(map[string]map[*staticStream]struct{}),
		instanceID:  staticSequence.Add(1),
	}
	discovery.services["default"] = nodesFromAddresses(addresses)
	discovery.generations["default"] = 1
	discovery.sources["default"] = discovery.sourceLocked("default")
	return discovery
}

func (d *StaticDiscovery) Discover(ctx context.Context, serviceName string) (gyro.TopologySnapshot, error) {
	if err := contextErr(ctx); err != nil {
		return gyro.TopologySnapshot{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ensureServiceLocked(serviceName)
	return d.snapshotLocked(serviceName)
}

func (d *StaticDiscovery) Watch(ctx context.Context, serviceName string) (gyro.TopologyStream, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	stream := &staticStream{owner: d, serviceName: serviceName, updates: make(chan gyro.TopologySnapshot, 1), done: make(chan struct{})}
	d.mu.Lock()
	d.ensureServiceLocked(serviceName)
	snapshot, err := d.snapshotLocked(serviceName)
	if err == nil {
		if d.watchers[serviceName] == nil {
			d.watchers[serviceName] = make(map[*staticStream]struct{})
		}
		d.watchers[serviceName][stream] = struct{}{}
		stream.updates <- snapshot
	}
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-stream.done:
		}
	}()
	return stream, nil
}

func (d *StaticDiscovery) Register(ctx context.Context, serviceName string, node gyro.NodeInfo) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if node.ID == "" || node.Address == "" {
		return gyro.ErrInvalidSnapshot
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ensureServiceLocked(serviceName)
	nodes := d.services[serviceName]
	for i := range nodes {
		if nodes[i].ID == node.ID {
			nodes[i] = cloneNode(node)
			d.services[serviceName] = nodes
			d.publishLocked(serviceName)
			return nil
		}
	}
	d.services[serviceName] = append(nodes, cloneNode(node))
	d.publishLocked(serviceName)
	return nil
}

func (d *StaticDiscovery) Unregister(ctx context.Context, serviceName, nodeID string) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	nodes, exists := d.services[serviceName]
	if !exists {
		return fmt.Errorf("service %s not found", serviceName)
	}
	for i, node := range nodes {
		if node.ID == nodeID {
			d.services[serviceName] = append(nodes[:i], nodes[i+1:]...)
			d.publishLocked(serviceName)
			return nil
		}
	}
	return fmt.Errorf("node %s not found in service %s", nodeID, serviceName)
}

func (d *StaticDiscovery) SetNodes(serviceName string, nodes []gyro.NodeInfo) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if serviceName == "" {
		return fmt.Errorf("service name cannot be empty")
	}
	d.ensureServiceLocked(serviceName)
	candidate := snapshotFromNodes(d.sources[serviceName], d.generations[serviceName]+1, nodes)
	if _, err := normalize(candidate); err != nil {
		return err
	}
	d.services[serviceName] = cloneNodes(nodes)
	d.publishLocked(serviceName)
	return nil
}

func (d *StaticDiscovery) UpdateNodes(serviceName string, addresses []string) error {
	nodes := nodesFromAddresses(addresses)
	return d.SetNodes(serviceName, nodes)
}

func (d *StaticDiscovery) ensureServiceLocked(serviceName string) {
	if serviceName == "" {
		serviceName = "default"
	}
	if _, exists := d.services[serviceName]; !exists {
		d.services[serviceName] = cloneNodes(d.services["default"])
	}
	if d.generations[serviceName] == 0 {
		d.generations[serviceName] = 1
	}
	if d.sources[serviceName] == "" {
		d.sources[serviceName] = d.sourceLocked(serviceName)
	}
}

func (d *StaticDiscovery) sourceLocked(serviceName string) string {
	return fmt.Sprintf("static-%d:%s", d.instanceID, serviceName)
}

func (d *StaticDiscovery) snapshotLocked(serviceName string) (gyro.TopologySnapshot, error) {
	return normalize(snapshotFromNodes(d.sources[serviceName], d.generations[serviceName], d.services[serviceName]))
}

func (d *StaticDiscovery) publishLocked(serviceName string) {
	d.generations[serviceName]++
	snapshot, err := d.snapshotLocked(serviceName)
	if err != nil {
		return
	}
	for stream := range d.watchers[serviceName] {
		if stream.closed {
			continue
		}
		select {
		case stream.updates <- snapshot:
		default:
			select {
			case <-stream.updates:
			default:
			}
			stream.updates <- snapshot
		}
	}
}

type staticStream struct {
	owner       *StaticDiscovery
	serviceName string
	updates     chan gyro.TopologySnapshot
	done        chan struct{}
	closed      bool
}

func (s *staticStream) Next(ctx context.Context) (gyro.TopologySnapshot, error) {
	if err := contextErr(ctx); err != nil {
		return gyro.TopologySnapshot{}, err
	}
	select {
	case <-ctx.Done():
		return gyro.TopologySnapshot{}, ctx.Err()
	case <-s.done:
		return gyro.TopologySnapshot{}, io.EOF
	case snapshot, ok := <-s.updates:
		if !ok {
			return gyro.TopologySnapshot{}, io.EOF
		}
		select {
		case <-s.done:
			return gyro.TopologySnapshot{}, io.EOF
		default:
		}
		return cloneSnapshot(snapshot), nil
	}
}

func (s *staticStream) Close() error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	delete(s.owner.watchers[s.serviceName], s)
	close(s.done)
	close(s.updates)
	return nil
}

func snapshotFromNodes(source string, generation uint64, nodes []gyro.NodeInfo) gyro.TopologySnapshot {
	members := make([]gyro.Member, len(nodes))
	for i, node := range nodes {
		members[i] = gyro.Member{ID: node.ID, Endpoints: []gyro.Endpoint{{Address: node.Address}}, Attributes: cloneMap(node.Metadata)}
	}
	return gyro.TopologySnapshot{Revision: gyro.Revision{Source: source, Generation: generation, Token: fmt.Sprintf("%d", generation)}, Members: members}
}

func nodesFromAddresses(addresses []string) []gyro.NodeInfo {
	result := make([]gyro.NodeInfo, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, gyro.NodeInfo{ID: address, Address: address})
	}
	return result
}

func cloneNodes(nodes []gyro.NodeInfo) []gyro.NodeInfo {
	result := make([]gyro.NodeInfo, len(nodes))
	for i, node := range nodes {
		result[i] = cloneNode(node)
	}
	return result
}

func cloneNode(node gyro.NodeInfo) gyro.NodeInfo {
	node.Metadata = cloneMap(node.Metadata)
	return node
}

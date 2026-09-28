package gyro

import (
	"context"
	"io"
	"sync"
	"time"
)

// MockNode .
type MockNode struct {
	id      string
	address string
	healthy bool
	mu      sync.RWMutex

	checkCallCount int
}

// NewMockNode creates a new mock node
func NewMockNode(id, address string) *MockNode {
	return &MockNode{
		id:      id,
		address: address,
		healthy: true, // Start as healthy by default
	}
}

// ID returns the node ID
func (m *MockNode) ID() string {
	return m.id
}

// Address returns the node address
func (m *MockNode) Address() string {
	return m.address
}

// IsHealthy returns the current health status (controllable for testing)
func (m *MockNode) IsHealthy(ctx context.Context) bool {
	m.mu.Lock()
	m.checkCallCount++
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthy
}

// Close closes the mock node (for Node interface compliance)
func (m *MockNode) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = false
	return nil
}

// SetHealthy sets the health status for testing
func (m *MockNode) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

// GetCheckCallCount returns the number of times IsHealthy was called
func (m *MockNode) GetCheckCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkCallCount
}

type healthEventRecord struct {
	NodeID    string
	Healthy   bool
	Timestamp time.Time
}

// MockHealthListener is a test implementation that records health events.
type MockHealthListener struct {
	events []healthEventRecord
	mu     sync.RWMutex
	notify chan struct{}
}

// NewMockHealthListener creates a new mock health listener
func NewMockHealthListener() *MockHealthListener {
	return &MockHealthListener{
		events: make([]healthEventRecord, 0),
		notify: make(chan struct{}, 1),
	}
}

// AsHealthListener returns a HealthListener function that records events
func (m *MockHealthListener) AsHealthListener() HealthListener {
	return func(nodeID string, healthy bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.events = append(m.events, healthEventRecord{
			NodeID:    nodeID,
			Healthy:   healthy,
			Timestamp: time.Now(),
		})
		select {
		case m.notify <- struct{}{}:
		default:
		}
	}
}

// GetEvents returns all recorded events
func (m *MockHealthListener) GetEvents() []healthEventRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid race conditions
	events := make([]healthEventRecord, len(m.events))
	copy(events, m.events)
	return events
}

// GetEventCount returns the number of recorded events
func (m *MockHealthListener) GetEventCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.events)
}

// Clear clears all recorded events
func (m *MockHealthListener) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = m.events[:0]
}

// WaitForEvents waits for a specific number of events (for testing async behavior)
func (m *MockHealthListener) WaitForEvents(expectedCount int, timeout context.Context) bool {
	for {
		if m.GetEventCount() >= expectedCount {
			return true
		}
		select {
		case <-timeout.Done():
			return false
		case <-m.notify:
		}
	}
}

// MockServiceDiscovery is a test implementation of ServiceDiscovery
type MockServiceDiscovery struct {
	nodes []NodeInfo
	watch *mockTopologyStream
	mu    sync.RWMutex
}

// NewMockServiceDiscovery creates a new mock service discovery
func NewMockServiceDiscovery(initialNodes []NodeInfo) *MockServiceDiscovery {
	return &MockServiceDiscovery{
		nodes: cloneNodeInfos(initialNodes),
	}
}

// Discover returns the current complete topology snapshot.
func (m *MockServiceDiscovery) Discover(ctx context.Context, serviceName string) (TopologySnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return normalizeTopologySnapshot(topologySnapshotFromNodeInfos("mock-source", 1, m.nodes))
}

// Watch returns a complete snapshot stream.
func (m *MockServiceDiscovery) Watch(ctx context.Context, serviceName string) (TopologyStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := &mockTopologyStream{updates: make(chan TopologySnapshot, 10)}
	snapshot, err := normalizeTopologySnapshot(topologySnapshotFromNodeInfos("mock-source", 1, m.nodes))
	if err != nil {
		return nil, err
	}
	stream.updates <- snapshot
	m.watch = stream
	go func() {
		<-ctx.Done()
		_ = stream.Close()
	}()
	return stream, nil
}

type mockTopologyStream struct {
	mu      sync.Mutex
	updates chan TopologySnapshot
	closed  bool
}

func (s *mockTopologyStream) Next(ctx context.Context) (TopologySnapshot, error) {
	select {
	case <-ctx.Done():
		return TopologySnapshot{}, ctx.Err()
	case snapshot, ok := <-s.updates:
		if !ok {
			return TopologySnapshot{}, io.EOF
		}
		return cloneTopologySnapshot(snapshot), nil
	}
}

func (s *mockTopologyStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.updates)
	}
	return nil
}

// MockNodeFactory is a test implementation of NodeFactory
type MockNodeFactory struct {
	nodes map[string]*MockNode
	mu    sync.RWMutex
}

// NewMockNodeFactory creates a new mock node factory
func NewMockNodeFactory() *MockNodeFactory {
	return &MockNodeFactory{
		nodes: make(map[string]*MockNode),
	}
}

// CreateNode creates a mock node
func (m *MockNodeFactory) CreateNode(info NodeInfo) (Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node := NewMockNode(info.ID, info.Address)
	m.nodes[info.ID] = node
	return node, nil
}

// GetMockNode returns the mock node for testing
func (m *MockNodeFactory) GetMockNode(nodeID string) *MockNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[nodeID]
}

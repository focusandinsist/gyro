package gyro

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MockNode .
type MockNode struct {
	id      string
	address string
	healthy bool
	mu      sync.RWMutex

	// Test hooks
	checkCallCount int
	lastCheckError error
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

// CheckHealth performs a health check and returns an error if unhealthy (for HealthChecker testing)
func (m *MockNode) CheckHealth(ctx context.Context) error {
	m.mu.Lock()
	m.checkCallCount++
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.healthy {
		if m.lastCheckError != nil {
			return m.lastCheckError
		}
		return fmt.Errorf("mock node %s is unhealthy", m.id)
	}
	return nil
}

// SetHealthy sets the health status for testing
func (m *MockNode) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

// SetCheckError sets the error to return on health checks
func (m *MockNode) SetCheckError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastCheckError = err
}

// GetCheckCallCount returns the number of times IsHealthy was called
func (m *MockNode) GetCheckCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkCallCount
}

// ResetCheckCallCount resets the check call counter
func (m *MockNode) ResetCheckCallCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkCallCount = 0
}

// MockHealthListener is a test implementation that records health events
type MockHealthListener struct {
	events []HealthEvent
	mu     sync.RWMutex
}

// NewMockHealthListener creates a new mock health listener
func NewMockHealthListener() *MockHealthListener {
	return &MockHealthListener{
		events: make([]HealthEvent, 0),
	}
}

// AsHealthListener returns a HealthListener function that records events
func (m *MockHealthListener) AsHealthListener() HealthListener {
	return func(nodeID string, healthy bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.events = append(m.events, HealthEvent{
			NodeID:    nodeID,
			Healthy:   healthy,
			Timestamp: time.Now(),
		})
	}
}

// GetEvents returns all recorded events
func (m *MockHealthListener) GetEvents() []HealthEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid race conditions
	events := make([]HealthEvent, len(m.events))
	copy(events, m.events)
	return events
}

// GetEventCount returns the number of recorded events
func (m *MockHealthListener) GetEventCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.events)
}

// GetLastEvent returns the last recorded event
func (m *MockHealthListener) GetLastEvent() *HealthEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.events) == 0 {
		return nil
	}

	event := m.events[len(m.events)-1]
	return &event
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
		select {
		case <-timeout.Done():
			return false
		default:
			if m.GetEventCount() >= expectedCount {
				return true
			}
			// Small sleep to avoid busy waiting
			select {
			case <-timeout.Done():
				return false
			default:
				continue
			}
		}
	}
}

// MockServiceDiscovery is a test implementation of ServiceDiscovery
type MockServiceDiscovery struct {
	nodes   []NodeInfo
	watchCh chan []NodeInfo
	mu      sync.RWMutex
}

// NewMockServiceDiscovery creates a new mock service discovery
func NewMockServiceDiscovery(initialNodes []NodeInfo) *MockServiceDiscovery {
	return &MockServiceDiscovery{
		nodes:   initialNodes,
		watchCh: make(chan []NodeInfo, 10),
	}
}

// Discover returns the current list of nodes
func (m *MockServiceDiscovery) Discover(ctx context.Context, serviceName string) ([]NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid race conditions
	nodes := make([]NodeInfo, len(m.nodes))
	copy(nodes, m.nodes)
	return nodes, nil
}

// Watch returns a channel for node changes
func (m *MockServiceDiscovery) Watch(ctx context.Context, serviceName string) (<-chan []NodeInfo, error) {
	return m.watchCh, nil
}

// UpdateNodes updates the node list and notifies watchers
func (m *MockServiceDiscovery) UpdateNodes(nodes []NodeInfo) {
	m.mu.Lock()
	m.nodes = make([]NodeInfo, len(nodes))
	copy(m.nodes, nodes)
	m.mu.Unlock()

	// Notify watchers
	select {
	case m.watchCh <- nodes:
	default:
		// Channel is full, skip notification
	}
}

// AddNode adds a node and notifies watchers
func (m *MockServiceDiscovery) AddNode(node NodeInfo) {
	m.mu.Lock()
	m.nodes = append(m.nodes, node)
	nodesCopy := make([]NodeInfo, len(m.nodes))
	copy(nodesCopy, m.nodes)
	m.mu.Unlock()

	// Notify watchers
	select {
	case m.watchCh <- nodesCopy:
	default:
		// Channel is full, skip notification
	}
}

// RemoveNode removes a node and notifies watchers
func (m *MockServiceDiscovery) RemoveNode(nodeID string) {
	m.mu.Lock()
	for i, node := range m.nodes {
		if node.ID == nodeID {
			m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
			break
		}
	}
	nodesCopy := make([]NodeInfo, len(m.nodes))
	copy(nodesCopy, m.nodes)
	m.mu.Unlock()

	// Notify watchers
	select {
	case m.watchCh <- nodesCopy:
	default:
		// Channel is full, skip notification
	}
}

// Close closes the watch channel
func (m *MockServiceDiscovery) Close() {
	close(m.watchCh)
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

// SetNodeHealthy sets the health status of a specific node
func (m *MockNodeFactory) SetNodeHealthy(nodeID string, healthy bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if node, exists := m.nodes[nodeID]; exists {
		node.SetHealthy(healthy)
	}
}

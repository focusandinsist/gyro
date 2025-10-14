package gossip

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Node represents a node that can be health checked
type Node interface {
	ID() string
	IsHealthy(ctx context.Context) bool
}

// HealthListener is called when a node's health status changes
type HealthListener func(nodeID string, healthy bool)

// GossipConfig configures the Gossip health checker
type GossipConfig struct {
	Enabled        bool                `json:"enabled"`
	ProbingRatio   float64             `json:"probing_ratio"`   // Ratio of nodes each client probes (0.1 = 10%)
	GossipInterval time.Duration       `json:"gossip_interval"` // Gossip communication interval
	PeerDiscovery  PeerDiscoveryConfig `json:"peer_discovery"`  // Peer discovery configuration
	Transport      TransportConfig     `json:"transport"`       // Transport layer configuration
	MaxPeers       int                 `json:"max_peers"`       // Maximum number of peers
	MessageTTL     time.Duration       `json:"message_ttl"`     // Message time-to-live
}

// NodeStatus represents the health status of a node
type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusUp
	NodeStatusDown
)

func (ns NodeStatus) String() string {
	switch ns {
	case NodeStatusUp:
		return "UP"
	case NodeStatusDown:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

// NodeHealthState represents the health state of a node
type NodeHealthState struct {
	NodeID    string        `json:"node_id"`
	Status    NodeStatus    `json:"status"`
	Timestamp time.Time     `json:"timestamp"`
	Version   int64         `json:"version"`
	Source    string        `json:"source"` // Information source (which client probed it)
	TTL       time.Duration `json:"ttl"`    // Information time-to-live
}

// IsExpired checks if the state has expired
func (nhs *NodeHealthState) IsExpired() bool {
	return time.Since(nhs.Timestamp) > nhs.TTL
}

// IsNewerThan checks if this state is newer than another state
func (nhs *NodeHealthState) IsNewerThan(other *NodeHealthState) bool {
	if nhs.Version != other.Version {
		return nhs.Version > other.Version
	}
	return nhs.Timestamp.After(other.Timestamp)
}

// GossipMessage represents a Gossip message
type GossipMessage struct {
	SenderID   string             `json:"sender_id"`
	MessageID  string             `json:"message_id"`
	Timestamp  time.Time          `json:"timestamp"`
	NodeStates []*NodeHealthState `json:"node_states"`
	PeerStates []*PeerInfo        `json:"peer_states,omitempty"` // Optional: propagate peer information
}

// PeerInfo represents peer information
type PeerInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`  // Gossip communication address
	Metadata map[string]string `json:"metadata"` // Additional information
	LastSeen time.Time         `json:"last_seen"`
	Version  int64             `json:"version"`
}

// PeerDiscovery provides peer discovery capabilities
type PeerDiscovery interface {
	// DiscoverPeers discovers currently available peers
	DiscoverPeers(ctx context.Context) ([]PeerInfo, error)

	// WatchPeers watches for peer changes
	WatchPeers(ctx context.Context) (<-chan []PeerInfo, error)

	// Register registers self as a discoverable peer
	Register(ctx context.Context, self PeerInfo) error

	// Unregister unregisters self
	Unregister(ctx context.Context, peerID string) error
}

// PeerDiscoveryConfig configures peer discovery
type PeerDiscoveryConfig struct {
	Type   string                 `json:"type"`   // "static", "consul", "kubernetes"
	Config map[string]interface{} `json:"config"` // Specific configuration
}

// GossipTransport provides Gossip transport layer interface
type GossipTransport interface {
	// Start starts transport layer listening
	Start(ctx context.Context, bindAddr string) error

	// Send sends message to specified peer
	Send(ctx context.Context, peer PeerInfo, msg *GossipMessage) error

	// Receive returns the channel for receiving messages
	Receive() <-chan *ReceivedMessage

	// Stop stops the transport layer
	Stop() error

	// LocalAddr returns the local listening address
	LocalAddr() string
}

// ReceivedMessage represents a received message
type ReceivedMessage struct {
	From    PeerInfo
	Message *GossipMessage
	ReplyTo func(*GossipMessage) error // Optional reply function
}

// TransportConfig configures the transport layer
type TransportConfig struct {
	Type     string                 `json:"type"`      // "udp", "http"
	BindAddr string                 `json:"bind_addr"` // Listening address
	Config   map[string]interface{} `json:"config"`    // Specific configuration
}

// GossipStats contains Gossip statistics
type GossipStats struct {
	MessagesSent     atomic.Int64
	MessagesReceived atomic.Int64
	SendErrors       atomic.Int64
	ReceiveErrors    atomic.Int64
	PeersDiscovered  atomic.Int64
	NodesProbed      atomic.Int64
	StateUpdates     atomic.Int64
}

// GossipHealthChecker is a Gossip protocol-based health checker
type GossipHealthChecker struct {
	// Configuration
	config GossipConfig

	// Components
	peerDiscovery PeerDiscovery
	transport     GossipTransport
	nodeProber    *NodeProber

	// State management
	localState *sync.Map // map[nodeID]*NodeHealthState
	knownPeers *sync.Map // map[peerID]*PeerInfo

	// Self information
	selfPeerID string
	selfPeer   PeerInfo

	// Control
	stopCh  chan struct{}
	running atomic.Bool

	// Statistics
	stats *GossipStats

	// Listeners
	healthListeners []HealthListener
	mu              sync.RWMutex
}

// NewGossipHealthChecker creates a new Gossip health checker
func NewGossipHealthChecker(config GossipConfig) (*GossipHealthChecker, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("gossip health checker is not enabled")
	}

	// Generate unique Peer ID
	peerID, err := generatePeerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate peer ID: %w", err)
	}

	// Create components
	peerDiscovery, err := createPeerDiscovery(config.PeerDiscovery)
	if err != nil {
		return nil, fmt.Errorf("failed to create peer discovery: %w", err)
	}

	transport, err := createGossipTransport(config.Transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossip transport: %w", err)
	}

	nodeProber := NewNodeProber(NodeProberConfig{
		Timeout:           5 * time.Second,
		MaxConcurrentJobs: 10,
	})

	ghc := &GossipHealthChecker{
		config:        config,
		peerDiscovery: peerDiscovery,
		transport:     transport,
		nodeProber:    nodeProber,
		localState:    &sync.Map{},
		knownPeers:    &sync.Map{},
		selfPeerID:    peerID,
		stopCh:        make(chan struct{}),
		stats:         &GossipStats{},
	}

	return ghc, nil
}

// generatePeerID generates a unique Peer ID
func generatePeerID() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// createPeerDiscovery creates a peer discovery instance based on configuration
func createPeerDiscovery(config PeerDiscoveryConfig) (PeerDiscovery, error) {
	switch config.Type {
	case "static":
		return NewStaticPeerDiscovery(), nil
	case "consul":
		// TODO: implement Consul peer discovery
		return nil, fmt.Errorf("consul peer discovery not implemented yet")
	case "kubernetes":
		// TODO: implement Kubernetes peer discovery
		return nil, fmt.Errorf("kubernetes peer discovery not implemented yet")
	default:
		return nil, fmt.Errorf("unknown peer discovery type: %s", config.Type)
	}
}

// createGossipTransport creates a transport layer instance based on configuration
func createGossipTransport(config TransportConfig) (GossipTransport, error) {
	switch config.Type {
	case "udp":
		return NewUDPGossipTransport(), nil
	case "http":
		// TODO: implement HTTP transport layer
		return nil, fmt.Errorf("http gossip transport not implemented yet")
	default:
		return nil, fmt.Errorf("unknown gossip transport type: %s", config.Type)
	}
}

// Start starts the Gossip health checker
func (ghc *GossipHealthChecker) Start(ctx context.Context) error {
	if ghc.running.Load() {
		return fmt.Errorf("gossip health checker is already running")
	}

	// 1. Start transport layer
	if err := ghc.transport.Start(ctx, ghc.config.Transport.BindAddr); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// 2. Set self information
	ghc.selfPeer = PeerInfo{
		ID:       ghc.selfPeerID,
		Address:  ghc.transport.LocalAddr(),
		LastSeen: time.Now(),
		Version:  1,
	}

	// 3. Register self to peer discovery
	if err := ghc.peerDiscovery.Register(ctx, ghc.selfPeer); err != nil {
		return fmt.Errorf("failed to register self peer: %w", err)
	}

	// 4. Start worker loops
	go ghc.probingLoop(ctx)   // Active probing loop
	go ghc.gossipLoop(ctx)    // Gossip communication loop
	go ghc.peerWatchLoop(ctx) // Peer watching loop
	go ghc.messageLoop(ctx)   // Message processing loop

	ghc.running.Store(true)
	return nil
}

// Stop stops the Gossip health checker
func (ghc *GossipHealthChecker) Stop() error {
	if !ghc.running.Load() {
		return nil
	}

	ghc.running.Store(false)
	close(ghc.stopCh)

	// Unregister self
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ghc.peerDiscovery.Unregister(ctx, ghc.selfPeerID)

	// Stop transport layer
	return ghc.transport.Stop()
}

// AddNode adds a node to monitor
func (ghc *GossipHealthChecker) AddNode(node Node) {
	state := &NodeHealthState{
		NodeID:    node.ID(),
		Status:    NodeStatusUnknown,
		Timestamp: time.Now(),
		Version:   1,
		Source:    ghc.selfPeerID,
		TTL:       ghc.config.MessageTTL,
	}
	ghc.localState.Store(node.ID(), state)
}

// RemoveNode removes a node from monitoring
func (ghc *GossipHealthChecker) RemoveNode(nodeID string) {
	ghc.localState.Delete(nodeID)
}

// IsNodeHealthy checks if a node is healthy
func (ghc *GossipHealthChecker) IsNodeHealthy(nodeID string) bool {
	if state, ok := ghc.localState.Load(nodeID); ok {
		healthState := state.(*NodeHealthState)
		if healthState.IsExpired() {
			return false // Expired information is considered unhealthy
		}
		return healthState.Status == NodeStatusUp
	}
	return true // Unknown nodes are considered healthy by default
}

// AddHealthListener adds a health status change listener
func (ghc *GossipHealthChecker) AddHealthListener(listener HealthListener) {
	ghc.mu.Lock()
	defer ghc.mu.Unlock()
	ghc.healthListeners = append(ghc.healthListeners, listener)
}

// GetStats returns statistics information
func (ghc *GossipHealthChecker) GetStats() GossipStats {
	return GossipStats{
		MessagesSent:     atomic.Int64{},
		MessagesReceived: atomic.Int64{},
		SendErrors:       atomic.Int64{},
		ReceiveErrors:    atomic.Int64{},
		PeersDiscovered:  atomic.Int64{},
		NodesProbed:      atomic.Int64{},
		StateUpdates:     atomic.Int64{},
	}
}

// selectNodesToProbe selects nodes to probe based on probing ratio
func (ghc *GossipHealthChecker) selectNodesToProbe(allNodes []Node, ratio float64) []Node {
	if ratio >= 1.0 {
		return allNodes
	}

	count := int(math.Ceil(float64(len(allNodes)) * ratio))
	if count == 0 && len(allNodes) > 0 {
		count = 1 // Probe at least one node
	}

	// TODO: Implement smart selection algorithm (consider last probe time, node importance, etc.)
	// Simple random selection implementation here
	if count >= len(allNodes) {
		return allNodes
	}

	selected := make([]Node, count)
	copy(selected, allNodes[:count])
	return selected
}

// probingLoop is the active probing loop
func (ghc *GossipHealthChecker) probingLoop(ctx context.Context) {
	ticker := time.NewTicker(ghc.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ghc.stopCh:
			return
		case <-ticker.C:
			ghc.performProbing(ctx)
		}
	}
}

// performProbing performs the probing
func (ghc *GossipHealthChecker) performProbing(ctx context.Context) {
	// 1. Get all nodes that need monitoring
	allNodes := ghc.getAllMonitoredNodes()
	if len(allNodes) == 0 {
		return
	}

	// 2. Select nodes to probe based on ProbingRatio
	nodesToProbe := ghc.selectNodesToProbe(allNodes, ghc.config.ProbingRatio)

	// 3. Concurrently probe selected nodes
	var wg sync.WaitGroup
	for _, node := range nodesToProbe {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			ghc.probeNode(ctx, n)
		}(node)
	}
	wg.Wait()
}

// getAllMonitoredNodes gets all monitored nodes
func (ghc *GossipHealthChecker) getAllMonitoredNodes() []Node {
	// TODO: Need to get node list from external source
	// Return empty list for now, actual implementation needs integration with Locator
	return []Node{}
}

// probeNode probes a single node
func (ghc *GossipHealthChecker) probeNode(ctx context.Context, node Node) {
	healthy := node.IsHealthy(ctx)
	status := NodeStatusDown
	if healthy {
		status = NodeStatusUp
	}

	// Update local state
	ghc.updateNodeState(node.ID(), status, ghc.selfPeerID)
	ghc.stats.NodesProbed.Add(1)
}

// updateNodeState updates node state
func (ghc *GossipHealthChecker) updateNodeState(nodeID string, status NodeStatus, source string) {
	now := time.Now()

	// Get current state
	var currentState *NodeHealthState
	if state, ok := ghc.localState.Load(nodeID); ok {
		currentState = state.(*NodeHealthState)
	}

	// Create new state
	newState := &NodeHealthState{
		NodeID:    nodeID,
		Status:    status,
		Timestamp: now,
		Source:    source,
		TTL:       ghc.config.MessageTTL,
	}

	// Set version number
	if currentState != nil {
		if currentState.Status != status {
			newState.Version = currentState.Version + 1 // Increment version when status changes
		} else {
			newState.Version = currentState.Version // Keep version when status unchanged
		}
	} else {
		newState.Version = 1 // New node starts from version 1
	}

	// Store new state
	ghc.localState.Store(nodeID, newState)
	ghc.stats.StateUpdates.Add(1)

	// Notify listeners if status changed
	if currentState == nil || currentState.Status != status {
		ghc.notifyHealthChange(nodeID, status == NodeStatusUp)
	}
}

// notifyHealthChange notifies health status changes
func (ghc *GossipHealthChecker) notifyHealthChange(nodeID string, healthy bool) {
	ghc.mu.RLock()
	listeners := make([]HealthListener, len(ghc.healthListeners))
	copy(listeners, ghc.healthListeners)
	ghc.mu.RUnlock()

	for _, listener := range listeners {
		go listener(nodeID, healthy)
	}
}

// gossipLoop is the Gossip communication loop
func (ghc *GossipHealthChecker) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(ghc.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ghc.stopCh:
			return
		case <-ticker.C:
			ghc.performGossip(ctx)
		}
	}
}

// performGossip performs Gossip communication
func (ghc *GossipHealthChecker) performGossip(ctx context.Context) {
	// 1. Select random peers
	peers := ghc.selectRandomPeers(3) // Communicate with 3 peers each time
	if len(peers) == 0 {
		return
	}

	// 2. Prepare state information to send
	msg := ghc.prepareGossipMessage()

	// 3. Send concurrently to selected peers
	for _, peer := range peers {
		go func(p PeerInfo) {
			if err := ghc.transport.Send(ctx, p, msg); err != nil {
				ghc.stats.SendErrors.Add(1)
			} else {
				ghc.stats.MessagesSent.Add(1)
			}
		}(peer)
	}
}

// selectRandomPeers selects random peers
func (ghc *GossipHealthChecker) selectRandomPeers(count int) []PeerInfo {
	var peers []PeerInfo
	ghc.knownPeers.Range(func(key, value interface{}) bool {
		peer := value.(PeerInfo)
		if peer.ID != ghc.selfPeerID { // Exclude self
			peers = append(peers, peer)
		}
		return true
	})

	// Simple random selection (should use better random algorithm in practice)
	if len(peers) <= count {
		return peers
	}

	selected := make([]PeerInfo, count)
	copy(selected, peers[:count])
	return selected
}

// prepareGossipMessage prepares a Gossip message
func (ghc *GossipHealthChecker) prepareGossipMessage() *GossipMessage {
	var nodeStates []*NodeHealthState

	// Collect all local states
	ghc.localState.Range(func(key, value interface{}) bool {
		state := value.(*NodeHealthState)
		if !state.IsExpired() { // Only send non-expired states
			nodeStates = append(nodeStates, state)
		}
		return true
	})

	// Generate message ID
	msgID, _ := generatePeerID()

	return &GossipMessage{
		SenderID:   ghc.selfPeerID,
		MessageID:  msgID,
		Timestamp:  time.Now(),
		NodeStates: nodeStates,
	}
}

// peerWatchLoop is the peer watching loop
func (ghc *GossipHealthChecker) peerWatchLoop(ctx context.Context) {
	peersCh, err := ghc.peerDiscovery.WatchPeers(ctx)
	if err != nil {
		fmt.Printf("Failed to watch peers: %v\n", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ghc.stopCh:
			return
		case peers, ok := <-peersCh:
			if !ok {
				return
			}
			ghc.updateKnownPeers(peers)
		}
	}
}

// updateKnownPeers updates the known peers list
func (ghc *GossipHealthChecker) updateKnownPeers(peers []PeerInfo) {
	// Clean up expired peers
	ghc.knownPeers.Range(func(key, value interface{}) bool {
		peer := value.(PeerInfo)
		found := false
		for _, p := range peers {
			if p.ID == peer.ID {
				found = true
				break
			}
		}
		if !found {
			ghc.knownPeers.Delete(key)
		}
		return true
	})

	// Add new peers
	for _, peer := range peers {
		if peer.ID != ghc.selfPeerID { // Exclude self
			ghc.knownPeers.Store(peer.ID, peer)
		}
	}

	ghc.stats.PeersDiscovered.Store(int64(len(peers)))
}

// messageLoop is the message processing loop
func (ghc *GossipHealthChecker) messageLoop(ctx context.Context) {
	recvCh := ghc.transport.Receive()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ghc.stopCh:
			return
		case receivedMsg, ok := <-recvCh:
			if !ok {
				return
			}
			ghc.handleReceivedMessage(receivedMsg)
		}
	}
}

// handleReceivedMessage handles received messages
func (ghc *GossipHealthChecker) handleReceivedMessage(receivedMsg *ReceivedMessage) {
	ghc.stats.MessagesReceived.Add(1)

	msg := receivedMsg.Message
	if msg == nil {
		ghc.stats.ReceiveErrors.Add(1)
		return
	}

	// Ignore messages sent by self
	if msg.SenderID == ghc.selfPeerID {
		return
	}

	// Handle node state updates
	for _, nodeState := range msg.NodeStates {
		ghc.mergeNodeState(nodeState)
	}

	// Handle peer state updates (if any)
	if len(msg.PeerStates) > 0 {
		peerSlice := make([]PeerInfo, len(msg.PeerStates))
		for i, peer := range msg.PeerStates {
			peerSlice[i] = *peer
		}
		ghc.updateKnownPeers(peerSlice)
	}
}

// mergeNodeState merges node state (conflict resolution)
func (ghc *GossipHealthChecker) mergeNodeState(incomingState *NodeHealthState) {
	if incomingState.IsExpired() {
		return // Ignore expired state
	}

	// Get current local state
	currentValue, exists := ghc.localState.Load(incomingState.NodeID)
	if !exists {
		// New node, accept directly
		ghc.localState.Store(incomingState.NodeID, incomingState)
		ghc.stats.StateUpdates.Add(1)
		ghc.notifyHealthChange(incomingState.NodeID, incomingState.Status == NodeStatusUp)
		return
	}

	currentState := currentValue.(*NodeHealthState)

	// Conflict resolution: trust whoever has newer message
	if incomingState.IsNewerThan(currentState) {
		oldStatus := currentState.Status
		ghc.localState.Store(incomingState.NodeID, incomingState)
		ghc.stats.StateUpdates.Add(1)

		// Notify listeners if status changed
		if oldStatus != incomingState.Status {
			ghc.notifyHealthChange(incomingState.NodeID, incomingState.Status == NodeStatusUp)
		}
	}
}

// Check HealthChecker interface
func (ghc *GossipHealthChecker) Check(ctx context.Context, node Node) error {
	// In Gossip mode, Check method is mainly used for manual probe triggering
	ghc.probeNode(ctx, node)
	return nil
}

// StartMonitoring HealthChecker interface
func (ghc *GossipHealthChecker) StartMonitoring(ctx context.Context) {
	ghc.Start(ctx)
}

// StopMonitoring HealthChecker interface
func (ghc *GossipHealthChecker) StopMonitoring() {
	ghc.Stop()
}

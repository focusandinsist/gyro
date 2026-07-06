package gossip

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// discardLogger is the default logger: silent until SetLogger is called.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

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
	ProbingRatio   float64             `json:"probing_ratio"` // fraction of nodes each client probes, e.g. 0.1 = 10%
	GossipInterval time.Duration       `json:"gossip_interval"`
	PeerDiscovery  PeerDiscoveryConfig `json:"peer_discovery"`
	Transport      TransportConfig     `json:"transport"`
	MaxPeers       int                 `json:"max_peers"`
	MessageTTL     time.Duration       `json:"message_ttl"`
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
	Source    string        `json:"source"` // ID of the peer that produced this state
	TTL       time.Duration `json:"ttl"`
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
	PeerStates []*PeerInfo        `json:"peer_states,omitempty"` // optional peer-list propagation
}

// PeerInfo represents peer information
type PeerInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"` // gossip transport address, not the node's service address
	Metadata map[string]string `json:"metadata"`
	LastSeen time.Time         `json:"last_seen"`
	Version  int64             `json:"version"`
}

// PeerDiscovery provides peer discovery capabilities
type PeerDiscovery interface {
	DiscoverPeers(ctx context.Context) ([]PeerInfo, error)
	WatchPeers(ctx context.Context) (<-chan []PeerInfo, error)
	Register(ctx context.Context, self PeerInfo) error
	Unregister(ctx context.Context, peerID string) error
}

// PeerDiscoveryConfig configures peer discovery
type PeerDiscoveryConfig struct {
	Type   string                 `json:"type"` // "static", "consul", "kubernetes"
	Config map[string]interface{} `json:"config"`
}

// GossipTransport provides Gossip transport layer interface
type GossipTransport interface {
	Start(ctx context.Context, bindAddr string) error
	Send(ctx context.Context, peer PeerInfo, msg *GossipMessage) error
	Receive() <-chan *ReceivedMessage
	Stop() error
	LocalAddr() string
}

// ReceivedMessage represents a received message
type ReceivedMessage struct {
	From    PeerInfo
	Message *GossipMessage
	ReplyTo func(*GossipMessage) error // optional
}

// TransportConfig configures the transport layer
type TransportConfig struct {
	Type     string                 `json:"type"` // "udp", "http"
	BindAddr string                 `json:"bind_addr"`
	Config   map[string]interface{} `json:"config"`
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
	config GossipConfig

	peerDiscovery PeerDiscovery
	transport     GossipTransport
	nodeProber    *NodeProber

	localState *sync.Map // map[nodeID]*NodeHealthState
	knownPeers *sync.Map // map[peerID]*PeerInfo

	selfPeerID string
	selfPeer   PeerInfo

	stopCh  chan struct{}
	running atomic.Bool

	stats *GossipStats

	healthListeners []HealthListener
	mu              sync.RWMutex
	logger          atomic.Pointer[slog.Logger]
}

// NewGossipHealthChecker creates a new Gossip health checker
func NewGossipHealthChecker(config GossipConfig) (*GossipHealthChecker, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("gossip health checker is not enabled")
	}

	peerID, err := generatePeerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate peer ID: %w", err)
	}

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
	ghc.logger.Store(discardLogger)

	return ghc, nil
}

// SetLogger overrides the logger used for internal diagnostics. Passing nil
// restores the default no-op logger.
func (ghc *GossipHealthChecker) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardLogger
	}
	ghc.logger.Store(logger)
}

func (ghc *GossipHealthChecker) log() *slog.Logger {
	return ghc.logger.Load()
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

	if err := ghc.transport.Start(ctx, ghc.config.Transport.BindAddr); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	ghc.selfPeer = PeerInfo{
		ID:       ghc.selfPeerID,
		Address:  ghc.transport.LocalAddr(),
		LastSeen: time.Now(),
		Version:  1,
	}

	if err := ghc.peerDiscovery.Register(ctx, ghc.selfPeer); err != nil {
		return fmt.Errorf("failed to register self peer: %w", err)
	}

	go ghc.probingLoop(ctx)
	go ghc.gossipLoop(ctx)
	go ghc.peerWatchLoop(ctx)
	go ghc.messageLoop(ctx)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ghc.peerDiscovery.Unregister(ctx, ghc.selfPeerID)

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
			return false // stale information is treated as unhealthy
		}
		return healthState.Status == NodeStatusUp
	}
	return true // no data yet, assume healthy
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
		count = 1
	}

	// TODO: smarter selection (last probe time, node importance, etc.) instead of a fixed prefix.
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
	allNodes := ghc.getAllMonitoredNodes()
	if len(allNodes) == 0 {
		return
	}

	nodesToProbe := ghc.selectNodesToProbe(allNodes, ghc.config.ProbingRatio)

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
	// TODO: this needs to source the node list from the Locator; returns
	// empty until that integration exists, so probing is currently a no-op.
	return []Node{}
}

// probeNode probes a single node
func (ghc *GossipHealthChecker) probeNode(ctx context.Context, node Node) {
	healthy := node.IsHealthy(ctx)
	status := NodeStatusDown
	if healthy {
		status = NodeStatusUp
	}

	ghc.updateNodeState(node.ID(), status, ghc.selfPeerID)
	ghc.stats.NodesProbed.Add(1)
}

// updateNodeState updates node state
func (ghc *GossipHealthChecker) updateNodeState(nodeID string, status NodeStatus, source string) {
	now := time.Now()

	var currentState *NodeHealthState
	if state, ok := ghc.localState.Load(nodeID); ok {
		currentState = state.(*NodeHealthState)
	}

	newState := &NodeHealthState{
		NodeID:    nodeID,
		Status:    status,
		Timestamp: now,
		Source:    source,
		TTL:       ghc.config.MessageTTL,
	}

	if currentState != nil {
		if currentState.Status != status {
			newState.Version = currentState.Version + 1 // bump version only on a real status change
		} else {
			newState.Version = currentState.Version
		}
	} else {
		newState.Version = 1
	}

	ghc.localState.Store(nodeID, newState)
	ghc.stats.StateUpdates.Add(1)

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
	peers := ghc.selectRandomPeers(3)
	if len(peers) == 0 {
		return
	}

	msg := ghc.prepareGossipMessage()

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
		if peer.ID != ghc.selfPeerID {
			peers = append(peers, peer)
		}
		return true
	})

	// Not truly random - just takes the first `count`. Good enough for now.
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

	ghc.localState.Range(func(key, value interface{}) bool {
		state := value.(*NodeHealthState)
		if !state.IsExpired() {
			nodeStates = append(nodeStates, state)
		}
		return true
	})

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
		ghc.log().Error("failed to watch peers", "error", err)
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
	// Drop peers no longer present in the latest discovery result.
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

	for _, peer := range peers {
		if peer.ID != ghc.selfPeerID {
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

	if msg.SenderID == ghc.selfPeerID {
		return
	}

	for _, nodeState := range msg.NodeStates {
		ghc.mergeNodeState(nodeState)
	}

	if len(msg.PeerStates) > 0 {
		peerSlice := make([]PeerInfo, len(msg.PeerStates))
		for i, peer := range msg.PeerStates {
			peerSlice[i] = *peer
		}
		ghc.updateKnownPeers(peerSlice)
	}
}

// mergeNodeState merges an incoming node state into local state, resolving
// conflicts by trusting whichever state is newer (by version, then timestamp).
func (ghc *GossipHealthChecker) mergeNodeState(incomingState *NodeHealthState) {
	if incomingState.IsExpired() {
		return
	}

	currentValue, exists := ghc.localState.Load(incomingState.NodeID)
	if !exists {
		ghc.localState.Store(incomingState.NodeID, incomingState)
		ghc.stats.StateUpdates.Add(1)
		ghc.notifyHealthChange(incomingState.NodeID, incomingState.Status == NodeStatusUp)
		return
	}

	currentState := currentValue.(*NodeHealthState)

	if incomingState.IsNewerThan(currentState) {
		oldStatus := currentState.Status
		ghc.localState.Store(incomingState.NodeID, incomingState)
		ghc.stats.StateUpdates.Add(1)

		if oldStatus != incomingState.Status {
			ghc.notifyHealthChange(incomingState.NodeID, incomingState.Status == NodeStatusUp)
		}
	}
}

// Check implements HealthChecker. In Gossip mode this triggers an immediate
// manual probe rather than waiting for the next probing-loop tick.
func (ghc *GossipHealthChecker) Check(ctx context.Context, node Node) error {
	ghc.probeNode(ctx, node)
	return nil
}

// StartMonitoring implements HealthChecker.
func (ghc *GossipHealthChecker) StartMonitoring(ctx context.Context) {
	ghc.Start(ctx)
}

// StopMonitoring implements HealthChecker.
func (ghc *GossipHealthChecker) StopMonitoring() {
	ghc.Stop()
}

package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// NodeProber is responsible for probing node health status
type NodeProber struct {
	config   NodeProberConfig
	jobCh    chan *ProbeJob
	resultCh chan *ProbeResult
	stopCh   chan struct{}
	running  bool
	mu       sync.RWMutex
}

// NodeProberConfig configures the node prober
type NodeProberConfig struct {
	Timeout           time.Duration
	MaxConcurrentJobs int
}

// ProbeJob represents a probing task
type ProbeJob struct {
	Node   Node
	Ctx    context.Context
	Result chan *ProbeResult
}

// ProbeResult represents a probing result
type ProbeResult struct {
	NodeID  string
	Healthy bool
	Error   error
	Latency time.Duration
}

// NewNodeProber creates a new node prober
func NewNodeProber(config NodeProberConfig) *NodeProber {
	return &NodeProber{
		config:   config,
		jobCh:    make(chan *ProbeJob, config.MaxConcurrentJobs*2),
		resultCh: make(chan *ProbeResult, config.MaxConcurrentJobs*2),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the prober
func (np *NodeProber) Start() {
	np.mu.Lock()
	defer np.mu.Unlock()

	if np.running {
		return
	}

	np.running = true

	// Start worker goroutines
	for i := 0; i < np.config.MaxConcurrentJobs; i++ {
		go np.worker()
	}
}

// Stop stops the prober
func (np *NodeProber) Stop() {
	np.mu.Lock()
	defer np.mu.Unlock()

	if !np.running {
		return
	}

	np.running = false
	close(np.stopCh)
}

// ProbeAsync probes a node asynchronously
func (np *NodeProber) ProbeAsync(ctx context.Context, node Node) <-chan *ProbeResult {
	resultCh := make(chan *ProbeResult, 1)

	job := &ProbeJob{
		Node:   node,
		Ctx:    ctx,
		Result: resultCh,
	}

	select {
	case np.jobCh <- job:
		// Task submitted
	default:
		// Queue full, return error directly
		go func() {
			resultCh <- &ProbeResult{
				NodeID:  node.ID(),
				Healthy: false,
				Error:   fmt.Errorf("probe queue is full"),
			}
		}()
	}

	return resultCh
}

// worker is the worker goroutine
func (np *NodeProber) worker() {
	for {
		select {
		case <-np.stopCh:
			return
		case job := <-np.jobCh:
			np.executeProbe(job)
		}
	}
}

// executeProbe executes the probing
func (np *NodeProber) executeProbe(job *ProbeJob) {
	start := time.Now()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(job.Ctx, np.config.Timeout)
	defer cancel()

	// Execute health check
	healthy := job.Node.IsHealthy(ctx)
	latency := time.Since(start)

	result := &ProbeResult{
		NodeID:  job.Node.ID(),
		Healthy: healthy,
		Latency: latency,
	}

	// Send result
	select {
	case job.Result <- result:
	case <-time.After(time.Second):
		// Timeout, abandon sending result
	}
}

// StaticPeerDiscovery static peer discovery
type StaticPeerDiscovery struct {
	mu    sync.RWMutex
	peers []PeerInfo
}

// NewStaticPeerDiscovery creates a static peer discovery
func NewStaticPeerDiscovery() *StaticPeerDiscovery {
	return &StaticPeerDiscovery{
		peers: make([]PeerInfo, 0),
	}
}

// SetPeers sets the peer list
func (spd *StaticPeerDiscovery) SetPeers(peers []PeerInfo) {
	spd.mu.Lock()
	defer spd.mu.Unlock()
	spd.peers = make([]PeerInfo, len(peers))
	copy(spd.peers, peers)
}

// DiscoverPeers discovers peers
func (spd *StaticPeerDiscovery) DiscoverPeers(ctx context.Context) ([]PeerInfo, error) {
	spd.mu.RLock()
	defer spd.mu.RUnlock()

	result := make([]PeerInfo, len(spd.peers))
	copy(result, spd.peers)
	return result, nil
}

// WatchPeers watches for peer changes
func (spd *StaticPeerDiscovery) WatchPeers(ctx context.Context) (<-chan []PeerInfo, error) {
	ch := make(chan []PeerInfo, 1)

	go func() {
		defer close(ch)

		// Send initial peer list
		peers, _ := spd.DiscoverPeers(ctx)
		select {
		case ch <- peers:
		case <-ctx.Done():
			return
		}

		// Static discovery has no changes, just wait for context cancellation
		<-ctx.Done()
	}()

	return ch, nil
}

// Register registers a peer
func (spd *StaticPeerDiscovery) Register(ctx context.Context, self PeerInfo) error {
	// Static discovery does not support dynamic registration
	return nil
}

// Unregister unregisters a peer
func (spd *StaticPeerDiscovery) Unregister(ctx context.Context, peerID string) error {
	// Static discovery does not support dynamic unregistration
	return nil
}

// UDPGossipTransport UDP transport layer
type UDPGossipTransport struct {
	conn    *net.UDPConn
	recvCh  chan *ReceivedMessage
	stopCh  chan struct{}
	running bool
	mu      sync.RWMutex
}

// NewUDPGossipTransport creates a UDP transport layer
func NewUDPGossipTransport() *UDPGossipTransport {
	return &UDPGossipTransport{
		recvCh: make(chan *ReceivedMessage, 100),
		stopCh: make(chan struct{}),
	}
}

// Start starts the transport layer
func (ugt *UDPGossipTransport) Start(ctx context.Context, bindAddr string) error {
	ugt.mu.Lock()
	defer ugt.mu.Unlock()

	if ugt.running {
		return fmt.Errorf("UDP transport is already running")
	}

	// Resolve address
	addr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	// Listen on UDP port
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen UDP: %w", err)
	}

	ugt.conn = conn
	ugt.running = true

	// Start receive goroutine
	go ugt.receiveLoop()

	return nil
}

// Stop stops the transport layer
func (ugt *UDPGossipTransport) Stop() error {
	ugt.mu.Lock()
	defer ugt.mu.Unlock()

	if !ugt.running {
		return nil
	}

	ugt.running = false
	close(ugt.stopCh)

	if ugt.conn != nil {
		return ugt.conn.Close()
	}

	return nil
}

// Send sends a message
func (ugt *UDPGossipTransport) Send(ctx context.Context, peer PeerInfo, msg *GossipMessage) error {
	ugt.mu.RLock()
	conn := ugt.conn
	running := ugt.running
	ugt.mu.RUnlock()

	if !running || conn == nil {
		return fmt.Errorf("UDP transport is not running")
	}

	// Serialize message
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Resolve target address
	addr, err := net.ResolveUDPAddr("udp", peer.Address)
	if err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}

	// Send data
	_, err = conn.WriteToUDP(data, addr)
	if err != nil {
		return fmt.Errorf("failed to send UDP message: %w", err)
	}

	return nil
}

// Receive returns the message receiving channel
func (ugt *UDPGossipTransport) Receive() <-chan *ReceivedMessage {
	return ugt.recvCh
}

// LocalAddr returns the local address
func (ugt *UDPGossipTransport) LocalAddr() string {
	ugt.mu.RLock()
	defer ugt.mu.RUnlock()

	if ugt.conn != nil {
		return ugt.conn.LocalAddr().String()
	}
	return ""
}

// receiveLoop is the receive loop
func (ugt *UDPGossipTransport) receiveLoop() {
	buffer := make([]byte, 64*1024) // 64KB buffer

	for {
		select {
		case <-ugt.stopCh:
			return
		default:
			// Set read timeout
			ugt.conn.SetReadDeadline(time.Now().Add(time.Second))

			n, addr, err := ugt.conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Timeout, continue loop
				}
				return // Other error, exit
			}

			// Parse message
			var msg GossipMessage
			if err := json.Unmarshal(buffer[:n], &msg); err != nil {
				continue // Parse failed, ignore message
			}

			// Create received message
			receivedMsg := &ReceivedMessage{
				From: PeerInfo{
					Address: addr.String(),
				},
				Message: &msg,
			}

			// Send to receive channel
			select {
			case ugt.recvCh <- receivedMsg:
			default:
				// Channel full, drop message
			}
		}
	}
}

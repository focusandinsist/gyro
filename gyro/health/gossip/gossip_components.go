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
	default:
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

	ctx, cancel := context.WithTimeout(job.Ctx, np.config.Timeout)
	defer cancel()

	healthy := job.Node.IsHealthy(ctx)
	latency := time.Since(start)

	result := &ProbeResult{
		NodeID:  job.Node.ID(),
		Healthy: healthy,
		Latency: latency,
	}

	select {
	case job.Result <- result:
	case <-time.After(time.Second):
		// Caller stopped waiting; drop the result rather than block forever.
	}
}

// StaticPeerDiscovery is a fixed, in-memory PeerDiscovery.
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

		peers, _ := spd.DiscoverPeers(ctx)
		select {
		case ch <- peers:
		case <-ctx.Done():
			return
		}

		// The peer set is fixed, so there's nothing further to watch for.
		<-ctx.Done()
	}()

	return ch, nil
}

// Register registers a peer
func (spd *StaticPeerDiscovery) Register(ctx context.Context, self PeerInfo) error {
	return nil // static discovery has a fixed peer set; nothing to register
}

// Unregister unregisters a peer
func (spd *StaticPeerDiscovery) Unregister(ctx context.Context, peerID string) error {
	return nil // static discovery has a fixed peer set; nothing to unregister
}

// UDPGossipTransport is a GossipTransport implementation over UDP.
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

	addr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen UDP: %w", err)
	}

	ugt.conn = conn
	ugt.running = true

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

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	addr, err := net.ResolveUDPAddr("udp", peer.Address)
	if err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}

	if _, err := conn.WriteToUDP(data, addr); err != nil {
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
	buffer := make([]byte, 64*1024)

	for {
		select {
		case <-ugt.stopCh:
			return
		default:
			ugt.conn.SetReadDeadline(time.Now().Add(time.Second))

			n, addr, err := ugt.conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return
			}

			var msg GossipMessage
			if err := json.Unmarshal(buffer[:n], &msg); err != nil {
				continue
			}

			receivedMsg := &ReceivedMessage{
				From:    PeerInfo{Address: addr.String()},
				Message: &msg,
			}

			select {
			case ugt.recvCh <- receivedMsg:
			default:
				// Receiver isn't keeping up; drop rather than block the read loop.
			}
		}
	}
}

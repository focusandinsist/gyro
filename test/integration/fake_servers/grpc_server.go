// Package fake_servers provides controllable mock servers for integration testing.
package fake_servers

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// FakeGRPCServer is a controllable gRPC-like server for testing.
// It uses simple TCP instead of real gRPC to avoid external dependencies.
type FakeGRPCServer struct {
	port     int
	listener net.Listener

	// Control state
	mu        sync.RWMutex
	healthy   bool
	responses map[string]string // key -> response mapping

	// Statistics
	requestCount int64
	lastRequest  time.Time

	// Control channels
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

// NewFakeGRPCServer creates a new controllable gRPC server.
func NewFakeGRPCServer(port int) *FakeGRPCServer {
	return &FakeGRPCServer{
		port:       port,
		healthy:    true,
		responses:  make(map[string]string),
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Start starts the fake gRPC server.
func (s *FakeGRPCServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	s.listener = listener

	go func() {
		defer close(s.doneCh)
		log.Printf("FakeGRPCServer starting on port %d", s.port)

		for {
			select {
			case <-s.shutdownCh:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-s.shutdownCh:
						return
					default:
						log.Printf("FakeGRPCServer accept error: %v", err)
						continue
					}
				}

				go s.handleConnection(conn)
			}
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Stop stops the fake gRPC server.
func (s *FakeGRPCServer) Stop() {
	close(s.shutdownCh)
	if s.listener != nil {
		s.listener.Close()
	}
	<-s.doneCh
	log.Printf("FakeGRPCServer on port %d stopped", s.port)
}

// SetHealthy controls the server's health status.
func (s *FakeGRPCServer) SetHealthy(healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = healthy
	log.Printf("FakeGRPCServer on port %d health set to: %v", s.port, healthy)
}

// IsHealthy returns the current health status.
func (s *FakeGRPCServer) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// SetResponse sets a predefined response for a key.
func (s *FakeGRPCServer) SetResponse(key, response string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses[key] = response
}

// GetStats returns server statistics.
func (s *FakeGRPCServer) GetStats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]any{
		"port":          s.port,
		"healthy":       s.healthy,
		"request_count": s.requestCount,
		"last_request":  s.lastRequest,
		"responses":     len(s.responses),
	}
}

// handleConnection handles a single connection.
func (s *FakeGRPCServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	s.mu.Lock()
	s.requestCount++
	s.lastRequest = time.Now()
	healthy := s.healthy
	s.mu.Unlock()

	if !healthy {
		conn.Write([]byte("ERROR: server is unhealthy\n"))
		return
	}

	// Simple protocol: read a line, respond with a line
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	request := string(buffer[:n])
	response := fmt.Sprintf("response_from_port_%d_for_%s", s.port, request)

	log.Printf("FakeGRPCServer on port %d handled request: %s", s.port, request)
	conn.Write([]byte(response + "\n"))
}

// simulateRequest simulates handling a request for testing.
func (s *FakeGRPCServer) simulateRequest(key string) (string, error) {
	s.mu.Lock()
	s.requestCount++
	s.lastRequest = time.Now()
	healthy := s.healthy
	s.mu.Unlock()

	if !healthy {
		return "", fmt.Errorf("server is unhealthy")
	}

	s.mu.RLock()
	response, exists := s.responses[key]
	s.mu.RUnlock()

	if !exists {
		response = fmt.Sprintf("response_from_port_%d_for_%s", s.port, key)
	}

	log.Printf("FakeGRPCServer on port %d handled request for key '%s'", s.port, key)
	return response, nil
}

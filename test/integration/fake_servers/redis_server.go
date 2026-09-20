// Package fake_servers provides controllable mock servers for integration testing.
package fake_servers

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// FakeRedisServer is a controllable Redis server for testing.
type FakeRedisServer struct {
	port     int
	listener net.Listener

	// Control state
	mu      sync.RWMutex
	healthy bool
	data    map[string]string // key -> value storage

	// Statistics
	requestCount int64
	lastRequest  time.Time

	// Control channels
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

// NewFakeRedisServer creates a new controllable Redis server.
func NewFakeRedisServer(port int) *FakeRedisServer {
	return &FakeRedisServer{
		port:       port,
		healthy:    true,
		data:       make(map[string]string),
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

// Start starts the Redis server.
func (s *FakeRedisServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	s.listener = listener

	go func() {
		defer close(s.doneCh)
		log.Printf("FakeRedisServer starting on port %d", s.port)

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
						log.Printf("FakeRedisServer accept error: %v", err)
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

// Stop stops the Redis server.
func (s *FakeRedisServer) Stop() {
	close(s.shutdownCh)
	if s.listener != nil {
		s.listener.Close()
	}
	<-s.doneCh
	log.Printf("FakeRedisServer on port %d stopped", s.port)
}

// SetHealthy controls the server's health status.
func (s *FakeRedisServer) SetHealthy(healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = healthy
	log.Printf("FakeRedisServer on port %d health set to: %v", s.port, healthy)
}

// IsHealthy returns the current health status.
func (s *FakeRedisServer) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// SetData sets a key-value pair in the server's data store.
func (s *FakeRedisServer) SetData(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// GetStats returns server statistics.
func (s *FakeRedisServer) GetStats() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]any{
		"port":          s.port,
		"healthy":       s.healthy,
		"request_count": s.requestCount,
		"last_request":  s.lastRequest,
		"data_keys":     len(s.data),
	}
}

// handleConnection handles a single Redis connection.
func (s *FakeRedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		response := s.handleCommand(line)
		if response != "" {
			conn.Write([]byte(response + "\r\n"))
		}
	}
}

// handleCommand processes a Redis command and returns a response.
func (s *FakeRedisServer) handleCommand(command string) string {
	s.mu.Lock()
	s.requestCount++
	s.lastRequest = time.Now()
	healthy := s.healthy
	s.mu.Unlock()

	if !healthy {
		return "-ERR server is unhealthy"
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "-ERR empty command"
	}

	cmd := strings.ToUpper(parts[0])
	log.Printf("FakeRedisServer on port %d handling command: %s", s.port, cmd)

	switch cmd {
	case "PING":
		return "+PONG"

	case "GET":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments for 'get' command"
		}
		key := parts[1]

		s.mu.RLock()
		value, exists := s.data[key]
		s.mu.RUnlock()

		if !exists {
			return "$-1" // Redis null bulk string
		}
		return fmt.Sprintf("$%d\r\n%s", len(value), value)

	case "SET":
		if len(parts) != 3 {
			return "-ERR wrong number of arguments for 'set' command"
		}
		key, value := parts[1], parts[2]

		s.mu.Lock()
		s.data[key] = value
		s.mu.Unlock()

		return "+OK"

	case "EXISTS":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments for 'exists' command"
		}
		key := parts[1]

		s.mu.RLock()
		_, exists := s.data[key]
		s.mu.RUnlock()

		if exists {
			return ":1"
		}
		return ":0"

	case "DEL":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments for 'del' command"
		}
		key := parts[1]

		s.mu.Lock()
		_, existed := s.data[key]
		delete(s.data, key)
		s.mu.Unlock()

		if existed {
			return ":1"
		}
		return ":0"

	case "INFO":
		info := fmt.Sprintf("# Server\r\nport:%d\r\nhealthy:%v\r\nrequests:%d\r\n",
			s.port, s.IsHealthy(), s.requestCount)
		return fmt.Sprintf("$%d\r\n%s", len(info), info)

	default:
		return fmt.Sprintf("-ERR unknown command '%s'", cmd)
	}
}

// Helper method to format Redis bulk string response
func formatBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s", len(s), s)
}

// Helper method to format Redis integer response
func formatInteger(i int64) string {
	return fmt.Sprintf(":%d", i)
}

// Helper method to format Redis simple string response
func formatSimpleString(s string) string {
	return fmt.Sprintf("+%s", s)
}

// Helper method to format Redis error response
func formatError(err string) string {
	return fmt.Sprintf("-ERR %s", err)
}

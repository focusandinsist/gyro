// Package fake_servers provides controllable mock servers for integration testing.
package fake_servers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
)

// ControlAPI provides HTTP endpoints to control fake servers during testing.
type ControlAPI struct {
	port   int
	server *http.Server

	// Server registries
	mu           sync.RWMutex
	grpcServers  map[int]*FakeGRPCServer
	redisServers map[int]*FakeRedisServer
}

// NewControlAPI creates a new control API server.
func NewControlAPI(port int) *ControlAPI {
	return &ControlAPI{
		port:         port,
		grpcServers:  make(map[int]*FakeGRPCServer),
		redisServers: make(map[int]*FakeRedisServer),
	}
}

// RegisterGRPCServer registers a gRPC server for control.
func (c *ControlAPI) RegisterGRPCServer(port int, server *FakeGRPCServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.grpcServers[port] = server
}

// RegisterRedisServer registers a Redis server for control.
func (c *ControlAPI) RegisterRedisServer(port int, server *FakeRedisServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.redisServers[port] = server
}

// Start starts the control API server.
func (c *ControlAPI) Start() error {
	mux := http.NewServeMux()

	// Health control endpoints
	mux.HandleFunc("/grpc/health", c.handleGRPCHealth)
	mux.HandleFunc("/redis/health", c.handleRedisHealth)

	// Statistics endpoints
	mux.HandleFunc("/grpc/stats", c.handleGRPCStats)
	mux.HandleFunc("/redis/stats", c.handleRedisStats)

	// Data manipulation endpoints
	mux.HandleFunc("/grpc/response", c.handleGRPCResponse)
	mux.HandleFunc("/redis/data", c.handleRedisData)

	// Cluster overview
	mux.HandleFunc("/cluster/status", c.handleClusterStatus)

	c.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", c.port),
		Handler: mux,
	}

	go func() {
		log.Printf("ControlAPI starting on port %d", c.port)
		if err := c.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("ControlAPI error: %v", err)
		}
	}()

	return nil
}

// Stop stops the control API server.
func (c *ControlAPI) Stop() error {
	if c.server != nil {
		return c.server.Close()
	}
	return nil
}

// handleGRPCHealth handles gRPC server health control.
// POST /grpc/health?port=8001&healthy=true
func (c *ControlAPI) handleGRPCHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	healthyStr := r.URL.Query().Get("healthy")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	healthy, err := strconv.ParseBool(healthyStr)
	if err != nil {
		http.Error(w, "Invalid healthy value", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.grpcServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "gRPC server not found", http.StatusNotFound)
		return
	}

	server.SetHealthy(healthy)

	response := map[string]any{
		"port":    port,
		"healthy": healthy,
		"type":    "grpc",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleRedisHealth handles Redis server health control.
// POST /redis/health?port=6379&healthy=false
func (c *ControlAPI) handleRedisHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	healthyStr := r.URL.Query().Get("healthy")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	healthy, err := strconv.ParseBool(healthyStr)
	if err != nil {
		http.Error(w, "Invalid healthy value", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.redisServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "Redis server not found", http.StatusNotFound)
		return
	}

	server.SetHealthy(healthy)

	response := map[string]any{
		"port":    port,
		"healthy": healthy,
		"type":    "redis",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGRPCStats returns gRPC server statistics.
// GET /grpc/stats?port=8001
func (c *ControlAPI) handleGRPCStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	if portStr == "" {
		// Return stats for all gRPC servers
		c.mu.RLock()
		allStats := make(map[string]any)
		for port, server := range c.grpcServers {
			allStats[fmt.Sprintf("grpc_%d", port)] = server.GetStats()
		}
		c.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(allStats)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.grpcServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "gRPC server not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(server.GetStats())
}

// handleRedisStats returns Redis server statistics.
// GET /redis/stats?port=6379
func (c *ControlAPI) handleRedisStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	if portStr == "" {
		// Return stats for all Redis servers
		c.mu.RLock()
		allStats := make(map[string]any)
		for port, server := range c.redisServers {
			allStats[fmt.Sprintf("redis_%d", port)] = server.GetStats()
		}
		c.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(allStats)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.redisServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "Redis server not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(server.GetStats())
}

// handleGRPCResponse sets predefined responses for gRPC servers.
// POST /grpc/response?port=8001&key=user123&value=response_data
func (c *ControlAPI) handleGRPCResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.grpcServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "gRPC server not found", http.StatusNotFound)
		return
	}

	server.SetResponse(key, value)

	response := map[string]any{
		"port":  port,
		"key":   key,
		"value": value,
		"type":  "grpc",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleRedisData sets data in Redis servers.
// POST /redis/data?port=6379&key=user123&value=data
func (c *ControlAPI) handleRedisData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	portStr := r.URL.Query().Get("port")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	port, err := strconv.Atoi(portStr)
	if err != nil {
		http.Error(w, "Invalid port", http.StatusBadRequest)
		return
	}

	c.mu.RLock()
	server, exists := c.redisServers[port]
	c.mu.RUnlock()

	if !exists {
		http.Error(w, "Redis server not found", http.StatusNotFound)
		return
	}

	server.SetData(key, value)

	response := map[string]any{
		"port":  port,
		"key":   key,
		"value": value,
		"type":  "redis",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleClusterStatus returns overall cluster status.
// GET /cluster/status
func (c *ControlAPI) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	status := map[string]any{
		"grpc_servers":  make([]map[string]any, 0),
		"redis_servers": make([]map[string]any, 0),
	}

	for port, server := range c.grpcServers {
		stats := server.GetStats()
		stats["port"] = port
		stats["type"] = "grpc"
		status["grpc_servers"] = append(status["grpc_servers"].([]map[string]any), stats)
	}

	for port, server := range c.redisServers {
		stats := server.GetStats()
		stats["port"] = port
		stats["type"] = "redis"
		status["redis_servers"] = append(status["redis_servers"].([]map[string]any), stats)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

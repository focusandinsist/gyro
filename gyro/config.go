package gyro

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ConfigManager manages configuration updates.
type ConfigManager struct {
	mu       sync.RWMutex
	config   *ClientConfig
	watchers []ConfigWatcher
}

// ClientConfig the configuration for Gyro clients.
// This is a composition of individual component configurations.
type ClientConfig struct {
	// Locator configures the connection locator behavior.
	Locator LocatorConfig `json:"locator"`

	// HealthChecker configures health checking behavior.
	HealthChecker HealthCheckerConfig `json:"health_checker"`

	// Connection configures connection-specific behavior.
	Connection ConnectionConfig `json:"connection"`
}

// ConnectionConfig configures connection-specific behavior.
type ConnectionConfig struct {
	// MaxIdleConns is the maximum number of idle connections per node.
	MaxIdleConns int `json:"max_idle_conns"`

	// MaxActiveConns is the maximum number of active connections per node.
	MaxActiveConns int `json:"max_active_conns"`

	// IdleTimeout is the timeout for idle connections.
	IdleTimeout time.Duration `json:"idle_timeout"`

	// ConnectTimeout is the timeout for establishing connections.
	ConnectTimeout time.Duration `json:"connect_timeout"`

	// ReadTimeout is the timeout for read operations.
	ReadTimeout time.Duration `json:"read_timeout"`

	// WriteTimeout is the timeout for write operations.
	WriteTimeout time.Duration `json:"write_timeout"`
}

// DefaultConnectionConfig .
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		MaxIdleConns:   10,
		MaxActiveConns: 100,
		IdleTimeout:    5 * time.Minute,
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

// DefaultClientConfig .
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Locator:       DefaultLocatorConfig(),
		HealthChecker: DefaultHealthCheckerConfig(),
		Connection:    DefaultConnectionConfig(),
	}
}

// ConfigWatcher is called when configuration changes.
type ConfigWatcher func(oldConfig, newConfig *ClientConfig) error

// NewConfigManager creates a new configuration manager.
func NewConfigManager(config *ClientConfig) *ConfigManager {
	if config == nil {
		config = DefaultClientConfig()
	}

	return &ConfigManager{
		config:   config,
		watchers: make([]ConfigWatcher, 0),
	}
}

// GetConfig returns the current configuration.
func (cm *ConfigManager) GetConfig() *ClientConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a deep copy
	configJSON, _ := json.Marshal(cm.config)
	var configCopy ClientConfig
	json.Unmarshal(configJSON, &configCopy)
	return &configCopy
}

// UpdateConfig updates the configuration and notifies watchers.
func (cm *ConfigManager) UpdateConfig(newConfig *ClientConfig) error {
	if newConfig == nil {
		return fmt.Errorf("new config cannot be nil")
	}

	cm.mu.Lock()
	oldConfig := cm.config
	cm.config = newConfig
	watchers := make([]ConfigWatcher, len(cm.watchers))
	copy(watchers, cm.watchers)
	cm.mu.Unlock()

	// Notify all watchers
	for _, watcher := range watchers {
		if err := watcher(oldConfig, newConfig); err != nil {
			// TODO:handle this gracefully??
			return fmt.Errorf("config watcher failed: %w", err)
		}
	}

	return nil
}

// AddConfigWatcher adds a configuration change watcher.
func (cm *ConfigManager) AddConfigWatcher(watcher ConfigWatcher) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.watchers = append(cm.watchers, watcher)
}

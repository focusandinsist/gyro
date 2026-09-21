package gyro

import (
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

// ClientConfig is the configuration for Gyro clients, composed of the
// individual component configs below.
type ClientConfig struct {
	Locator       LocatorConfig       `json:"locator"`
	HealthChecker HealthCheckerConfig `json:"health_checker"`
	Connection    ConnectionConfig    `json:"connection"`
}

// ConnectionConfig configures connection-specific behavior.
type ConnectionConfig struct {
	MaxIdleConns   int           `json:"max_idle_conns"`
	MaxActiveConns int           `json:"max_active_conns"`
	IdleTimeout    time.Duration `json:"idle_timeout"`
	ConnectTimeout time.Duration `json:"connect_timeout"`
	ReadTimeout    time.Duration `json:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`
}

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
		config:   cloneClientConfig(config),
		watchers: make([]ConfigWatcher, 0),
	}
}

// GetConfig returns the current configuration.
func (cm *ConfigManager) GetConfig() *ClientConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cloneClientConfig(cm.config)
}

// UpdateConfig updates the configuration and notifies watchers.
func (cm *ConfigManager) UpdateConfig(newConfig *ClientConfig) error {
	if newConfig == nil {
		return fmt.Errorf("new config cannot be nil")
	}

	cm.mu.RLock()
	oldConfig := cloneClientConfig(cm.config)
	watchers := make([]ConfigWatcher, len(cm.watchers))
	copy(watchers, cm.watchers)
	cm.mu.RUnlock()

	for _, watcher := range watchers {
		if err := watcher(cloneClientConfig(oldConfig), cloneClientConfig(newConfig)); err != nil {
			return fmt.Errorf("config watcher failed: %w", err)
		}
	}

	cm.mu.Lock()
	cm.config = cloneClientConfig(newConfig)
	cm.mu.Unlock()

	return nil
}

func cloneClientConfig(config *ClientConfig) *ClientConfig {
	if config == nil {
		return nil
	}
	copy := *config
	return &copy
}

// AddConfigWatcher adds a configuration change watcher.
func (cm *ConfigManager) AddConfigWatcher(watcher ConfigWatcher) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.watchers = append(cm.watchers, watcher)
}

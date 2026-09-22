package gyro

import (
	"fmt"
	"sync"
	"time"
)

// Config is the configuration for Gyro clients, composed of the
// individual component configs below.
type Config struct {
	Locator       LocatorConfig       `json:"locator"`
	HealthChecker HealthCheckerConfig `json:"health_checker"`
	Connection    ConnectionConfig    `json:"connection"`
}

// ConfigManager manages configuration updates.
type ConfigManager struct {
	mu       sync.RWMutex
	updateMu sync.Mutex
	config   *Config
	watchers []ConfigWatcher
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

// DefaultConnectionConfig returns connection defaults suitable for adapters.
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

// DefaultConfig returns a complete configuration with the default component settings.
func DefaultConfig() *Config {
	return &Config{
		Locator:       DefaultLocatorConfig(),
		HealthChecker: DefaultHealthCheckerConfig(),
		Connection:    DefaultConnectionConfig(),
	}
}

// ConfigWatcher is called when configuration changes.
type ConfigWatcher func(oldConfig, newConfig *Config) error

// NewConfigManager creates a new configuration manager.
func NewConfigManager(config *Config) *ConfigManager {
	if config == nil {
		config = DefaultConfig()
	}

	return &ConfigManager{
		config:   cloneConfig(config),
		watchers: make([]ConfigWatcher, 0),
	}
}

// GetConfig returns the current configuration.
func (cm *ConfigManager) GetConfig() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cloneConfig(cm.config)
}

// UpdateConfig updates the configuration and notifies watchers.
func (cm *ConfigManager) UpdateConfig(newConfig *Config) error {
	if newConfig == nil {
		return fmt.Errorf("new config cannot be nil")
	}

	// Watchers prepare external resources, so they cannot run under cm.mu.
	// Serialize the full prepare/commit transaction separately to prevent two
	// updates from validating against the same old snapshot and committing out
	// of order.
	cm.updateMu.Lock()
	defer cm.updateMu.Unlock()

	cm.mu.RLock()
	oldConfig := cloneConfig(cm.config)
	watchers := make([]ConfigWatcher, len(cm.watchers))
	copy(watchers, cm.watchers)
	cm.mu.RUnlock()

	for _, watcher := range watchers {
		if err := watcher(cloneConfig(oldConfig), cloneConfig(newConfig)); err != nil {
			return fmt.Errorf("config watcher failed: %w", err)
		}
	}

	cm.mu.Lock()
	cm.config = cloneConfig(newConfig)
	cm.mu.Unlock()

	return nil
}

func cloneConfig(config *Config) *Config {
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

package gyro

import (
	"context"
	"fmt"
	"time"
)

// HealthChecker provides health checking capabilities for nodes.
type HealthChecker interface {
	Check(ctx context.Context, node Node) error
	AddNode(node Node)
	RemoveNode(nodeID string)
	StartMonitoring(ctx context.Context)
	StopMonitoring()
	IsNodeHealthy(nodeID string) bool
	AddHealthListener(listener HealthListener)
}

// ConfigurableHealthChecker optionally supports runtime configuration changes.
// HealthChecker implementations that do not need dynamic configuration do not
// need to implement this interface.
type ConfigurableHealthChecker interface {
	HealthChecker
	UpdateConfig(newConfig HealthCheckerConfig) error
	GetConfig() HealthCheckerConfig
	IsEnabled() bool
}

type HealthCheckerConfig struct {
	Enabled           bool          `json:"enabled"`
	Interval          time.Duration `json:"interval"`
	Timeout           time.Duration `json:"timeout"`
	FailureThreshold  int           `json:"failure_threshold"`
	RecoveryThreshold int           `json:"recovery_threshold"`
}

// ValidateHealthCheckerConfig validates values that are required by the
// monitoring runtime before it starts background workers.
func ValidateHealthCheckerConfig(config HealthCheckerConfig) error {
	if config.Interval <= 0 {
		return fmt.Errorf("health checker interval must be positive")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("health checker timeout must be positive")
	}
	if config.FailureThreshold <= 0 {
		return fmt.Errorf("health checker failure threshold must be positive")
	}
	if config.RecoveryThreshold <= 0 {
		return fmt.Errorf("health checker recovery threshold must be positive")
	}
	return nil
}

func DefaultHealthCheckerConfig() HealthCheckerConfig {
	return HealthCheckerConfig{
		Enabled:           true,
		Interval:          30 * time.Second,
		Timeout:           5 * time.Second,
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

type NodeHealthStats struct {
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastCheckTime        time.Time
	IsHealthy            bool
	TotalChecks          int64
	TotalFailures        int64
}

type HealthListener func(nodeID string, healthy bool)

// HealthAwarePoolStats contains statistics about a health-aware pool
type HealthAwarePoolStats struct {
	TotalNodes     int `json:"total_nodes"`
	HealthyNodes   int `json:"healthy_nodes"`
	UnhealthyNodes int `json:"unhealthy_nodes"`
}

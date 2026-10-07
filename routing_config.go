package gyro

// RoutingConfig contains the protocol-neutral settings shared by fixed-address
// adapters. Protocol packages own and interpret their connection settings.
type RoutingConfig struct {
	Locator       LocatorConfig       `json:"locator"`
	HealthChecker HealthCheckerConfig `json:"health_checker"`
}

// DefaultRoutingConfig returns the default routing and health settings.
func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		Locator:       DefaultLocatorConfig(),
		HealthChecker: DefaultHealthCheckerConfig(),
	}
}

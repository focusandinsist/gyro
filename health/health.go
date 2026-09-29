// Package health exposes construction helpers for the internal health
// implementation. Domain contracts remain in package gyro.
package health

import (
	"github.com/focusandinsist/gyro/gyro"
	internalhealth "github.com/focusandinsist/gyro/internal/health"
)

type Checker = internalhealth.DefaultHealthChecker
type Config = gyro.HealthCheckerConfig

func NewChecker(config Config) *Checker { return internalhealth.NewDefaultHealthChecker(config) }

func DefaultConfig() Config { return gyro.DefaultHealthCheckerConfig() }

func ValidateConfig(config Config) error { return gyro.ValidateHealthCheckerConfig(config) }

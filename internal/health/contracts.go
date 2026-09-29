package health

import "github.com/focusandinsist/gyro/gyro"

type Node = gyro.Node
type HealthCheckerConfig = gyro.HealthCheckerConfig
type HealthListener = gyro.HealthListener
type NodeHealthStats = gyro.NodeHealthStats
type HealthStatus = gyro.HealthStatus

const (
	Unknown   = gyro.Unknown
	Healthy   = gyro.Healthy
	Unhealthy = gyro.Unhealthy
)

var ValidateHealthCheckerConfig = gyro.ValidateHealthCheckerConfig

type healthProbe struct {
	node       Node
	generation uint64
}

package health

import (
	"log/slog"

	"github.com/focusandinsist/gyro/gyro"
)

type Node = gyro.Node
type Locator = gyro.Locator
type FailurePolicy = gyro.FailurePolicy
type HealthView = gyro.HealthView
type HealthChecker = gyro.HealthChecker
type ConfigurableHealthChecker = gyro.ConfigurableHealthChecker
type HealthCheckerConfig = gyro.HealthCheckerConfig
type HealthListener = gyro.HealthListener
type NodeHealthStats = gyro.NodeHealthStats
type HealthStatus = gyro.HealthStatus
type HealthAwarePoolStats = gyro.HealthAwarePoolStats
type Member = gyro.Member
type Endpoint = gyro.Endpoint
type Revision = gyro.Revision
type Candidate = gyro.Candidate
type CandidateSet = gyro.CandidateSet
type RouteRequest = gyro.RouteRequest
type RouteDecision = gyro.RouteDecision
type TopologySnapshot = gyro.TopologySnapshot

const (
	Unknown   = gyro.Unknown
	Healthy   = gyro.Healthy
	Unhealthy = gyro.Unhealthy
)

var (
	ErrNilContext               = gyro.ErrNilContext
	ErrLocatorClosed            = gyro.ErrLocatorClosed
	ErrNoMembers                = gyro.ErrNoMembers
	ErrNoEligibleCandidate      = gyro.ErrNoEligibleCandidate
	ErrFailoverNotAllowed       = gyro.ErrFailoverNotAllowed
	ErrSelectionMismatch        = gyro.ErrSelectionMismatch
	ValidateHealthCheckerConfig = gyro.ValidateHealthCheckerConfig
)

type healthProbe struct {
	node       Node
	generation uint64
}

var discardLogger = slog.New(slog.NewTextHandler(ioDiscard{}, nil))

type ioDiscard struct{}

func (ioDiscard) Write([]byte) (int, error) { return 0, nil }

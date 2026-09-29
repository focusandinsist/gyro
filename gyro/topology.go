package gyro

import (
	"context"
	"errors"
)

var (
	ErrNilContext           = errors.New("context cannot be nil")
	ErrInvalidSnapshot      = errors.New("invalid topology snapshot")
	ErrStaleRevision        = errors.New("stale topology revision")
	ErrRevisionConflict     = errors.New("topology revision conflict")
	ErrIncomparableRevision = errors.New("topology revisions are incomparable")
)

type Endpoint struct {
	Address    string
	Attributes map[string]string
}

type Member struct {
	ID         string
	Endpoints  []Endpoint
	Attributes map[string]string
}

type Revision struct {
	Source     string
	Generation uint64
	Token      string
}

type TopologySnapshot struct {
	Revision Revision
	Members  []Member
}

type TopologyStore interface {
	Snapshot() (TopologySnapshot, bool)
	Publish(context.Context, TopologySnapshot) error
	ResetSource(context.Context, TopologySnapshot) error
}

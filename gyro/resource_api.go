package gyro

import (
	"context"
	"errors"
)

var (
	ErrResourceUnavailable = errors.New("route resource unavailable")
	ErrResourcePoolClosed  = errors.New("resource pool is closed")
)

// Resource is an adapter-owned resource managed by an internal resource pool.
type Resource interface {
	MemberID() string
	Close() error
}

// ResourceFactory creates a resource for one topology member.
type ResourceFactory interface {
	Create(context.Context, Member) (Resource, error)
}

// ResourceHandle keeps a resource alive until Release is called.
type ResourceHandle interface {
	Resource() Resource
	Release() error
}

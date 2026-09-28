package test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/focusandinsist/gyro/gyro"
)

type fakeResource struct {
	id     string
	mu     sync.Mutex
	closed int
}

func (r *fakeResource) MemberID() string { return r.id }
func (r *fakeResource) Close() error {
	r.mu.Lock()
	r.closed++
	r.mu.Unlock()
	return nil
}

type fakeResourceFactory struct {
	mu      sync.Mutex
	created []*fakeResource
	failID  string
}

func (f *fakeResourceFactory) Create(_ context.Context, member gyro.Member) (gyro.Resource, error) {
	if member.ID == f.failID {
		return nil, errors.New("create failed")
	}
	resource := &fakeResource{id: member.ID}
	f.mu.Lock()
	f.created = append(f.created, resource)
	f.mu.Unlock()
	return resource, nil
}

func resourceMember(id, address string) gyro.Member {
	return gyro.Member{ID: id, Endpoints: []gyro.Endpoint{{Address: address}}}
}

func TestResourcePoolPreparesAtomicallyAndReusesUnchangedResources(t *testing.T) {
	factory := &fakeResourceFactory{}
	pool, err := gyro.NewResourcePool(factory)
	if err != nil {
		t.Fatalf("NewResourcePool failed: %v", err)
	}
	ctx := context.Background()
	if err := pool.Replace(ctx, []gyro.Member{resourceMember("a", "a"), resourceMember("b", "b")}); err != nil {
		t.Fatalf("initial Replace failed: %v", err)
	}
	handle, err := pool.Acquire(ctx, "a")
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	old := handle.Resource().(*fakeResource)
	if err := pool.Replace(ctx, []gyro.Member{resourceMember("a", "a"), resourceMember("b", "b2")}); err != nil {
		t.Fatalf("replacement failed: %v", err)
	}
	if old.closed != 0 {
		t.Fatal("unchanged borrowed resource was closed")
	}
	if err := handle.Release(); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if err := pool.Replace(ctx, []gyro.Member{resourceMember("a", "a"), resourceMember("b", "b2")}); err != nil {
		t.Fatalf("idempotent replacement failed: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := pool.Acquire(ctx, "a"); !errors.Is(err, gyro.ErrResourcePoolClosed) {
		t.Fatalf("Acquire after Close error = %v, want ErrResourcePoolClosed", err)
	}
}

func TestResourcePoolCreateFailureDoesNotPublishPartialResources(t *testing.T) {
	factory := &fakeResourceFactory{failID: "b"}
	pool, err := gyro.NewResourcePool(factory)
	if err != nil {
		t.Fatalf("NewResourcePool failed: %v", err)
	}
	err = pool.Replace(context.Background(), []gyro.Member{resourceMember("a", "a"), resourceMember("b", "b")})
	if !errors.Is(err, gyro.ErrResourceUnavailable) {
		t.Fatalf("Replace error = %v, want ErrResourceUnavailable", err)
	}
	if _, err := pool.Acquire(context.Background(), "a"); !errors.Is(err, gyro.ErrResourceUnavailable) {
		t.Fatalf("partial resource was published: %v", err)
	}
}

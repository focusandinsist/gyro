package main

import (
	"context"
	"fmt"

	"github.com/focusandinsist/gyro/gyro"
)

type workerResource struct{ id string }

func (r workerResource) MemberID() string { return r.id }
func (workerResource) Close() error       { return nil }

type workerFactory struct{}

func (workerFactory) Create(_ context.Context, member gyro.Member) (gyro.Resource, error) {
	return workerResource{id: member.ID}, nil
}

func main() {
	selector := gyro.NewRendezvousSelector("worker-demo-v1")
	snapshot := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "workers", Generation: 1, Token: "1"},
		Members: []gyro.Member{
			{ID: "worker-a", Endpoints: []gyro.Endpoint{{Address: "worker-a:8080"}}},
			{ID: "worker-b", Endpoints: []gyro.Endpoint{{Address: "worker-b:8080"}}},
		},
	}
	selection, err := selector.Select(context.Background(), gyro.RouteRequest{Key: "task-42"}, snapshot)
	if err != nil {
		panic(err)
	}
	pool, err := gyro.NewResourcePool(workerFactory{})
	if err != nil {
		panic(err)
	}
	if err := pool.Replace(context.Background(), snapshot.Members); err != nil {
		panic(err)
	}
	handle, err := pool.Acquire(context.Background(), selection.Candidates[0].MemberID)
	if err != nil {
		panic(err)
	}
	fmt.Println("task-42 ->", handle.Resource().MemberID())
	_ = handle.Release()
	_ = pool.Close()
}

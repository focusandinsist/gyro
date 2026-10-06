package main

import (
	"context"
	"fmt"

	"gyro"
	"gyro/internal/selector"
)

func main() {
	selector := selector.NewRendezvousSelector("demo-v1")
	snapshot := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "demo", Generation: 1, Token: "1"},
		Members: []gyro.Member{
			{ID: "worker-a", Endpoints: []gyro.Endpoint{{Address: "worker-a:8080"}}},
			{ID: "worker-b", Endpoints: []gyro.Endpoint{{Address: "worker-b:8080"}}},
			{ID: "worker-c", Endpoints: []gyro.Endpoint{{Address: "worker-c:8080"}}},
		},
	}
	selection, err := selector.Select(context.Background(), gyro.RouteRequest{Key: "task-42"}, snapshot)
	if err != nil {
		panic(err)
	}
	fmt.Println("primary:", selection.Candidates[0].MemberID)
	for _, candidate := range selection.Candidates[1:] {
		fmt.Println("candidate:", candidate.MemberID)
	}
}

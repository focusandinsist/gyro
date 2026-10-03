package main

import (
	"context"
	"fmt"

	"gyro"
)

func main() {
	router, err := gyro.NewRouter([]gyro.Member{
		{ID: "worker-a", Endpoints: []gyro.Endpoint{{Address: "worker-a:8080"}}},
		{ID: "worker-b", Endpoints: []gyro.Endpoint{{Address: "worker-b:8080"}}},
	}, gyro.DefaultLocatorConfig())
	if err != nil {
		panic(err)
	}
	member, err := router.Route(context.Background(), "task-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(member.ID, member.Endpoints[0].Address)
}

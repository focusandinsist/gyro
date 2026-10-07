package main

import (
	"context"
	"fmt"

	redisadapter "gyro/adapters/redis"
	clientpkg "gyro/client"
	"gyro/discovery/static"
	"gyro/health"
)

func main() {
	discovery := static.New([]string{"127.0.0.1:6379", "127.0.0.1:6380"})
	manager := clientpkg.NewConfigManager(clientpkg.DefaultConfig())
	client, err := clientpkg.NewClient("default", discovery, manager, redisadapter.NewNodeFactory(), health.NewChecker(health.DefaultConfig()))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if err := client.Start(context.Background()); err != nil {
		panic(err)
	}

	candidates, err := client.GetLocator().GetReplicas(context.Background(), "user:123", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(candidates[0].ID(), candidates[0].Address())
}

package main

import (
	"context"
	"fmt"

	redisadapter "gyro/adapters/redis"
)

func main() {
	client, err := redisadapter.NewCluster([]string{"127.0.0.1:6379", "127.0.0.1:6380"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	clients, err := client.GetClientsForReplicas(context.Background(), "user:123", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(clients[0].Options().Addr)
}

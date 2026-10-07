package main

import (
	"context"
	"fmt"

	grpcadapter "gyro/adapters/grpc"
)

func main() {
	client, err := grpcadapter.NewCluster([]string{"127.0.0.1:50051", "127.0.0.1:50052"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	connections, err := client.GetClientsForReplicas(context.Background(), "user:123", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(connections[0].Target())
}

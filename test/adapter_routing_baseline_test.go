package test

import (
	"context"
	"reflect"
	"testing"

	grpcadapter "gyro/adapters/grpc"
	redisadapter "gyro/adapters/redis"
)

func TestAdapterDefaultCandidateOrderBaseline(t *testing.T) {
	addresses := []string{"node-a.test:1234", "node-b.test:1234", "node-c.test:1234"}
	const key = "routing-regression-key"

	t.Run("redis", func(t *testing.T) {
		client, err := redisadapter.NewClient(addresses, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		clients, err := client.GetClientsForReplicas(context.Background(), key, len(addresses))
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, len(clients))
		for i, client := range clients {
			got[i] = client.Options().Addr
		}
		want := []string{"node-c.test:1234", "node-a.test:1234", "node-b.test:1234"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("default Redis candidate order = %v, want %v", got, want)
		}
	})

	t.Run("grpc", func(t *testing.T) {
		client, err := grpcadapter.NewClient(addresses, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		clients, err := client.GetClientsForReplicas(context.Background(), key, len(addresses))
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, len(clients))
		for i, client := range clients {
			got[i] = client.Target()
		}
		want := []string{"node-b.test:1234", "node-a.test:1234", "node-c.test:1234"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("default gRPC candidate order = %v, want %v", got, want)
		}
	})
}

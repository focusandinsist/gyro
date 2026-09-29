package test

import (
	"context"
	"net"
	"testing"

	grpcadapter "github.com/focusandinsist/gyro/adapters/grpc"
	"google.golang.org/grpc"
)

func TestGRPCEndToEndSameTopologyRoutesSameKeyAcrossClients(t *testing.T) {
	addresses := make([]string, 2)
	servers := make([]*grpc.Server, 2)
	listeners := make([]net.Listener, 2)
	for i := range addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		server := grpc.NewServer()
		go server.Serve(listener)
		addresses[i], servers[i], listeners[i] = listener.Addr().String(), server, listener
	}
	defer func() {
		for i := range servers {
			servers[i].GracefulStop()
			_ = listeners[i].Close()
		}
	}()

	first, err := grpcadapter.NewClient(addresses, nil)
	if err != nil {
		t.Fatalf("first client: %v", err)
	}
	second, err := grpcadapter.NewClient(addresses, nil)
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	defer first.Close()
	defer second.Close()
	key := "room-42"
	left, err := first.GetNodeForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("first route: %v", err)
	}
	right, err := second.GetNodeForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("second route: %v", err)
	}
	if left.ID() != right.ID() {
		t.Fatalf("same topology routed differently: %s vs %s", left.ID(), right.ID())
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	restarted, err := grpcadapter.NewClient(addresses, nil)
	if err != nil {
		t.Fatalf("restarted client: %v", err)
	}
	defer restarted.Close()
	restartedNode, err := restarted.GetNodeForKey(context.Background(), key)
	if err != nil {
		t.Fatalf("restarted route: %v", err)
	}
	if restartedNode.ID() != right.ID() {
		t.Fatalf("restart changed route: %s vs %s", restartedNode.ID(), right.ID())
	}
}

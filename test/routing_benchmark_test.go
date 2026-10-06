package test

import (
	"context"
	"fmt"
	"testing"

	"gyro"
	"gyro/internal/resource"
	"gyro/internal/selector"
)

func benchmarkSnapshot(size int) gyro.TopologySnapshot {
	members := make([]gyro.Member, size)
	for i := range members {
		members[i] = gyro.Member{ID: fmt.Sprintf("worker-%03d", i), Endpoints: []gyro.Endpoint{{Address: fmt.Sprintf("127.0.0.1:%d", 7000+i)}}}
	}
	return gyro.TopologySnapshot{Revision: gyro.Revision{Source: "benchmark", Generation: 1, Token: "1"}, Members: members}
}

func BenchmarkRendezvousSelector(b *testing.B) {
	sel := selector.NewRendezvousSelector("benchmark-v1")
	snapshot := benchmarkSnapshot(64)
	request := gyro.RouteRequest{Key: "tenant-42"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := sel.Select(context.Background(), request, snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConsistentHashSelector(b *testing.B) {
	sel, err := gyro.NewConsistentHashSelector(gyro.DefaultLocatorConfig())
	if err != nil {
		b.Fatal(err)
	}
	snapshot := benchmarkSnapshot(64)
	request := gyro.RouteRequest{Key: "tenant-42"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := sel.Select(context.Background(), request, snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResourcePoolAcquireRelease(b *testing.B) {
	factory := &workerFactory{}
	pool, err := resource.NewResourcePool(factory)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Replace(context.Background(), benchmarkSnapshot(32).Members); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handle, err := pool.Acquire(context.Background(), "worker-000")
		if err != nil {
			b.Fatal(err)
		}
		if err := handle.Release(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestResourcePoolConcurrentAcquireReplaceClose(t *testing.T) {
	pool, err := resource.NewResourcePool(&workerFactory{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pool.Replace(ctx, benchmarkSnapshot(8).Members); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = pool.Replace(ctx, benchmarkSnapshot(8+i%3).Members)
		}
	}()
	for i := 0; i < 500; i++ {
		if handle, acquireErr := pool.Acquire(ctx, "worker-000"); acquireErr == nil {
			_ = handle.Release()
		}
	}
	<-done
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

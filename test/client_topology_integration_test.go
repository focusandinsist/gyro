package test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	clientpkg "gyro/client"
	"gyro/gyro"
)

func TestClientRetainsTopologyAndMarksItStaleAfterWatchClose(t *testing.T) {
	snapshot := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-a", Generation: 1, Token: "1"},
		Members:  []gyro.Member{{ID: "node-a", Endpoints: []gyro.Endpoint{{Address: "node-a"}}}},
	}
	discovery := newScriptedDiscovery(snapshot)
	config := clientpkg.DefaultConfig()
	config.HealthChecker.Enabled = false
	client, err := clientpkg.NewClient(
		"orders",
		discovery,
		clientpkg.NewConfigManager(config),
		testNodeFactory{},
		testHealthChecker{},
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	select {
	case <-discovery.stream.firstDelivered:
	case <-time.After(time.Second):
		t.Fatal("watch did not receive its initial complete snapshot")
	}
	discovery.stream.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := client.GetTopologyStatus()
		if status.Stale {
			if !status.HasSnapshot || status.Snapshot.Revision != snapshot.Revision {
				t.Fatalf("stale status lost the last accepted snapshot: %#v", status)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("client did not expose stale topology after watch close")
}

func TestClientRejectsRetiredSourceEventsAfterReconnect(t *testing.T) {
	sourceA := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-a", Generation: 1, Token: "1"},
		Members:  []gyro.Member{{ID: "a", Endpoints: []gyro.Endpoint{{Address: "a"}}}},
	}
	sourceB := gyro.TopologySnapshot{
		Revision: gyro.Revision{Source: "source-b", Generation: 1, Token: "1"},
		Members:  []gyro.Member{{ID: "b", Endpoints: []gyro.Endpoint{{Address: "b"}}}},
	}
	retiredA := sourceA
	retiredA.Revision.Generation = 100
	discovery := newSequencedDiscovery(sourceA, sourceB, retiredA)
	config := clientpkg.DefaultConfig()
	config.HealthChecker.Enabled = false
	client, err := clientpkg.NewClient("orders", discovery, clientpkg.NewConfigManager(config), testNodeFactory{}, testHealthChecker{})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	first := discovery.nextWatch(t)
	first.Close()
	second := discovery.nextWatch(t)
	select {
	case <-second.firstDelivered:
	case <-time.After(time.Second):
		t.Fatal("new source snapshot was not delivered")
	}
	second.Close()
	third := discovery.nextWatch(t)
	select {
	case <-third.firstDelivered:
	case <-time.After(2 * time.Second):
		t.Fatal("retired source snapshot was not observed")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status := client.GetTopologyStatus()
		if status.HasSnapshot && status.Snapshot.Revision.Source == "source-b" && status.Stale {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("retired source changed client topology: %#v", client.GetTopologyStatus())
}

type scriptedDiscovery struct {
	snapshot gyro.TopologySnapshot
	stream   *scriptedStream
}

type sequencedDiscovery struct {
	mu       sync.Mutex
	initial  gyro.TopologySnapshot
	streams  []*scriptedStream
	watching chan *scriptedStream
}

func newSequencedDiscovery(initial gyro.TopologySnapshot, next ...gyro.TopologySnapshot) *sequencedDiscovery {
	streams := make([]*scriptedStream, 0, len(next)+1)
	streams = append(streams, newScriptedStream(initial))
	for _, snapshot := range next {
		streams = append(streams, newScriptedStream(snapshot))
	}
	return &sequencedDiscovery{initial: initial, streams: streams, watching: make(chan *scriptedStream, len(streams)+1)}
}

func (d *sequencedDiscovery) Discover(context.Context, string) (gyro.TopologySnapshot, error) {
	return d.initial, nil
}

func (d *sequencedDiscovery) Watch(context.Context, string) (gyro.TopologyStream, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.streams) == 0 {
		return nil, io.EOF
	}
	stream := d.streams[0]
	d.streams = d.streams[1:]
	d.watching <- stream
	return stream, nil
}

func (d *sequencedDiscovery) nextWatch(t *testing.T) *scriptedStream {
	t.Helper()
	select {
	case stream := <-d.watching:
		return stream
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for discovery reconnect")
		return nil
	}
}

func newScriptedDiscovery(snapshot gyro.TopologySnapshot) *scriptedDiscovery {
	return &scriptedDiscovery{snapshot: snapshot, stream: newScriptedStream(snapshot)}
}

func (d *scriptedDiscovery) Discover(context.Context, string) (gyro.TopologySnapshot, error) {
	return d.snapshot, nil
}

func (d *scriptedDiscovery) Watch(context.Context, string) (gyro.TopologyStream, error) {
	return d.stream, nil
}

type scriptedStream struct {
	mu             sync.Mutex
	updates        chan gyro.TopologySnapshot
	firstDelivered chan struct{}
	closed         bool
}

func newScriptedStream(snapshot gyro.TopologySnapshot) *scriptedStream {
	stream := &scriptedStream{
		updates:        make(chan gyro.TopologySnapshot, 1),
		firstDelivered: make(chan struct{}),
	}
	stream.updates <- snapshot
	return stream
}

func (s *scriptedStream) Next(ctx context.Context) (gyro.TopologySnapshot, error) {
	select {
	case <-ctx.Done():
		return gyro.TopologySnapshot{}, ctx.Err()
	case snapshot, ok := <-s.updates:
		if !ok {
			return gyro.TopologySnapshot{}, io.EOF
		}
		select {
		case <-s.firstDelivered:
		default:
			close(s.firstDelivered)
		}
		return snapshot, nil
	}
}

func (s *scriptedStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.updates)
	}
	return nil
}

type testNode struct {
	id      string
	address string
}

func (n testNode) ID() string                     { return n.id }
func (n testNode) Address() string                { return n.address }
func (n testNode) IsHealthy(context.Context) bool { return true }
func (n testNode) Close() error                   { return nil }

type testNodeFactory struct{}

func (testNodeFactory) CreateNode(info gyro.NodeInfo) (gyro.Node, error) {
	return testNode{id: info.ID, address: info.Address}, nil
}

type testHealthChecker struct{}

func (testHealthChecker) Check(context.Context, gyro.Node) error { return nil }
func (testHealthChecker) AddNode(gyro.Node)                      {}
func (testHealthChecker) RemoveNode(string)                      {}
func (testHealthChecker) StartMonitoring(context.Context)        {}
func (testHealthChecker) StopMonitoring()                        {}
func (testHealthChecker) IsNodeHealthy(string) bool              { return true }
func (testHealthChecker) AddHealthListener(gyro.HealthListener)  {}

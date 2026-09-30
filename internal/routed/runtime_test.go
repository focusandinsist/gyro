package routed

import (
	"context"
	"errors"
	"testing"

	"gyro/gyro"
)

type testNode struct {
	id, address string
	closed      bool
}

func (n *testNode) ID() string                     { return n.id }
func (n *testNode) Address() string                { return n.address }
func (n *testNode) IsHealthy(context.Context) bool { return !n.closed }
func (n *testNode) Close() error                   { n.closed = true; return nil }

func TestNewRollsBackCreatedNodes(t *testing.T) {
	var first *testNode
	_, err := New([]string{"first", "second"}, gyro.DefaultLocatorConfig(), gyro.DefaultHealthCheckerConfig(), "test", func(info gyro.NodeInfo) (gyro.Node, error) {
		if info.Address == "second" {
			return nil, errors.New("dial failed")
		}
		first = &testNode{id: info.ID, address: info.Address}
		return first, nil
	}, nil)
	if err == nil || first == nil || !first.closed {
		t.Fatalf("failed construction did not close the first node: err=%v, node=%+v", err, first)
	}
}

func TestRuntimeNativeTraversalAndClose(t *testing.T) {
	var nodes []*testNode
	config := gyro.DefaultHealthCheckerConfig()
	config.Enabled = false
	runtime, err := New([]string{"first", "second"}, gyro.DefaultLocatorConfig(), config, "test", func(info gyro.NodeInfo) (gyro.Node, error) {
		node := &testNode{id: info.ID, address: info.Address}
		nodes = append(nodes, node)
		return node, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	all := runtime.Locator().GetAllNodes()
	if len(all) != 2 {
		t.Fatalf("unexpected nodes: %v", all)
	}
	replicas, err := runtime.Locator().GetReplicas(context.Background(), "key", 2)
	if err != nil || len(replicas) != 2 {
		t.Fatalf("replicas = %v, err = %v", replicas, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	for _, node := range nodes {
		if !node.closed {
			t.Fatalf("node %s was not closed", node.id)
		}
	}
}

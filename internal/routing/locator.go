package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/resource"
	"github.com/focusandinsist/gyro/internal/selector"
)

// Locator binds pure candidate selection to adapter nodes. ResourcePool is the
// only owner of node Close calls; the node map is a routing index.
type Locator struct {
	lifecycleMu sync.Mutex
	mu          sync.RWMutex
	nodes       map[string]gyro.Node
	selector    gyro.Selector
	resources   *resource.ResourcePool
	closed      bool
	logger      atomic.Pointer[slog.Logger]
}

var _ gyro.Locator = (*Locator)(nil)

func NewLocator(config gyro.LocatorConfig) (*Locator, error) {
	selection, err := selector.NewConsistentHashSelector(config)
	if err != nil {
		return nil, err
	}
	locator := &Locator{nodes: make(map[string]gyro.Node), selector: selection, resources: resource.NewOwnedPool()}
	locator.logger.Store(poolDiscardLogger)
	return locator, nil
}

func (l *Locator) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = poolDiscardLogger
	}
	l.logger.Store(logger)
}

func (l *Locator) selectRoute(ctx context.Context, key string) (gyro.TopologySnapshot, gyro.CandidateSet, []gyro.Node, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.selectRouteLocked(ctx, key)
}

func (l *Locator) selectRouteLocked(ctx context.Context, key string) (gyro.TopologySnapshot, gyro.CandidateSet, []gyro.Node, error) {
	if l.closed {
		return gyro.TopologySnapshot{}, gyro.CandidateSet{}, nil, gyro.ErrLocatorClosed
	}
	members := make([]gyro.Member, 0, len(l.nodes))
	for id, node := range l.nodes {
		members = append(members, gyro.Member{ID: id, Endpoints: []gyro.Endpoint{{Address: node.Address()}}})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	ids := make([]string, len(members))
	for i := range members {
		ids[i] = members[i].ID
	}
	snapshot := gyro.TopologySnapshot{Revision: gyro.Revision{Source: "locator", Generation: 1, Token: strings.Join(ids, "\x00")}, Members: members}
	selection, err := l.selector.Select(ctx, gyro.RouteRequest{Key: key}, snapshot)
	if err != nil {
		return gyro.TopologySnapshot{}, gyro.CandidateSet{}, nil, err
	}
	nodes := make([]gyro.Node, 0, len(selection.Candidates))
	for _, candidate := range selection.Candidates {
		nodes = append(nodes, l.nodes[candidate.MemberID])
	}
	return snapshot, selection, nodes, nil
}

func (l *Locator) Get(ctx context.Context, key string) (gyro.Node, error) {
	_, _, nodes, err := l.selectRoute(ctx, key)
	if err != nil {
		return nil, err
	}
	return nodes[0], nil
}

func (l *Locator) GetReplicas(ctx context.Context, key string, count int) ([]gyro.Node, error) {
	if count <= 0 {
		l.mu.RLock()
		closed := l.closed
		l.mu.RUnlock()
		if closed {
			return nil, gyro.ErrLocatorClosed
		}
		return []gyro.Node{}, nil
	}
	_, _, nodes, err := l.selectRoute(ctx, key)
	if err != nil {
		return nil, err
	}
	if count < len(nodes) {
		nodes = nodes[:count]
	}
	return nodes, nil
}

func (l *Locator) AddNodeContext(ctx context.Context, node gyro.Node) error {
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	if ctx == nil {
		return gyro.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if node == nil || node.ID() == "" {
		return fmt.Errorf("node and node ID are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return gyro.ErrLocatorClosed
	}
	if err := l.resources.Add(nodeResource{node}); err != nil {
		return err
	}
	l.nodes[node.ID()] = node
	return nil
}

func (l *Locator) RemoveNodeContext(ctx context.Context, nodeID string) error {
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	if ctx == nil {
		return gyro.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return gyro.ErrLocatorClosed
	}
	if _, exists := l.nodes[nodeID]; !exists {
		l.mu.Unlock()
		return fmt.Errorf("node %s not found in locator", nodeID)
	}
	delete(l.nodes, nodeID)
	l.mu.Unlock()
	if err := l.resources.Remove(nodeID); err != nil {
		l.logger.Load().Warn("failed to close node", "node_id", nodeID, "error", err)
	}
	return nil
}

func (l *Locator) GetAllNodes() []gyro.Node {
	l.mu.RLock()
	defer l.mu.RUnlock()
	nodes := make([]gyro.Node, 0, len(l.nodes))
	for _, node := range l.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (l *Locator) Close() error {
	l.lifecycleMu.Lock()
	defer l.lifecycleMu.Unlock()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.nodes = make(map[string]gyro.Node)
	l.mu.Unlock()
	return l.resources.Close()
}

type nodeResource struct{ gyro.Node }

func (r nodeResource) MemberID() string { return r.Node.ID() }

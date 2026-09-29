package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/focusandinsist/gyro/gyro"
	"github.com/focusandinsist/gyro/internal/health"
	"github.com/focusandinsist/gyro/internal/routing"
	"github.com/focusandinsist/gyro/internal/topology"
)

// updateServiceDiscoveryHealth updates the service discovery health status.
func (c *Client) updateServiceDiscoveryHealth(run *clientRun, healthy bool, err error) {
	// Serialize health publication with topology reconciliation so stateMu is
	// never held while another lifecycle operation performs node I/O.
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.state.run != run {
		return
	}

	c.state.serviceDiscoveryHealthy = healthy
	if !healthy {
		c.state.topologyStale = true
	}
	if err != nil {
		c.state.lastServiceDiscoveryError = err.Error()
		if !healthy {
			c.state.serviceDiscoveryRetries++
		}
	} else {
		c.state.lastServiceDiscoveryError = ""
		if healthy {
			c.state.serviceDiscoveryRetries = 0
		}
	}
}

func (c *Client) acceptTopologySnapshot(ctx context.Context, snapshot gyro.TopologySnapshot, allowSourceReset bool) (gyro.TopologySnapshot, error) {
	c.topologyMu.Lock()
	defer c.topologyMu.Unlock()

	c.stateMu.RLock()
	store := c.state.topologyStore
	_, retired := c.state.retiredSources[snapshot.Revision.Source]
	c.stateMu.RUnlock()
	if store == nil {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	if retired {
		return gyro.TopologySnapshot{}, gyro.ErrIncomparableRevision
	}

	current, hasCurrent := store.Snapshot()
	if !hasCurrent || current.Revision.Source == snapshot.Revision.Source {
		if err := store.Publish(ctx, snapshot); err != nil {
			return gyro.TopologySnapshot{}, err
		}
	} else {
		if !allowSourceReset {
			return gyro.TopologySnapshot{}, gyro.ErrIncomparableRevision
		}
		if err := store.ResetSource(ctx, snapshot); err != nil {
			return gyro.TopologySnapshot{}, err
		}
		c.stateMu.Lock()
		if c.state.retiredSources == nil {
			c.state.retiredSources = make(map[string]struct{})
		}
		c.state.retiredSources[current.Revision.Source] = struct{}{}
		c.stateMu.Unlock()
	}

	c.stateMu.Lock()
	c.state.topologyStale = false
	c.stateMu.Unlock()
	accepted, ok := store.Snapshot()
	if !ok {
		return gyro.TopologySnapshot{}, gyro.ErrInvalidSnapshot
	}
	return accepted, nil
}

func (c *Client) reconcileAcceptedTopology(run *clientRun, snapshot gyro.TopologySnapshot, previous gyro.TopologySnapshot, hasPrevious bool) {
	if hasPrevious {
		diff := topology.Diff(previous, snapshot)
		if len(diff.Added) == 0 && len(diff.Removed) == 0 && len(diff.Updated) == 0 {
			return
		}
		if previous.Revision.Source != snapshot.Revision.Source {
			// Source epochs are different identity domains. Reconcile through a
			// full remove/add cycle even when member IDs happen to match.
			c.reconcileServiceNodesFromReset(run, snapshot)
			return
		}
	}
	nodeInfos, err := nodeInfosFromTopologySnapshot(snapshot)
	if err != nil {
		c.log().Error("failed to convert topology snapshot", "error", err)
		return
	}
	c.reconcileServiceNodes(run, nodeInfos)
}

func (c *Client) reconcileServiceNodesFromReset(run *clientRun, snapshot gyro.TopologySnapshot) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.stateMu.RLock()
	if c.state.run != run || run.ctx == nil || run.ctx.Err() != nil {
		c.stateMu.RUnlock()
		return
	}
	ids := make([]string, 0, len(c.state.nodeInfos))
	for id := range c.state.nodeInfos {
		ids = append(ids, id)
	}
	locator := run.pool
	nodeFactory := c.state.nodeFactory
	c.stateMu.RUnlock()
	sort.Strings(ids)
	for _, id := range ids {
		_ = locator.RemoveNodeContext(run.ctx, id)
	}
	nodeInfos, err := nodeInfosFromTopologySnapshot(snapshot)
	if err != nil {
		return
	}
	current := make(map[string]gyro.NodeInfo, len(nodeInfos))
	for _, info := range nodeInfos {
		node, createErr := nodeFactory.CreateNode(info)
		if createErr != nil {
			continue
		}
		if addErr := locator.AddNodeContext(run.ctx, node); addErr != nil {
			_ = node.Close()
			continue
		}
		current[info.ID] = cloneNodeInfo(info)
	}
	c.stateMu.Lock()
	if c.state.run == run && run.ctx.Err() == nil {
		c.state.nodeInfos = current
	}
	c.stateMu.Unlock()
}

func (c *Client) currentTopologySnapshot() (gyro.TopologySnapshot, bool) {
	c.stateMu.RLock()
	store := c.state.topologyStore
	c.stateMu.RUnlock()
	if store == nil {
		return gyro.TopologySnapshot{}, false
	}
	return store.Snapshot()
}

func nodeInfosFromTopologySnapshot(snapshot gyro.TopologySnapshot) ([]gyro.NodeInfo, error) {
	nodeInfos := make([]gyro.NodeInfo, len(snapshot.Members))
	for i, member := range snapshot.Members {
		if len(member.Endpoints) == 0 || member.Endpoints[0].Address == "" {
			return nil, gyro.ErrInvalidSnapshot
		}
		nodeInfos[i] = gyro.NodeInfo{
			ID:       member.ID,
			Address:  member.Endpoints[0].Address,
			Metadata: cloneStringMap(member.Attributes),
		}
	}
	sortNodeInfosByID(nodeInfos)
	return nodeInfos, nil
}

// watchServiceNodes watches for service node changes with retry mechanism.
func (c *Client) watchServiceNodes(run *clientRun) {
	defer close(run.done)
	ctx := run.ctx
	const (
		maxRetries = 10
		baseDelay  = time.Second
		maxDelay   = time.Minute
	)

	retryCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stream, err := c.deps.discovery.Watch(ctx, c.deps.serviceName)
		if err != nil {
			c.updateServiceDiscoveryHealth(run, false, err)
			c.log().Warn("service discovery watch failed", "attempt", retryCount+1, "max_retries", maxRetries, "error", err)

			retryCount++
			if retryCount >= maxRetries {
				c.log().Error("service discovery: giving up after max retries")
				return
			}

			// Exponential backoff.
			multiplier := 1
			for i := 0; i < retryCount; i++ {
				multiplier *= 2
			}
			delay := time.Duration(int64(baseDelay) * int64(multiplier))
			if delay > maxDelay {
				delay = maxDelay
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
				continue
			}
		}

		retryCount = 0
		c.updateServiceDiscoveryHealth(run, true, nil)
		c.log().Info("service discovery watch established")

		watchFailed := c.processServiceWatch(run, stream)
		if !watchFailed {
			return
		}

		c.updateServiceDiscoveryHealth(run, false, fmt.Errorf("service discovery watch stream ended"))
		c.log().Warn("service discovery watch failed, retrying")
	}
}

// processServiceWatch processes events from the service discovery watch channel.
// It returns true if the watch failed and should be retried, false for normal shutdown.
func (c *Client) processServiceWatch(run *clientRun, stream gyro.TopologyStream) bool {
	defer stream.Close()
	firstSnapshot := true
	for {
		snapshot, err := stream.Next(run.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || run.ctx.Err() != nil {
				return false
			}
			if errors.Is(err, io.EOF) {
				return true
			}
			c.log().Warn("service discovery stream failed", "error", err)
			return true
		}
		previous, hasPrevious := c.currentTopologySnapshot()
		accepted, err := c.acceptTopologySnapshot(run.ctx, snapshot, firstSnapshot)
		if err != nil {
			if errors.Is(err, gyro.ErrStaleRevision) || errors.Is(err, gyro.ErrRevisionConflict) {
				c.log().Debug("service discovery snapshot ignored", "error", err)
				continue
			}
			c.log().Warn("service discovery snapshot rejected", "error", err)
			return true
		}
		firstSnapshot = false
		c.reconcileAcceptedTopology(run, accepted, previous, hasPrevious)
	}
}

// reconcileServiceNodes serializes topology preparation with configuration
// replacement and publishes only if this run still owns the client state.
func (c *Client) reconcileServiceNodes(run *clientRun, newNodeInfos []gyro.NodeInfo) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	if c.state.run != run || run.ctx == nil || run.ctx.Err() != nil {
		c.stateMu.RUnlock()
		return
	}
	locator := run.pool
	nodeFactory := c.state.nodeFactory
	currentInfos := make(map[string]gyro.NodeInfo, len(c.state.nodeInfos))
	for nodeID, nodeInfo := range c.state.nodeInfos {
		currentInfos[nodeID] = cloneNodeInfo(nodeInfo)
	}
	c.stateMu.RUnlock()
	currentNodes := locator.GetAllNodes()

	currentNodeMap := make(map[string]gyro.Node)
	for _, node := range currentNodes {
		currentNodeMap[node.ID()] = node
	}

	newNodeMap := make(map[string]gyro.NodeInfo)
	for _, nodeInfo := range newNodeInfos {
		newNodeMap[nodeInfo.ID] = nodeInfo
	}

	var nodesToRemove []string
	for nodeID := range currentNodeMap {
		if _, exists := newNodeMap[nodeID]; !exists {
			nodesToRemove = append(nodesToRemove, nodeID)
		}
	}
	sort.Strings(nodesToRemove)

	var nodesToAdd []gyro.NodeInfo
	for nodeID, nodeInfo := range newNodeMap {
		if _, exists := currentNodeMap[nodeID]; !exists {
			nodesToAdd = append(nodesToAdd, nodeInfo)
		}
	}
	sortNodeInfosByID(nodesToAdd)

	var nodesToUpdate []gyro.NodeInfo
	for nodeID, newNodeInfo := range newNodeMap {
		if currentNode, exists := currentNodeMap[nodeID]; exists {
			oldNodeInfo, hasOldInfo := currentInfos[nodeID]
			if currentNode.Address() != newNodeInfo.Address || !hasOldInfo || !stringMapEqual(oldNodeInfo.Metadata, newNodeInfo.Metadata) {
				nodesToUpdate = append(nodesToUpdate, newNodeInfo)
			}
		}
	}
	sortNodeInfosByID(nodesToUpdate)

	for _, nodeID := range nodesToRemove {
		if err := locator.RemoveNodeContext(run.ctx, nodeID); err != nil {
			c.log().Error("failed to remove node", "node_id", nodeID, "error", err)
		} else {
			delete(currentInfos, nodeID)
			c.log().Info("node removed", "node_id", nodeID)
		}
	}

	for _, nodeInfo := range nodesToAdd {
		node, err := nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNodeContext(run.ctx, node); err != nil {
			_ = node.Close()
			c.log().Error("failed to add node", "node_id", nodeInfo.ID, "error", err)
		} else {
			currentInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			c.log().Info("node added", "node_id", nodeInfo.ID)
		}
	}

	for _, nodeInfo := range nodesToUpdate {
		if err := locator.RemoveNodeContext(run.ctx, nodeInfo.ID); err != nil {
			c.log().Error("failed to remove node for update", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		node, err := nodeFactory.CreateNode(nodeInfo)
		if err != nil {
			c.log().Error("failed to create updated node", "node_id", nodeInfo.ID, "error", err)
			continue
		}

		if err := locator.AddNodeContext(run.ctx, node); err != nil {
			_ = node.Close()
			c.log().Error("failed to add updated node", "node_id", nodeInfo.ID, "error", err)
		} else {
			currentInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			c.log().Info("node updated", "node_id", nodeInfo.ID)
		}
	}

	c.log().Info("incremental update completed",
		"added", len(nodesToAdd), "removed", len(nodesToRemove), "updated", len(nodesToUpdate))

	c.stateMu.Lock()
	if c.state.run == run && run.ctx.Err() == nil {
		c.state.nodeInfos = currentInfos
	}
	c.stateMu.Unlock()
}

// handleConfigChange handles configuration changes with incremental updates.
func (c *Client) handleConfigChange(oldConfig, newConfig *Config) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	run := c.state.run
	currentFactory := c.state.nodeFactory
	c.stateMu.RUnlock()
	if run == nil || run.ctx == nil || run.ctx.Err() != nil {
		return nil
	}
	activeLocator := run.pool

	locatorChanged := !c.locatorConfigEqual(oldConfig.Locator, newConfig.Locator)
	connectionChanged := !c.connectionConfigEqual(oldConfig.Connection, newConfig.Connection)
	healthCheckerChanged := !c.healthCheckerConfigEqual(oldConfig.HealthChecker, newConfig.HealthChecker)

	var (
		replacementLocator   *routing.Locator
		replacementFactory   gyro.NodeFactory
		replacementNodeInfos []gyro.NodeInfo
	)

	if locatorChanged || connectionChanged {
		replacementFactory = currentFactory
		if connectionChanged {
			configurableFactory, ok := currentFactory.(gyro.ConnectionConfigurableNodeFactory)
			if !ok {
				return fmt.Errorf("node factory does not support connection configuration updates")
			}

			var err error
			replacementFactory, err = configurableFactory.WithConnectionConfig(newConfig.Connection)
			if err != nil {
				return fmt.Errorf("failed to prepare node factory for connection config update: %w", err)
			}
		}

		var err error
		replacementLocator, replacementNodeInfos, err = c.buildLocatorUnsafe(newConfig, replacementFactory)
		if err != nil {
			return fmt.Errorf("failed to build replacement locator: %w", err)
		}
	}

	if healthCheckerChanged {
		if err := c.updateHealthCheckerConfig(newConfig.HealthChecker); err != nil {
			if replacementLocator != nil {
				_ = replacementLocator.Close()
			}
			c.log().Error("failed to update health checker config", "error", err)
			return err
		}
	}

	if replacementLocator != nil {
		if err := activeLocator.ReplaceLocator(replacementLocator); err != nil {
			_ = replacementLocator.Close()
			if healthCheckerChanged {
				if rollbackErr := c.updateHealthCheckerConfig(oldConfig.HealthChecker); rollbackErr != nil {
					return fmt.Errorf("failed to replace active locator: %v; failed to roll back health checker config: %w", err, rollbackErr)
				}
			}
			return fmt.Errorf("failed to replace active locator: %w", err)
		}

		c.stateMu.Lock()
		if c.state.run == run && run.ctx.Err() == nil {
			c.state.nodeFactory = replacementFactory
			c.state.nodeInfos = make(map[string]gyro.NodeInfo, len(replacementNodeInfos))
			for _, nodeInfo := range replacementNodeInfos {
				c.state.nodeInfos[nodeInfo.ID] = cloneNodeInfo(nodeInfo)
			}
		}
		c.stateMu.Unlock()
		c.log().Info("client locator replaced after configuration change",
			"locator_changed", locatorChanged, "connection_changed", connectionChanged)
	}

	c.log().Info("config update completed")
	return nil
}

// locatorConfigEqual compares two locator configurations.
func (c *Client) locatorConfigEqual(old, new gyro.LocatorConfig) bool {
	return old.PartitionCount == new.PartitionCount &&
		old.ReplicationFactor == new.ReplicationFactor &&
		old.Load == new.Load &&
		old.HashFunction == new.HashFunction
}

// healthCheckerConfigEqual compares two health checker configurations.
func (c *Client) healthCheckerConfigEqual(old, new gyro.HealthCheckerConfig) bool {
	return old.Enabled == new.Enabled &&
		old.Interval == new.Interval &&
		old.Timeout == new.Timeout &&
		old.FailureThreshold == new.FailureThreshold &&
		old.RecoveryThreshold == new.RecoveryThreshold
}

// connectionConfigEqual compares two connection configurations.
func (c *Client) connectionConfigEqual(old, new gyro.ConnectionConfig) bool {
	return old.MaxIdleConns == new.MaxIdleConns &&
		old.MaxActiveConns == new.MaxActiveConns &&
		old.IdleTimeout == new.IdleTimeout &&
		old.ConnectTimeout == new.ConnectTimeout &&
		old.ReadTimeout == new.ReadTimeout &&
		old.WriteTimeout == new.WriteTimeout
}

func stringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

// updateHealthCheckerConfig updates the health checker configuration.
func (c *Client) updateHealthCheckerConfig(newConfig gyro.HealthCheckerConfig) error {
	if err := health.ValidateHealthCheckerConfig(newConfig); err != nil {
		return fmt.Errorf("invalid health checker config: %w", err)
	}

	checker, ok := c.deps.healthChecker.(gyro.ConfigurableHealthChecker)
	if !ok {
		return fmt.Errorf("health checker does not support runtime configuration")
	}
	if err := checker.UpdateConfig(newConfig); err != nil {
		return fmt.Errorf("failed to update health checker config: %w", err)
	}

	c.log().Info("health checker config updated",
		"enabled", newConfig.Enabled, "interval", newConfig.Interval, "timeout", newConfig.Timeout)

	return nil
}

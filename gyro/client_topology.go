package gyro

import (
	"fmt"
	"sort"
	"time"
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

// watchServiceNodes watches for service node changes with retry mechanism.
func (c *Client) watchServiceNodes(run *clientRun) {
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

		nodesCh, err := c.deps.discovery.Watch(ctx, c.deps.serviceName)
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

		watchFailed := c.processServiceWatch(run, nodesCh)
		if !watchFailed {
			return
		}

		c.updateServiceDiscoveryHealth(run, false, fmt.Errorf("service discovery watch channel closed unexpectedly"))
		c.log().Warn("service discovery watch failed, retrying")
	}
}

// processServiceWatch processes events from the service discovery watch channel.
// It returns true if the watch failed and should be retried, false for normal shutdown.
func (c *Client) processServiceWatch(run *clientRun, nodesCh <-chan []NodeInfo) bool {
	for {
		select {
		case <-run.ctx.Done():
			return false // normal shutdown
		case nodes, ok := <-nodesCh:
			if !ok {
				return true // channel closed, caller should retry
			}
			c.reconcileServiceNodes(run, nodes)
		}
	}
}

// reconcileServiceNodes serializes topology preparation with configuration
// replacement and publishes only if this run still owns the client state.
func (c *Client) reconcileServiceNodes(run *clientRun, newNodeInfos []NodeInfo) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	if c.state.run != run || run.ctx == nil || run.ctx.Err() != nil {
		c.stateMu.RUnlock()
		return
	}
	locator := run.pool
	healthChecker := c.deps.healthChecker
	nodeFactory := c.state.nodeFactory
	currentInfos := make(map[string]NodeInfo, len(c.state.nodeInfos))
	for nodeID, nodeInfo := range c.state.nodeInfos {
		currentInfos[nodeID] = cloneNodeInfo(nodeInfo)
	}
	c.stateMu.RUnlock()
	currentNodes := locator.GetAllNodes()

	currentNodeMap := make(map[string]Node)
	for _, node := range currentNodes {
		currentNodeMap[node.ID()] = node
	}

	newNodeMap := make(map[string]NodeInfo)
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

	var nodesToAdd []NodeInfo
	for nodeID, nodeInfo := range newNodeMap {
		if _, exists := currentNodeMap[nodeID]; !exists {
			nodesToAdd = append(nodesToAdd, nodeInfo)
		}
	}
	sortNodeInfosByID(nodesToAdd)

	var nodesToUpdate []NodeInfo
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
			if healthChecker != nil {
				healthChecker.RemoveNode(nodeID)
			}
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
			if healthChecker != nil {
				healthChecker.AddNode(node)
			}
			c.log().Info("node added", "node_id", nodeInfo.ID)
		}
	}

	for _, nodeInfo := range nodesToUpdate {
		if err := locator.RemoveNodeContext(run.ctx, nodeInfo.ID); err != nil {
			c.log().Error("failed to remove node for update", "node_id", nodeInfo.ID, "error", err)
			continue
		}
		if healthChecker != nil {
			healthChecker.RemoveNode(nodeInfo.ID)
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
			if healthChecker != nil {
				healthChecker.AddNode(node)
			}
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
		replacementLocator   Locator
		replacementFactory   NodeFactory
		replacementNodeInfos []NodeInfo
	)

	if locatorChanged || connectionChanged {
		replacementFactory = currentFactory
		if connectionChanged {
			configurableFactory, ok := currentFactory.(ConnectionConfigurableNodeFactory)
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
			c.state.nodeInfos = make(map[string]NodeInfo, len(replacementNodeInfos))
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
func (c *Client) locatorConfigEqual(old, new LocatorConfig) bool {
	return old.PartitionCount == new.PartitionCount &&
		old.ReplicationFactor == new.ReplicationFactor &&
		old.Load == new.Load &&
		old.HashFunction == new.HashFunction
}

// healthCheckerConfigEqual compares two health checker configurations.
func (c *Client) healthCheckerConfigEqual(old, new HealthCheckerConfig) bool {
	return old.Enabled == new.Enabled &&
		old.Interval == new.Interval &&
		old.Timeout == new.Timeout &&
		old.FailureThreshold == new.FailureThreshold &&
		old.RecoveryThreshold == new.RecoveryThreshold
}

// connectionConfigEqual compares two connection configurations.
func (c *Client) connectionConfigEqual(old, new ConnectionConfig) bool {
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
func (c *Client) updateHealthCheckerConfig(newConfig HealthCheckerConfig) error {
	if err := ValidateHealthCheckerConfig(newConfig); err != nil {
		return fmt.Errorf("invalid health checker config: %w", err)
	}

	checker, ok := c.deps.healthChecker.(ConfigurableHealthChecker)
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

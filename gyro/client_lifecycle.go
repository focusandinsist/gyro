package gyro

import (
	"context"
	"fmt"
)

// Start starts the client with service discovery and config watching.
func (c *Client) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.stateMu.RLock()
	run := c.state.run
	c.stateMu.RUnlock()
	if run != nil && run.ctx != nil {
		return fmt.Errorf("client is already running")
	}

	c.stateMu.RLock()
	prepared := c.state.prepared
	c.stateMu.RUnlock()
	if prepared == nil {
		if err := c.initialize(); err != nil {
			return fmt.Errorf("failed to initialize client for start: %w", err)
		}
	}

	runCtx, runCancel := context.WithCancel(ctx)
	c.stateMu.Lock()
	run = &clientRun{ctx: runCtx, cancel: runCancel, pool: c.state.prepared}
	c.state.prepared = nil
	c.state.run = run
	registerConfigWatcher := !c.state.configWatcher
	c.state.configWatcher = true
	c.stateMu.Unlock()

	if registerConfigWatcher {
		c.deps.configManager.AddConfigWatcher(c.handleConfigChange)
	}

	go c.watchServiceNodes(run)
	run.pool.StartHealthMonitoring(runCtx)
	return nil
}

// Stop stops the client.
func (c *Client) Stop() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.stateMu.Lock()
	run := c.state.run
	prepared := c.state.prepared
	c.state.run = nil
	c.state.prepared = nil
	c.state.nodeInfos = nil
	c.stateMu.Unlock()
	if run != nil {
		if run.cancel != nil {
			run.cancel()
		}
		return run.pool.Close()
	}
	if prepared != nil {
		return prepared.Close()
	}
	return nil
}

// GetLocator returns the underlying locator for direct access.
func (c *Client) GetLocator() Locator { return c.getLocator() }

// Close closes the client.
func (c *Client) Close() error { return c.Stop() }

// Package client exposes the dynamic discovery/configuration client. The
// implementation lives in internal/client so the public gyro package remains
// a protocol-neutral contract package.
package client

import (
	"github.com/focusandinsist/gyro/gyro"
	internalclient "github.com/focusandinsist/gyro/internal/client"
)

type Client = internalclient.Client
type Config = internalclient.Config
type ConfigManager = internalclient.ConfigManager
type ConfigWatcher = internalclient.ConfigWatcher
type ConnectionConfig = internalclient.ConnectionConfig
type ClientHealth = internalclient.ClientHealth
type ClientTopologyStatus = internalclient.ClientTopologyStatus

func NewClient(serviceName string, discovery gyro.ServiceDiscovery, configManager *ConfigManager, nodeFactory gyro.NodeFactory, healthChecker gyro.HealthChecker) (*Client, error) {
	return internalclient.NewClient(serviceName, discovery, configManager, nodeFactory, healthChecker)
}

func NewConfigManager(config *Config) *ConfigManager { return internalclient.NewConfigManager(config) }

func DefaultConfig() *Config { return internalclient.DefaultConfig() }

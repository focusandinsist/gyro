# Core Package Map

The `gyro` package is the public core of the project. Protocol-specific
convenience clients live in the sibling `redis` and `grpc` packages.

## Public Entry Points

- `gyro.go`: package overview and the dependency-injected `NewClient` entry.
- `node.go`: runtime node and node-factory interfaces.
- `config.go`: `Config`, defaults, and `ConfigManager`.
- `discovery.go`: service discovery contracts and static discovery.
- `client.go`: `Client` state and dependency model.

## Client Implementation

- `client_lifecycle.go`: start, stop, restart, and close.
- `client_topology.go`: discovery watches, topology reconciliation, and config replacement.
- `client_health.go`: client health and routing summaries.

## Health and Routing

- `health_types.go`: health contracts, configuration, and statistics.
- `health_checker.go`: probing, thresholds, listeners, and worker lifecycle.
- `health_pool.go`: health-aware snapshots, failover, replacement, and ownership.
- `locator.go`: consistent-hash routing and node membership.

## Tests

Tests are colocated because many core tests intentionally exercise unexported
state and lock boundaries. `api_contract_test.go` is the black-box exception;
it uses `package gyro_test` and verifies the public implementation contracts.

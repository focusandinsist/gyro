# Core Package Map

The `gyro` package is the public protocol-neutral core of the project.
Concrete selectors, failure policies, and resource pools live under
`internal/`; protocol-specific convenience clients live in
`adapters/redis` and `adapters/grpc`.

## Public Entry Points

- `gyro.go`: package overview for the public contracts.
- `node.go`: runtime node and node-factory interfaces.
- `config.go`: `Config`, defaults, and `ConfigManager`.
- `discovery.go`: service discovery contracts and static discovery.
- `client.go`: `Client` state and dependency model.

## Client and Runtime Implementation

- `client_lifecycle.go`: start, stop, restart, and close.
- `client_topology.go`: discovery watches, topology reconciliation, and config replacement.
- `client_health.go`: client health and routing summaries.
- `../internal/client`: dynamic client orchestration (migration target).
- `../internal/routed`: adapter shared lifecycle composition.

## Public Contracts

- `health_types.go`: health contracts, configuration, and statistics.
- `health_checker.go` and `health_pool.go` are transitional runtime files and
  will move to `internal/health` as the client lifecycle is migrated.
- `locator.go` is a transitional node-backed locator; new code should depend
  on `Selector`, `HealthView`, `FailurePolicy`, and `Resource` instead.

## Internal Implementations

- `../internal/selector`: consistent-hash and rendezvous selectors.
- `../internal/policy`: primary-only and healthy-candidate policies.
- `../internal/resource`: resource pool ownership and leases.

## Tests

Tests are colocated because many core tests intentionally exercise unexported
state and lock boundaries. `api_contract_test.go` is the black-box exception;
it uses `package gyro_test` and verifies the public implementation contracts.

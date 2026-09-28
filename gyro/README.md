# Core Package Map

The `gyro` package is the public protocol-neutral core of the project.
Concrete selectors, failure policies, and resource pools live under
`internal/`; protocol-specific convenience clients live in
`adapters/redis` and `adapters/grpc`.

## Public Entry Points

- `gyro.go`: package overview for the public contracts.
- `node.go`: runtime node and node-factory interfaces.
- `config.go`: `Config`, defaults, and `ConfigManager`.
- `discovery.go`: legacy discovery contracts; use `discovery/static` for the
  reference implementation.
- `client.go`: transitional client implementation; use the public `client`
  facade for dynamic discovery/configuration.

## Client and Runtime Implementation

- `client_lifecycle.go`: start, stop, restart, and close.
- `client_topology.go`: discovery watches, topology reconciliation, and config replacement.
- `client_health.go`: client health and routing summaries.
- `../client`: public facade backed by `../internal/client`.
- `../internal/routed`: adapter shared lifecycle composition.

## Public Contracts

- `health_types.go`: health contracts, configuration, and statistics.
- `health_checker.go`, `health_pool.go`, and `locator.go` are legacy runtime
  files retained only while downstream code is migrated. New code should use
  the `health`, `internal/health`, `internal/selector`, and `internal/routed`
  seams instead.

## Internal Implementations

- `../internal/selector`: consistent-hash and rendezvous selectors.
- `../internal/policy`: primary-only and healthy-candidate policies.
- `../internal/resource`: resource pool ownership and leases.
- `../internal/health`: probing, threshold state, worker lifecycle, and health snapshots.
- `../internal/client`: dynamic discovery/configuration client implementation.
- `../client`: public facade for the dynamic client.

## Tests

Tests are colocated because many core tests intentionally exercise unexported
state and lock boundaries. `api_contract_test.go` is the black-box exception;
it uses `package gyro_test` and verifies the public implementation contracts.

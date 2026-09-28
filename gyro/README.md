# Core Package Map

The `gyro` package is the public protocol-neutral core of the project.
Concrete selectors, failure policies, and resource pools live under
`internal/`; protocol-specific convenience clients live in
`adapters/redis` and `adapters/grpc`.

## Public Entry Points

- `topology.go`, `routing.go`, `failure_api.go`: public routing and topology contracts.
- `node.go`: runtime node and node-factory interfaces.
- `connection.go`: protocol-neutral connection settings.
- `discovery.go`: discovery contracts; use `discovery/static` for the
  reference implementation.
- `topology_diff_api.go`: the public topology diff value returned by
  `internal/topology`.

## Client and Runtime Implementation

- `../client`: public facade backed by `../internal/client`.
- `../internal/routed`: adapter shared lifecycle composition.

## Public Contracts

- `health_types.go`: health contracts, configuration, and statistics.
- `locator.go` remains the stateful node-membership seam used by the internal
  client and routed runtime. Health checking and pooling are owned by
  `internal/health`.

## Internal Implementations

- `../internal/selector`: consistent-hash and rendezvous selectors.
- `../internal/policy`: primary-only and healthy-candidate policies.
- `../internal/resource`: resource pool ownership and leases.
- `../internal/health`: probing, threshold state, worker lifecycle, and health snapshots.
- `../internal/client`: dynamic discovery/configuration client implementation.
- `../client`: public facade for the dynamic client.

## Tests

Implementation tests live with their owning `internal/` package. The public
contract package has no white-box implementation test suite; cross-package
behaviour is covered under `../test`.

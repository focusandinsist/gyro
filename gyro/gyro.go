// Package gyro provides client-side consistent-hash routing with health-aware
// failover, service discovery, and runtime configuration updates.
//
// Most applications should use a protocol adapter such as gyro/redis or
// gyro/grpc. The core Client API is available when an application needs to
// provide its own discovery, node factory, or health checker.
package gyro

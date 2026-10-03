// Package gyro routes keys to stable members using consistent hashing.
// NewRouter is the direct entry point when callers need a member without
// protocol connections or health checks.
//
// Fixed-address Redis and gRPC clients live in adapters/redis and
// adapters/grpc. Applications with dynamic discovery use the client package.
package gyro

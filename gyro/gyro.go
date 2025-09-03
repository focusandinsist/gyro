// Package gyro provides client-side sharding middleware for distributed services.
package gyro

func NewRedisCluster(addresses []string) (*RedisClient, error) {
	return NewRedisClient(addresses, nil)
}

func NewGRPCCluster(addresses []string) (*GRPCClient, error) {
	return NewGRPCClient(addresses, nil)
}

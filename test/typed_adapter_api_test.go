package test

import (
	"context"

	grpcadapter "gyro/adapters/grpc"
	redisadapter "gyro/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

var (
	_ func(*redisadapter.Client, context.Context, string) (*goredis.Client, error)           = (*redisadapter.Client).GetClientForKey
	_ func(*redisadapter.Client, context.Context, string, int) ([]*goredis.Client, error)    = (*redisadapter.Client).GetClientsForReplicas
	_ func(*redisadapter.Client) map[string]*goredis.Client                                  = (*redisadapter.Client).GetAllClients
	_ func(*redisadapter.Client, context.Context, string) (*redisadapter.ClientLease, error) = (*redisadapter.Client).BorrowClientForKey
	_ func(*grpcadapter.Client, context.Context, string) (*grpc.ClientConn, error)           = (*grpcadapter.Client).GetClientForKey
	_ func(*grpcadapter.Client, context.Context, string, int) ([]*grpc.ClientConn, error)    = (*grpcadapter.Client).GetClientsForReplicas
	_ func(*grpcadapter.Client) map[string]*grpc.ClientConn                                  = (*grpcadapter.Client).GetAllClients
	_ func(*grpcadapter.Client, context.Context, string) (*grpcadapter.ClientLease, error)   = (*grpcadapter.Client).BorrowClientForKey
)

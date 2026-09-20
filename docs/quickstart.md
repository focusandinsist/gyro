# Gyro 快速开始

> 本文所有代码示例都已经在仓库当前版本上实际跑通(参见仓库根目录 [README.md](../README.md))。之前版本的快速开始文档里使用的 `client.Set/Get`、`gyro.NewRedisCommand`、`gyro.NewDynamicClient` 等 API 已经不存在,如果你看到其它文档里还有这些写法,以本篇为准。

## 安装

安装 Gyro：

```bash
go get github.com/focusandinsist/gyro/gyro@latest
```

## 基础概念

- **Node**:集群中的一个后端服务实例(一个 Redis 实例、一个 gRPC 服务实例等)。
- **Locator**:基于一致性哈希管理节点集合,回答"这个 key 该找哪个 Node"。
- **HealthChecker**:主动探测节点是否健康。
- **Client**(`gyro.Client`):把 Locator、HealthChecker、ServiceDiscovery、ConfigManager 组合起来的顶层入口。

## Redis 集群示例

### 1. 最简单的用法:固定地址

```go
package main

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	redisadapter "github.com/focusandinsist/gyro/gyro/redis"
)

func main() {
	client, err := redisadapter.NewRedisCluster([]string{
		"127.0.0.1:6379",
		"127.0.0.1:6380",
		"127.0.0.1:6381",
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	// 按 key 一致性哈希路由到对应节点,拿到真正的 *redis.Client
	native, err := client.GetClientForKey(ctx, "user:1001")
	if err != nil {
		panic(err)
	}
	redisConn := native.(*goredis.Client)

	redisConn.Set(ctx, "user:1001", "Alice", 0)
	value, _ := redisConn.Get(ctx, "user:1001").Result()
	fmt.Println(value)
}
```

### 2. 自定义配置

```go
config := redisadapter.DefaultRedisClientConfig()

// 调整一致性哈希参数
config.Locator.PartitionCount = 512
config.Locator.ReplicationFactor = 40
config.Locator.Load = 1.5

// 调整连接参数(会被映射到 go-redis 的 PoolSize/DialTimeout 等)
config.Connection.MaxIdleConns = 20
config.Connection.MaxActiveConns = 200
config.Connection.ConnectTimeout = 3 * time.Second

client, err := redisadapter.NewRedisClient([]string{
	"redis1:6379", "redis2:6379", "redis3:6379",
}, config)
```

### 3. 读副本

```go
// 从 2 个副本节点读取,用于读扩展/高可用场景
clients, err := client.GetClientsForReplicas(ctx, "user:1001", 2)
if err != nil {
	panic(err)
}
for _, c := range clients {
	conn := c.(*goredis.Client)
	_ = conn // 各自读取,自行聚合结果
}
```

## gRPC 服务集群示例

用法与 Redis 完全对称,只是拿到的原生客户端是 `*grpc.ClientConn`:

```go
package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	grpcadapter "github.com/focusandinsist/gyro/gyro/grpc"
	pb "your/generated/proto/package"
)

func main() {
	client, err := grpcadapter.NewGRPCCluster([]string{
		"grpc1:8080",
		"grpc2:8080",
		"grpc3:8080",
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	native, err := client.GetClientForKey(ctx, "user:1001")
	if err != nil {
		panic(err)
	}
	conn := native.(*grpc.ClientConn)

	userClient := pb.NewUserServiceClient(conn)
	resp, err := userClient.GetUser(ctx, &pb.GetUserRequest{UserId: "1001"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("User: %+v\n", resp)
}
```

**健康检查说明**:gRPC 适配器的健康检查走标准的 `grpc.health.v1.Health` 协议。如果你的服务端注册了 `google.golang.org/grpc/health` 提供的健康检查服务,Gyro 会用它判断节点是否健康;如果没有注册(`Unimplemented`),Gyro 会退化成只看连接的连通性状态,不会因为你没实现健康检查协议就把节点全部判定为不健康。

## 动态服务发现 + 配置热更新

固定地址的 `NewRedisCluster`/`NewGRPCCluster` 适合节点不怎么变化的场景。如果节点会动态增减(接 Kubernetes Endpoints、注册中心等),用更底层的 `gyro.NewClient`:

```go
package main

import (
	"context"

	"github.com/focusandinsist/gyro/gyro"
	redisadapter "github.com/focusandinsist/gyro/gyro/redis"
)

func main() {
	// 把 StaticServiceDiscovery 换成你自己的 ServiceDiscovery 实现
	// 即可接入真实的注册中心/K8s Endpoints watch
	discovery := gyro.NewStaticServiceDiscovery([]string{
		"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381",
	})

	configManager := gyro.NewConfigManager(gyro.DefaultClientConfig())
	nodeFactory := redisadapter.NewRedisNodeFactory()
	healthChecker := gyro.NewDefaultHealthChecker(gyro.DefaultHealthCheckerConfig())

	client, err := gyro.NewClient("user-cache", discovery, configManager, nodeFactory, healthChecker)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		panic(err)
	}

	native, err := client.GetClientForKey(ctx, "user:1001")
	if err != nil {
		panic(err)
	}
	_ = native
}
```

节点列表或配置发生变化时,`Client` 会对比新旧节点/配置做增量应用(只增删变化的部分),不会推倒重建整个连接池,细节见 [architecture.md](./architecture.md) 和 [gyro-feature-documentation-20260814.md](./gyro-feature-documentation-20260814.md)。

## 健康检查和监控

```go
// 查看当前健康状态统计
stats := client.GetStats()
fmt.Printf("Total: %d, Healthy: %d, Unhealthy: %d\n",
	stats.TotalNodes, stats.HealthyNodes, stats.UnhealthyNodes)

// 查看客户端自身(服务发现连接)的健康状态
health := client.Health()
fmt.Printf("ServiceDiscoveryHealthy: %v\n", health.ServiceDiscoveryHealthy)
```

## 最佳实践

### 1. 键设计

```go
// 好的键设计 - 包含分片键,能均匀分布
userKey := fmt.Sprintf("user:%s", userID)
sessionKey := fmt.Sprintf("session:%s", sessionID)

// 避免的键设计 - 所有请求都会落到同一个节点
globalCounterKey := "global:counter"
```

### 2. 资源管理

```go
client, err := redisadapter.NewRedisCluster(addresses)
if err != nil {
	panic(err)
}
defer client.Close() // 关闭所有底层连接
```

## 下一步

- 阅读 [架构设计](./architecture.md) 了解一致性哈希路由、健康检查、增量重平衡的内部实现
- 阅读 [Gyro 功能文档](./gyro-feature-documentation-20260814.md) 了解当前能力、适用场景和已知限制

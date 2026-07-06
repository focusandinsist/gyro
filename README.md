# Gyro

Gyro 是一个基于一致性哈希的 Go 客户端侧分片中间件（client-side sharding）。它不是一个独立运行的代理进程，而是以库的形式嵌入到应用里：应用拿一个 key 向 Gyro 要节点，Gyro 在内部完成一致性哈希路由、健康检查和故障转移,应用不需要关心背后有多少台机器、机器何时增减。

## 核心特性

- **一致性哈希 + 固定分区**：默认 271 个分区、20 个虚拟节点,新增/删除节点时只重新分配受影响的分区(增量重平衡),而不是推倒重建整张哈希环。
- **健康检查**:内置主动探测(可配置故障/恢复阈值、并发 worker pool),另有一套基于 Gossip 协议的可选健康检查实现(默认关闭,适合节点规模较大、想摊薄探测开销的场景)。
- **服务发现 + 配置热更新**:节点列表或配置变化时做增量 diff 应用,不整体重建连接。
- **协议适配层**:核心抽象是 `Node`/`Locator`/`HealthChecker`,内置 Redis(`github.com/redis/go-redis/v9`)和 gRPC(`google.golang.org/grpc`)两个适配器,拿到的是真实的原生客户端(`*redis.Client` / `*grpc.ClientConn`),接口全部对外暴露,不做二次封装。

## 快速开始

```go
package main

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	redisadapter "gyro/gyro/redis"
)

func main() {
	// 三个 Redis 节点,按一致性哈希分片
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

	// 按 key 路由到对应节点,拿到的是原生 *redis.Client,直接用 go-redis 的完整 API
	native, err := client.GetClientForKey(ctx, "user:123")
	if err != nil {
		panic(err)
	}
	redisConn := native.(*goredis.Client)

	if err := redisConn.Set(ctx, "user:123", "Alice", 0).Err(); err != nil {
		panic(err)
	}
	val, _ := redisConn.Get(ctx, "user:123").Result()
	fmt.Println(val)
}
```

### 需要动态服务发现 / 配置热更新时

上面的 `NewRedisCluster` 是固定地址的便捷入口。如果节点列表会变化(比如接 Kubernetes Endpoints、注册中心),用更底层的依赖注入式 API:

```go
discovery := gyro.NewStaticServiceDiscovery([]string{
	"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381",
}) // 换成自己的 ServiceDiscovery 实现即可接入真实注册中心

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

native, err := client.GetClientForKey(ctx, "user:123")
```

gRPC 用法结构上完全对称,把 `redisadapter` 换成 `gyro/gyro/grpc`,拿到的原生客户端是 `*grpc.ClientConn`,自己用生成的 stub(如 `pb.NewUserServiceClient(conn)`)调用即可。gRPC 健康检查走的是标准 `grpc.health.v1.Health` 协议;如果后端服务没有注册这个健康检查服务,Gyro 会回退到用连接的连通性状态判断,不会因此把所有节点都判为不健康。

## 已知限制

- go.mod 的 module path 是 `gyro`,与实际仓库地址 `github.com/focusandinsist/gyro` 不一致,目前还不能通过 `go get github.com/focusandinsist/gyro` 直接拉取,只能本地 clone 后作为同名 module 使用。
- gRPC 适配器目前只支持不加密的传输(`insecure.NewCredentials()`),没有暴露 TLS 配置。
- Gossip 健康检查是实验性的,默认关闭,没有测试覆盖,不建议在生产环境启用。

## 项目结构

```
gyro/
├── gyro/                  # 核心:Locator(一致性哈希路由)、HealthChecker、ConfigManager、ServiceDiscovery
│   ├── redis/             # Redis 适配器(go-redis)
│   ├── grpc/              # gRPC 适配器(grpc-go)
│   └── health/gossip/     # 可选的 Gossip 健康检查(默认关闭)
├── test/integration/      # 基于 fake server 的集成测试
└── docs/                  # 详细文档
```

一致性哈希算法本身已经抽成独立的库:[focusandinsist/consistent-go](https://github.com/focusandinsist/consistent-go)。

## 文档

更详细的架构设计、配置说明、各类专题文档见 [docs/](docs/README.md)。

## License

[MIT](LICENSE)

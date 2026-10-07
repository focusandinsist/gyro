# Gyro

Gyro 是嵌入 Go 应用的一致性哈希路由库。最小用法是向根包提供成员和 key，取得对应成员；需要连接时，再使用 Redis 或 gRPC 适配器。动态服务发现和配置更新由 `client` 包提供。

## 按 Key 选节点

只需要选节点时，导入根包 `gyro`。`Router` 不创建连接、不探测健康，也不需要关闭。

```go
package main

import (
	"context"
	"fmt"

	"gyro"
)

func main() {
	router, err := gyro.NewRouter([]gyro.Member{
		{ID: "worker-a", Endpoints: []gyro.Endpoint{{Address: "worker-a:8080"}}},
		{ID: "worker-b", Endpoints: []gyro.Endpoint{{Address: "worker-b:8080"}}},
	}, gyro.DefaultLocatorConfig())
	if err != nil {
		panic(err)
	}
	member, err := router.Route(context.Background(), "task-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(member.ID, member.Endpoints[0].Address)
}
```

运行：`go run ./examples/router`。`Candidates(ctx, key, count)` 返回包含首选成员的有序候选；`ReplaceMembers(ctx, members)` 原子替换成员集合。成员 ID 是哈希身份，同一集群的调用方应使用相同的 ID 和哈希配置。

## 使用原生客户端

固定地址 Redis 的调用片段（在已有 `ctx` 的函数内）：

```go
client, err := redisadapter.NewCluster([]string{"127.0.0.1:6379", "127.0.0.1:6380"})
if err != nil {
	return err
}
defer client.Close()

lease, err := client.BorrowClientForKey(ctx, "user:123")
if err != nil {
	return err
}
defer lease.Release()
redisClient := lease.Client() // *redis.Client
```

导入路径是 `gyro/adapters/redis`，完整的无服务构造与候选查询示例在 [examples/redis/main.go](examples/redis/main.go)。gRPC 使用 `gyro/adapters/grpc` 的相同构造和借用流程，`lease.Client()` 返回 `*grpc.ClientConn`，由应用创建自己的服务 stub；见 [examples/grpc/main.go](examples/grpc/main.go)。adapter 负责健康探测与连接关闭，借用的客户端应在使用完毕后释放租约。

## 动态成员

节点列表需要 watch 或配置热更新时，使用 `gyro/client` 注入 `ServiceDiscovery`、`ConfigManager`、`NodeFactory` 和 `HealthChecker`，调用 `Start(ctx)` 后读取路由结果，最后调用 `Close()`。完整示例在 [examples/dynamic/main.go](examples/dynamic/main.go)。`client.Client` 当前返回节点元数据；它没有返回 Redis/gRPC 原生客户端的 `GetClientForKey` 方法。

## 目录

- 根目录 `package gyro`：`Router`、成员/拓扑类型、选路契约和公共错误。
- `adapters/redis`、`adapters/grpc`：协议连接、健康探测和类型化客户端。
- `client`、`discovery/static`、`health`：动态成员入口及其可组合依赖。
- `internal`：拓扑、健康、策略、资源和运行时实现。
- [docs/quickstart.md](docs/quickstart.md)：按场景展开的可运行示例；[docs/architecture.md](docs/architecture.md)：当前包职责与运行边界。

一致性哈希由 [consistent-go](https://github.com/focusandinsist/consistent-go) 实现。Gyro 不执行业务请求重试、数据复制或数据迁移；切换到健康候选也不代表数据已同步。当前 gRPC 适配器默认使用不加密传输，没有 TLS 配置入口。

## 验证

```bash
go test ./...
go vet ./...
```

[MIT](LICENSE)

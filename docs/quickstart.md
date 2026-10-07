# 快速开始

以下命令从仓库根目录运行。当前模块在 `go.mod` 中声明为 `gyro`，示例保持 `gyro/...` import 路径。

## 1. 纯路由

```bash
go run ./examples/router
```

[完整代码](../examples/router/main.go)只导入根包。`gyro.NewRouter(members, gyro.DefaultLocatorConfig())` 创建路由器，`Route(ctx, key)` 返回 `gyro.Member`，`Candidates(ctx, key, count)` 返回按哈希顺序排列的成员。成员更新使用 `ReplaceMembers(ctx, members)`；新集合完整准备后才会发布。纯路由不做健康检查，且不持有需要关闭的连接。

成员 ID 决定哈希归属。不同进程要得到相同的结果，必须使用相同的 ID 集合与 `LocatorConfig`。地址可以改变而不改变 ID；业务负责解释返回的 endpoint。

## 2. Redis

```bash
go run ./examples/redis
```

[完整代码](../examples/redis/main.go)创建两个固定地址节点，读取一个 key 的候选客户端地址，无需运行 Redis 服务。执行真实 Redis 命令时，使用租约确保选中的原生客户端在调用期间不会因成员变化而关闭。以下是已有 `ctx` 的函数内调用片段：

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
err = lease.Client().Set(ctx, "user:123", "Alice", 0).Err()
```

`redisadapter` 的 import 路径是 `gyro/adapters/redis`，`lease.Client()` 的类型是 `*redis.Client`。`NewClient(addresses, config)` 可自定义 `config.Locator`、`config.HealthChecker` 和 `config.Connection`。Redis 的连接设置会映射到 go-redis 的超时与连接池选项。

## 3. gRPC

```bash
go run ./examples/grpc
```

[完整代码](../examples/grpc/main.go)只读取候选连接的 target，不需要 gRPC 服务。应用调用服务时，同样借用连接并在调用后释放租约。以下是已有 `ctx` 的函数内调用片段：

```go
client, err := grpcadapter.NewCluster([]string{"127.0.0.1:50051", "127.0.0.1:50052"})
if err != nil {
	return err
}
defer client.Close()

lease, err := client.BorrowClientForKey(ctx, "user:123")
if err != nil {
	return err
}
defer lease.Release()
conn := lease.Client() // *grpc.ClientConn；用自己的生成代码创建服务 stub
```

`grpcadapter` 的 import 路径是 `gyro/adapters/grpc`。gRPC 健康探测优先使用标准 `grpc.health.v1.Health`；服务未实现该接口时会参考连接状态。当前只提供不加密连接；通用 `ConnectionConfig` 中的 `MaxActiveConns`、`MaxIdleConns` 对 gRPC 不生效。

两个 adapter 的 `GetClientsForReplicas` 返回未经过健康过滤、也未持有租约的候选快照。健康感知选择使用 `BorrowClientForKey`。Gyro 不重试业务 RPC，也不保证切换后的节点拥有相同数据。

## 4. 动态发现和配置

```bash
go run ./examples/dynamic
```

[完整代码](../examples/dynamic/main.go)把 `gyro/discovery/static`、`gyro/health` 和 Redis 的 `NodeFactory` 注入 `gyro/client`。构造时准备初始资源，`Start(ctx)` 启动 watch 和健康监控，`Close()` 停止并释放资源。要接入注册中心，实现根包的 `ServiceDiscovery` 接口，并提供跨更新稳定的节点 ID。

`client.Client.GetLocator().GetReplicas(ctx, key, count)` 返回候选 `gyro.Node`，不提供原生协议客户端，也不做健康过滤；示例只打印其 ID 和地址。`GetNodeForKey` 通过当前健康策略取节点，在探测完成前或无健康候选时可能返回错误。连接或 locator 配置变化可通过 `ConfigManager.UpdateConfig` 触发资源替换。

## 下一步

- [架构设计](./architecture.md)：三种入口的职责和资源生命周期。
- [合约测试](./testing-contracts.md)：当前测试覆盖与真实协议测试边界。

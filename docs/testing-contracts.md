# 测试合约与运行方式

测试覆盖根包纯路由、动态 Client、健康切换、原生客户端返回和资源关闭。普通测试不依赖运行中的 Redis 或 gRPC 服务。

## 1. 直接运行的测试入口

在仓库根目录运行：

```bash
go test .
go test ./adapters/redis
go test ./adapters/grpc
go test ./test
go test ./examples/...
go test ./...
```

适配器合约测试也可以单独运行：

```bash
go test ./adapters/redis -run 'TestRedisConvenienceClientUsesHealthAwareFailover|TestRedisNodeDoesNotGateNativeClientOnSingleFailedProbe'
go test ./adapters/grpc -run 'TestGRPCConvenienceClientUsesHealthAwareFailover|TestGRPCNodeDoesNotGateNativeClientOnSingleFailedProbe'
```

本地还可运行：

```bash
go vet ./...
go test -race ./...
```

`go test -race` 需要可用的 C 编译器；未执行时不能将 race 检查记为通过。

## 2. 根包与动态 Client

`test/router_test.go` 从根包构造 `Router`，覆盖稳定选路、成员替换、并发读取和错误路径。`test/` 中的路由、拓扑、资源池与 Client 测试覆盖动态发现和生命周期。公开使用示例位于 `examples/router`、`examples/redis`、`examples/grpc` 与 `examples/dynamic`，全部只导入公开包。

## 3. Redis 和 gRPC 共同合约

Redis 与 gRPC 适配器都必须满足以下行为：

1. 固定地址构造函数为每个地址创建一个节点，并把节点放入健康感知路由池。
2. `GetClientForKey(ctx, key)` 返回对应协议的原生客户端，而不是 `gyro.Node`。
3. `GetClientsForReplicas` 返回 ring 候选中的原生客户端；返回数量受实际节点数限制。
4. 健康 checker 达到失败阈值后，主节点不健康时应选择其他健康候选；主节点恢复达到恢复阈值后，应允许路由回到主节点。
5. 单次 probe 失败不会让适配器节点自行隐藏 native client；阈值状态由 `HealthAwarePool` 决定。
6. `Close` 必须关闭健康监控和底层连接，并且重复调用不会 panic。
7. 无效健康配置和连接配置应在构造阶段返回错误，而不是静默忽略。

这些行为由 `adapters/redis/redis_test.go`、`adapters/grpc/grpc_test.go` 和 `test/adapter_routing_baseline_test.go` 等测试覆盖。注入式连接测试使用可控 connection，不需要启动 Redis 或 gRPC 服务，因此不等于真实协议握手已经通过。

## 4. 真实协议测试边界

仓库当前不保留固定端口 fake server 集成树，也不把真实 Redis/gRPC 服务作为普通 `go test ./...` 的隐式依赖。需要验证真实协议时，应由 CI 或开发环境显式提供服务地址，并单独运行合约测试；这类测试必须：

- 使用临时端口或服务容器，不占用固定端口；
- 通过环境变量传入地址，缺少地址时明确跳过而不是伪造通过；
- 至少验证一次真实 native client 调用、健康失败、恢复和 Close；
- 不把 `*redis.Client` 或 `*grpc.ClientConn` 反向断言成 `gyro.Node`；
- 不依赖固定 `Sleep`，等待可观察的健康状态或使用带截止时间的轮询。

在这样的测试基础设施建立前，文档只承诺“注入式适配器契约测试通过”，不宣称协议级端到端覆盖完整。

## 5. 测试文件边界

- `test/*_test.go`：纯路由、核心组件、动态 Client 及公开入口的回归测试。
- `adapters/redis/*_test.go`、`adapters/grpc/*_test.go`：适配器的注入式合约测试。
- `examples/*/main.go`：公开入口的可编译程序；不依赖本机服务即可运行。

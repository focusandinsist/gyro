# 架构设计

Gyro 在业务进程内根据 key 选择成员。根包提供协议无关的纯路由；协议适配器负责连接和健康探测；动态 `client` 负责 discovery watch 与配置更新。一致性哈希计算依赖 `consistent-go`。

## 三种入口

| 场景 | 公开入口 | 结果 | 生命周期 |
| --- | --- | --- | --- |
| 只选节点 | `gyro.NewRouter` | `gyro.Member` 或有序候选 | 不持有连接；成员更新用 `ReplaceMembers` |
| 固定地址协议客户端 | `adapters/redis.NewCluster` / `adapters/grpc.NewCluster`，或各自 `NewClient` | 先取得适配器 `*Client`，再借用原生 `*redis.Client` / `*grpc.ClientConn` | 借用后 `Release`，使用完毕 `Close` |
| 动态服务发现 | `client.NewClient` | `gyro.Node`、locator 候选与拓扑状态 | `Start`、`Stop` / `Close` |

根包 `Router` 根据稳定成员 ID 构建哈希环，查询不访问网络、不检查健康。`ReplaceMembers` 准备新集合后原子发布；并发查询只看到旧集合或新集合。输入与返回的成员元数据会拷贝，调用方修改属性 map 不会改变内部快照。空 key、重复或空成员 ID 等错误由根包的公开错误表示。

## 固定地址适配器

`adapters/redis` 与 `adapters/grpc` 为每个地址创建一个节点，使用同一套根包一致性哈希选择逻辑。`internal/routed` 负责共用的节点构建、失败回滚、健康监控和关闭；适配器各自拥有连接设置、原生类型和探测方式。两者共用根包 `RoutingConfig` 的 locator 与健康配置，连接参数仍由协议解释。Redis 使用连接池设置，gRPC 忽略 `MaxActiveConns` 与 `MaxIdleConns`，当前默认使用 insecure credentials。

`BorrowClientForKey` 通过 `internal/routing.HealthAwarePool` 选择健康候选并发放租约；调用方用完后必须 `Release`。`GetClientForKey` 只返回当前原生客户端，不保留长期租约。`GetClientsForReplicas` 返回 selector 顺序的候选快照，不做健康过滤，也不保护连接免受后续拓扑变更影响。

固定地址入口按传入数组位置生成 `redis-1`、`grpc-1` 等 ID；不同进程必须保持地址顺序一致，才能保持相同的地址到 ID 映射。仅使用根包 `Router` 时，调用方直接提供稳定的成员 ID。

## 动态 Client

`client.NewClient` 接收服务名、`gyro.ServiceDiscovery`、`ConfigManager`、`gyro.NodeFactory` 和 `gyro.HealthChecker`。构造时发现初始拓扑并准备节点；`Start(ctx)` 启动 watch 和健康监控，`Stop` / `Close` 停止运行并关闭节点。同一 Client 支持重新 `Start`。

`discovery/static` 是内存中的完整拓扑参考实现；实际注册中心需实现 `Discover` 与 `Watch`。Client 根据稳定节点 ID 处理增删与元数据变化，locator 或 connection 配置变化时准备替换资源，再发布新资源。`client.Client` 公开节点元数据和 locator，不提供 Redis/gRPC 原生客户端访问方法。

## 核心边界

```text
gyro.Router -> 根包一致性哈希选择 -> gyro.Member
adapter -> internal/routed -> internal/routing -> 根包一致性哈希选择
client -> discovery/config/health -> internal/routing
```

根包不导入 adapter、动态 client 或依赖根包的 `internal` 实现，因此没有反向依赖。`HealthChecker` 维护观察状态，`FailurePolicy` 决定候选是否可用，资源池管理节点和租约；健康切换只改变路由选择，不执行请求重试、数据复制、事务或迁移。具体使用代码见[快速开始](./quickstart.md)，当前测试边界见[合约测试](./testing-contracts.md)。

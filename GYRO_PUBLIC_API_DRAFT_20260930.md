# Gyro 根包最小公开 API 草案

日期：2026-09-30  
依据：此前约定的 `GYRO_PUBLIC_ENTRY_PLAN_20260930.md` 步骤 1；该计划文件当前不在工作树中  
状态：步骤 1 的历史快照；步骤 2、3 已完成，步骤 4 已抽取共享路由配置和固定地址 runtime

## 1. 现状和调用者

步骤 1 时模块名是 `gyro`，根目录没有 Go 包。原 `gyro/` 导出的符号按职责如下；
步骤 2 已将这些符号移到根目录：

| 职责 | 当前公开符号 | 现有调用者 |
| --- | --- | --- |
| 拓扑 | `Endpoint`、`Member`、`Revision`、`TopologySnapshot`、`TopologyStore`、`TopologyDiff` | `internal/topology`、`internal/selector`、`client`、测试和纯路由示例 |
| 选路 | `RouteRequest`、`Candidate`、`CandidateSet`、`RouteDecision`、`Selector`、`Locator`、`LocatorConfig`、`LocatorStats`、`DefaultLocatorConfig` | `internal/selector`、`internal/routing`、两个 adapter、`client`、测试 |
| 健康与故障策略 | `HealthStatus`、`Unknown`、`Healthy`、`Unhealthy`、`HealthView`、`HealthChecker`、`ConfigurableHealthChecker`、`HealthCheckerConfig`、`FailurePolicy`、`DefaultHealthCheckerConfig`、`ValidateHealthCheckerConfig`、`NodeHealthStats`、`HealthListener`、`HealthAwarePoolStats` | `internal/health`、`internal/policy`、`internal/routing`、两个 adapter、`health`、`client`、测试 |
| 资源与连接 | `Resource`、`ResourceFactory`、`ResourceHandle`、`ConnectionConfig`、`DefaultConnectionConfig` | `internal/resource`、`internal/routing`、两个 adapter、`client`、测试 |
| 服务发现与旧节点模型 | `NodeInfo`、`TopologyStream`、`TopologySource`、`ServiceDiscovery`、`ServiceRegistrar`、`NewServiceTopologySource`、`Node`、`NodeFactory`、`ConnectionConfigurableNodeFactory` | `internal/topology`、`internal/routed`、`client`、两个 adapter、测试 |

现有公开错误完整清单：`ErrNilContext`、`ErrInvalidSnapshot`、
`ErrStaleRevision`、`ErrRevisionConflict`、`ErrIncomparableRevision`、
`ErrInvalidRequest`、`ErrNoMembers`、`ErrSelectionMismatch`、
`ErrLocatorClosed`、`ErrResourceUnavailable`、`ErrFailoverNotAllowed`、
`ErrNoEligibleCandidate`。其中纯路由最小入口直接使用前八项中的
`ErrNilContext`、`ErrInvalidSnapshot`、`ErrInvalidRequest` 和 `ErrNoMembers`。

固定地址用户调用 `adapters/redis.NewClient`/`NewCluster` 或
`adapters/grpc.NewClient`/`NewCluster`；动态发现用户调用 `client.NewClient`。
步骤 1 时 `internal/selector.NewConsistentHashSelector` 是纯选路实现，
`internal/routing.NewLocator` 和两个 adapter 都使用它。外部用户不能导入
`internal/selector`，因此步骤 3 将该实现和可用的 `Router` 入口迁到了根包。

## 2. 冻结的最小 API

当前公开入口采用以下签名。继续使用现有 `Member`、`Endpoint`
和 `LocatorConfig`，避免为同一配置引入第二种公开类型。

```go
package gyro

func NewRouter(members []Member, config LocatorConfig) (*Router, error)

func (r *Router) Route(ctx context.Context, key string) (Member, error)
func (r *Router) Candidates(ctx context.Context, key string, count int) ([]Member, error)
func (r *Router) ReplaceMembers(ctx context.Context, members []Member) error
```

默认配置通过现有 `DefaultLocatorConfig()` 获取。`Route` 返回一致性哈希首选成员；
`Candidates` 返回同一次选择中的有序成员，包含首选成员，`count` 超过成员数时返回
全部成员。纯路由不探测健康、不执行故障切换、不创建或关闭协议资源，也不需要
`Start`/`Close`。高阶 `Selector`、`FailurePolicy` 等接口保留为独立契约，
不增加到最小构造函数的参数中。

## 3. 输入、错误和并发语义

| 情况 | 约定 |
| --- | --- |
| 构造时成员列表为空 | `NewRouter` 返回 `ErrNoMembers`；不返回可用路由器 |
| 更新时成员列表为空 | `ReplaceMembers` 允许发布空拓扑；随后 `Route`/`Candidates` 返回 `ErrNoMembers` |
| 空 key、`count <= 0` | 返回 `ErrInvalidRequest` |
| 空成员 ID、重复成员 ID | 返回 `ErrInvalidSnapshot`；更新失败时保持原快照 |
| `nil` context | 返回 `ErrNilContext` |
| 已取消或超时的 context | 返回 `ctx.Err()`，可用 `errors.Is` 判断 |
| 无效哈希配置 | `NewRouter` 返回底层配置错误；具体错误文案不作为契约 |

成员 ID 是哈希身份，必须在节点地址变化和输入重排时保持稳定。相同配置、成员 ID
集合和 key 的选路结果必须一致；`Member` 的 address/attributes 变化不能改变选路
身份。构造和替换时深拷贝成员、端点和属性 map；`Route`、`Candidates` 返回独立副本。
调用方改动输入或返回值不能改变路由器内部状态。`ReplaceMembers` 原子发布完整成员
集合；并发查询只看到旧集合或新集合，不看到部分更新。失败不改变当前集合。

错误以 `errors.Is` 为判断方式，不依赖字符串。检查顺序为 `nil` context、context
取消、请求参数、拓扑状态；构造函数无 context。`Router` 不持有外部资源，故不定义
`Close`。若将来增加快照 revision，应以新能力扩展，不要求最小路径先创建
`TopologySnapshot`。

## 4. 当前行为基线

默认 `LocatorConfig` 是 271 个分区、20 个虚拟节点、负载系数 1.25、`xxhash`。
现有 selector 对成员 ID 排序后入环，同一成员集合的输入顺序不影响结果。
`test/consistent_hash_selector_test.go` 已固定 `routing-regression-key` 在
`node-a`/`node-b`/`node-c` 下的候选顺序为 `node-b`、`node-c`、`node-a`。

两个 adapter 当前按地址数组位置生成 `redis-1`/`redis-2`/... 和
`grpc-1`/`grpc-2`/... 的节点 ID；重排地址数组会改变 ID 与地址的对应关系。
它们的 `GetClientsForReplicas` 返回 selector 候选顺序，且不做健康过滤；
`GetClientForKey` 则使用 `HealthyCandidate{AllowUnknown: true}`，主节点不健康时
可以切到候选节点。这两种语义不能在根包 `Route` 中混为一谈。
`test/adapter_routing_baseline_test.go` 固定地址输入 `a`、`b`、`c` 和 key
`routing-regression-key` 下的候选地址顺序：Redis 为 `c`、`a`、`b`，gRPC 为
`b`、`a`、`c`。两者的节点 ID 前缀不同，因此结果不要求相同。

## 5. 依赖方向和后续步骤边界

```text
根包 gyro -> 标准库、consistent-go
internal/selector、internal/routing、internal/policy 等 -> 根包 gyro
client、adapters/redis、adapters/grpc -> 根包 gyro + 各自需要的 internal 包
```

步骤 1 时 `internal/selector` 导入 `gyro/gyro`；步骤 2 后改为导入根包 `gyro`。
根包不能调用 `internal/selector`，否则会形成导入环。步骤 3 已将默认一致性哈希
选择逻辑迁到根包，`internal/routing` 现在复用 `gyro.NewConsistentHashSelector`。
步骤 4 才收敛 adapter 重复。

步骤 4 现在由根包 `RoutingConfig` 统一 `Locator` 与 `HealthChecker` 设置；Redis、gRPC
和动态 `client.Config` 嵌入该配置，仍各自持有 `ConnectionConfig` 和 typed native
client。固定地址 adapter 通过 `internal/routed.NewFixedWithPolicy` 共享地址校验、
健康配置校验和 runtime 组装；协议连接创建与健康探测没有跨 adapter 合并。

完成检查：`go test ./...`、`go vet ./...`；步骤 3 已增加只导入根包的
`examples/router`，并通过仓库外部临时模块运行验证。

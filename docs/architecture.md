# Gyro 架构设计

> 本文描述的是当前代码库(`gyro/`)里的实际结构。部分类型名称在开发过程中发生过重命名(例如 `ConsistentPool` → `ConsistentLocator`,`pool.go` → `locator.go`),一致性哈希算法本身也已经抽成独立库 [focusandinsist/consistent-go](https://github.com/focusandinsist/consistent-go),不再随本仓库分发。如果你看到别的文档里还提到旧名字,以本文和源码为准。

## 系统概览

Gyro 是一个客户端侧的分片中间件，它将复杂的分布式系统路由逻辑封装在客户端库中，为开发者提供了一个简单、统一的接口来操作分布式服务集群。

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │   Application   │    │   Application   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Gyro Client    │    │  Gyro Client    │    │  Gyro Client    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Backend 1     │    │   Backend 2     │    │   Backend 3     │
│  (Redis/gRPC)   │    │  (Redis/gRPC)   │    │  (Redis/gRPC)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 核心组件

### 1. 接口层 (interfaces.go)

定义了系统的核心抽象：

- **Node**: 代表集群中的一个后端服务节点
- **Pool**: 管理节点集合，提供基于一致性哈希的路由
- **Client**: 为应用提供统一的服务接口
- **Command**: 封装要执行的命令
- **HealthChecker**: 提供健康检查能力
- **ServiceDiscovery**: 提供服务发现能力

### 2. 路由实现 (locator.go)

`ConsistentLocator` 是核心的一致性哈希路由实现:

```go
type ConsistentLocator struct {
    mu     sync.RWMutex
    nodes  map[string]Node        // 节点映射
    ring   *consistent.Consistent // 一致性哈希环(来自 consistent-go 独立库)
    config LocatorConfig          // 配置
}
```

**关键特性**:
- 基于一致性哈希的节点选择
- 支持动态添加/删除节点
- 自动故障转移到副本节点
- 线程安全的并发访问

### 3. 健康检查 (health.go)

`DefaultHealthChecker` 提供了完整的健康检查机制：

```go
type DefaultHealthChecker struct {
    mu              sync.RWMutex
    config          HealthCheckConfig
    nodeStats       map[string]*NodeHealthStats
    healthListeners []HealthListener
}
```

**功能**:
- 定期健康检查
- 故障阈值和恢复阈值
- 健康状态变化通知
- 统计信息收集

### 4. 服务适配器

#### Redis 适配器 (gyro/redis/redis.go)

将 Redis 服务器包装为 Gyro 节点,底层是真实的 `github.com/redis/go-redis/v9` 客户端:

```go
type RedisNode struct {
    id      string
    address string
    conn    RedisConnection // 内部持有 *redis.Client
    healthy bool
}
```

`GetNativeClient()` 返回的是原生 `*redis.Client`,拿到之后可以直接用 go-redis 的完整 API,Gyro 不做二次封装。

#### gRPC 适配器 (gyro/grpc/grpc.go)

将 gRPC 服务包装为 Gyro 节点,底层是真实的 `google.golang.org/grpc` `*grpc.ClientConn`:

```go
type GRPCNode struct {
    id      string
    address string
    conn    GRPCConnection // 内部持有 *grpc.ClientConn
    healthy bool
}
```

健康检查基于标准的 `grpc.health.v1.Health` 协议;目标服务未实现该协议时(`Unimplemented`),回退为只看连接的连通性状态。

### 5. 服务发现与配置管理 (gyro.go / config.go)

`Client` 把 ServiceDiscovery、ConfigManager、NodeFactory、HealthChecker 组合在一起,提供动态服务发现和配置热更新能力(节点/配置文件变化时增量应用,不推倒重建):

```go
type Client struct {
    locator       Locator
    healthChecker HealthChecker
    serviceName   string
    discovery     ServiceDiscovery
    configManager *ConfigManager
    nodeFactory   NodeFactory
}
```

## 一致性哈希实现

一致性哈希算法本身不在本仓库里,由独立库 [focusandinsist/consistent-go](https://github.com/focusandinsist/consistent-go) 提供,`gyro/locator.go` 只是对它的封装。它具有以下特性:

### 分区系统

```
Hash Ring (Virtual Nodes)
┌─────────────────────────────────────────────────────────┐
│  VN1   VN2   VN3   VN4   VN5   VN6   VN7   VN8   VN9   │
│   │     │     │     │     │     │     │     │     │    │
│  N1    N2    N1    N3    N2    N1    N3    N2    N3   │
└─────────────────────────────────────────────────────────┘

Partition System
┌─────────────────────────────────────────────────────────┐
│  P0   P1   P2   P3   P4   P5   P6   P7   P8   P9  ...  │
│   │    │    │    │    │    │    │    │    │    │        │
│  N1   N2   N1   N3   N2   N1   N3   N2   N3   N1       │
└─────────────────────────────────────────────────────────┘
```

### 增量重平衡

当添加新节点时,底层的 consistent-go 库使用增量重平衡算法(伪代码,实际实现见 consistent-go 仓库):

1. **范围计算**: 确定新节点影响的分区范围
2. **负载检查**: 只有在新节点未过载时才迁移分区
3. **最小化迁移**: 只迁移必要的分区

```go
func (c *Consistent) remapPartitionsForNewMember(member string) {
    // 为每个虚拟节点找到影响的分区范围
    for i := 0; i < c.config.ReplicationFactor; i++ {
        // 计算虚拟节点位置
        vnodeKey := buildVirtualNodeKey(member, i)
        h := c.hasher.Sum64(vnodeKey)

        // 找到前驱节点，确定影响范围
        // 只迁移在范围内且新节点未过载的分区
    }
}
```

## 请求流程

### 1. 单点执行流程

```
Application
    │
    ▼
Client.Execute(key, cmd)
    │
    ▼
Pool.Get(key) ──────────► ConsistentHash.LocateKey(key)
    │                              │
    ▼                              ▼
Node.Execute(cmd) ◄────────── Find Node by ID
    │
    ▼
Backend Service
```

### 2. 副本执行流程

```
Application
    │
    ▼
Client.ExecuteOnReplicas(key, cmd, N)
    │
    ▼
Pool.GetReplicas(key, N) ──► ConsistentHash.LocateReplicas(key, N)
    │                              │
    ▼                              ▼
[Node1, Node2, Node3] ◄────── Find N Closest Nodes
    │
    ▼
Parallel Execution on All Nodes
    │
    ▼
Aggregate Results
```

### 3. 广播执行流程

```
Application
    │
    ▼
Client.Broadcast(cmd)
    │
    ▼
Pool.GetAllNodes()
    │
    ▼
Parallel Execution on All Nodes
    │
    ▼
Collect Results from All Nodes
```

## 故障处理

### 节点故障检测

1. **主动检查**: 定期 ping 检查
2. **被动检查**: 请求失败时检查
3. **阈值机制**: 连续失败达到阈值才标记为不健康

### 故障转移策略

1. **副本转移**: 主节点故障时自动使用副本
2. **重试机制**: 临时故障时的重试逻辑
3. **降级处理**: 所有副本都故障时的降级策略

### 节点恢复

1. **健康检查**: 持续检查故障节点
2. **恢复阈值**: 连续成功达到阈值才恢复
3. **渐进恢复**: 逐步增加流量到恢复的节点

## 性能优化

### 1. 连接复用

- 每个节点维护连接池
- 支持连接的生命周期管理
- 自动清理空闲连接

### 2. 并发处理

- 读写锁保护共享状态
- 并行执行副本和广播请求
- 无锁的快速路径优化

### 3. 内存优化

- 预分配哈希环和分区映射
- 复用对象减少 GC 压力
- 延迟初始化非关键组件

## 扩展性设计

### 1. 插件化架构

- 可插拔的哈希算法
- 可扩展的健康检查策略
- 自定义的负载均衡算法

### 2. 协议无关

- 统一的 Node 和 Command 接口
- 支持任意协议的后端服务
- 简单的适配器开发模式

### 3. 配置驱动

- 运行时配置更新
- 服务发现集成
- 动态节点管理

这种架构设计确保了 Gyro 既能提供高性能，又能保持良好的可扩展性和可维护性。

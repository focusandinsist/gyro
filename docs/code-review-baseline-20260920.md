# Gyro 全项目基线审查

审查日期：2026-09-20  
审查范围：当前工作树中的 README、`gyro/` 核心代码、Redis/gRPC 适配器、单元测试和集成测试。  
审查边界：`consistent-go` 按可信的外部一致性哈希库处理，本报告只检查 Gyro 对它的封装、调用顺序和错误处理，不重复审查该库的算法实现。项目尚未上线，因此以下建议以“现在修正设计和行为”为准，不保留历史兼容约束。

## 结论

当前代码可以编译，核心单元测试在 Windows 目标下通过，但还不适合发布。最高风险集中在关闭/重启、健康检查 goroutine、配置热更新和适配器故障转移：这些路径可能导致进程 panic、后台 goroutine/连接泄漏，或把请求送到已经被判定为不可用的节点。

### 规范/代码质量轴（Standards）

主要问题是生命周期所有权不清、锁内执行网络 I/O、可变 channel/context 被跨运行复用、外部对象浅拷贝，以及 Redis/gRPC 适配器和 `Client` 中的重复/发散职责。P0/P1 项目属于可验证的并发或资源管理缺陷；P2/P3 项目属于需要在行为稳定后处理的结构性改进。

### 规格/行为轴（Spec）

README 和功能文档承诺了健康检查、故障转移、增量服务发现、配置热更新和原生客户端入口。当前实现中，便捷适配器绕过健康池、1/2 节点副本请求失败、节点 ID 会随地址顺序变化、配置重建不关闭旧资源，均属于公开行为与实现不一致。`consistent-go` 本身按可信依赖处理，本轴只报告 Gyro 的调用契约问题。

## 优先级定义

- **P0**：正常生命周期或故障路径即可崩溃、永久泄漏，必须先修。
- **P1**：用户可见的错误路由、故障转移失效、配置语义失效或发布阻塞。
- **P2**：正确性边界、并发性能、测试/文档工程质量问题。
- **P3**：可读性和长期维护性改进。

## 问题与优化点

### P0：先修复会崩溃或不可控泄漏的问题

1. **关闭时存在向已关闭 channel 发送的 panic**  
   位置：`gyro/health.go` 的健康 listener、事件 processor 和 `HealthAwarePool.Close`。健康监听器在独立 goroutine 中向 `healthEventChan` 发送；旧版 `Close` 直接关闭该 channel，却没有等待监听器和事件处理器退出。关闭与健康状态变化并发时会触发 `send on closed channel`。处理器也没有检查 `event, ok := <-channel`，channel 关闭后会持续消费零值并占用 CPU。  
   建议：增加独立的可取消生命周期、`sync.Once`/关闭状态和 `WaitGroup`；先广播关闭信号、停止 checker、等待 processor；不要关闭仍可能被异步 listener 持有的数据 channel。`Close` 必须幂等。

#### P0 修复记录：HealthAwarePool 关闭竞态

这次实现已经修复该 P0，不再只是记录风险。下面保留完整的原因和设计，便于后续 AI Coding Agent 在下一轮修改时理解不能回退的并发约束。

**原始故障时序**

1. `DefaultHealthChecker.Check` 检测到节点状态翻转。
2. `notifyHealthChange` 为每个 listener 启动独立 goroutine；listener 并不受 `StopMonitoring` 同步等待保护。
3. `HealthAwarePool` listener 向 `healthEventChan` 写入 `HealthEvent`。
4. 另一条 goroutine 同时调用 `HealthAwarePool.Close`。旧实现先停止 checker，随后直接 `close(hap.healthEventChan)`。
5. 第 3 步已经取得 channel 引用、但尚未完成发送的 goroutine 会向已关闭 channel 写入，运行时立即 panic：`send on closed channel`。这不是普通业务错误，而是会终止整个 Go 进程的并发生命周期错误。

还有第二条独立故障路径：旧的事件处理器使用 `event := <-hap.healthEventChan`，没有检查接收结果。Go 从已关闭 channel 接收时会立即返回元素类型的零值，并且永远保持 ready；因此 `select` 会反复选择该分支，处理伪造的空事件并形成 busy loop，造成 CPU 空转。仅仅给接收端加 `ok` 检查可以止住忙循环，但仍不能解决生产者向已关闭 channel 发送的问题。

**修复后的所有权模型**

- `healthEventChan` 是事件数据通道，可能被已经异步启动的 listener 持有引用，因此 **HealthAwarePool 不再关闭它**。pool 被释放后该 channel 会随对象一起由 GC 回收，不需要人为 close。
- 新增 `eventDoneCh`，它不是数据通道，而是 pool 生命周期的广播信号。只有 `HealthAwarePool.Close` 关闭它，用来通知 listener 和 processor“不要再产生/处理事件”。
- listener 在持有 `hap.mu` 时检查 `closed`，关闭后直接返回；发送阶段同时监听 `eventDoneCh`。即便 checker 在 Close 后仍回调 listener，也只会被丢弃，不会触碰已关闭的数据通道。
- processor 同时监听调用方 context 和 `eventDoneCh`。pool 关闭时 processor 会退出；不依赖数据通道关闭来结束循环，因此不会读取关闭 channel 的零值。
- `processorWG` 在启动流程中、与 `closed` 检查使用同一把锁完成 `Add`，Close 再关闭生命周期信号并 `Wait`。这样不会出现 `Wait` 已开始而另一个 goroutine 又对计数器 `Add` 的非法交错。
- `closeOnce` 保证重复 Close 只执行一次：不会重复关闭生命周期 channel、不会重复关闭 locator，也不会重复释放节点资源。

关闭顺序固定为：

```text
标记 closed + 关闭 eventDoneCh
        -> 停止 health checker 产生新的检查
        -> 等待事件处理器退出
        -> 关闭底层 locator 和节点连接
```

这里的关键是“关闭信号”和“数据 channel”分离。关闭信号可以安全地被多个 goroutine 监听；数据 channel 只有在确认所有生产者都退出后才适合关闭，而当前 `HealthChecker` 接口没有提供 listener 注销或异步回调完成通知，所以不能安全地关闭事件数据 channel。

**涉及的 Go 并发知识点**

1. **channel 关闭责任**：通常由发送方关闭 channel，但前提是发送方拥有全部生产者的生命周期。只要存在异步生产者或外部回调，就不能仅凭“我准备退出了”直接 close 数据 channel。
2. **关闭 channel 的语义**：关闭会唤醒接收者；接收表达式必须使用 `value, ok := <-ch` 判断是否还有数据，否则关闭 channel 会永久返回零值并让 select 分支持续就绪。
3. **done channel 作为广播取消**：关闭一个只读的 `done` channel 会同时通知所有监听 goroutine，适合表达“整个组件结束”，而不承担传输业务数据的职责。
4. **WaitGroup 的 Add/Wait 规则**：新 goroutine 必须在可能发生 Wait 之前完成 Add；本修复把 Add 放入与关闭状态检查相同的锁区间，避免 Start 和 Close 并发时计数器先归零、随后又 Add。
5. **sync.Once 的幂等释放**：资源释放通常不是天然幂等的，尤其是 close(channel)。`sync.Once` 把重复调用转化为安全的 no-op，同时让所有调用者看到同一次释放结果。
6. **竞态安全不等于生命周期安全**：即使 channel、map 和布尔值都有锁保护，仍可能出现“锁保护下检查、解锁后资源被关闭、随后继续使用”的 TOCTOU 生命周期问题。本修复用独立 done 信号和资源所有权顺序补上这一层。

**本轮代码变更**

- `gyro/health.go`：增加 `closed`、`closeOnce`、`closeErr`、`eventDoneCh` 和 `processorWG`；listener 在关闭状态下拒绝事件；processor 监听关闭信号；Close 改为幂等、有序等待且不关闭事件数据 channel。
- `gyro/health_test.go`：增加可控 HealthChecker，在 pool Close 后主动发送 late callback，验证不会 panic；同时验证关闭信号已发出和重复 Close 安全。

**验证结果**

- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run TestHealthAwarePool_CloseStopsProcessorAndRejectsLateEvents -count=100`：通过。
- `go vet ./gyro`：通过。
- `CGO_ENABLED=1 go test -race ./gyro`：当前环境缺少 `gcc`，race 构建在 `runtime/cgo` 阶段失败；不是本轮代码测试失败，但仍需在带 C 编译器的 CI 中执行。
- 重复运行既有 `TestDefaultHealthChecker_HealthListener` 时偶发事件顺序颠倒；该测试依赖异步 listener 和固定 `Sleep`，属于原有 flaky 测试，本轮未扩大到 P1/P2 测试治理。

**本轮明确未扩展的边界**

健康 checker 自身的 stop channel 替换导致 worker 泄漏、重复注册 listener、nil checker、配置更新使用 `context.Background()` 等问题属于报告中的 P1/P2，不能因为本轮修复了 HealthAwarePool 的 P0 关闭竞态就视为已解决。后续对 checker 生命周期的修改必须保持本节的“数据 channel 与关闭信号分离”原则。

### P1：发布前必须修复的功能和生命周期问题

2. **Client 停止后无法安全重启，且停止前 Close 会泄漏**  
   位置：`gyro/gyro.go:225-469`。旧实现把 `stopCh` 创建在 Client 构造时，`Stop` 关闭它和 locator，但 `Start` 不重建；再次 `Start` 的 watcher 会立即退出，再次 `Stop` 可能二次 close。`Stop` 在 Client 尚未运行时直接返回，构造阶段已经创建的 locator 和节点连接不会关闭。locator 仍留在 `c.locator` 中时，停止后的配置 watcher 还可能重新初始化资源；每次 `Start` 又重复注册一个 config watcher。  
   现在已改为可重启的运行实例模型：每次 `Start` 创建新的运行 context，`Stop` 取消该 context、摘除并关闭当前 locator，下一次 `Start` 重新初始化 locator；配置 watcher 只注册一次，停止态回调直接返回。`Close` 与未 `Start` 的 Client 都会释放已有 locator，且 Start/Stop 被生命周期锁串行化。

#### P1 修复记录：Client 生命周期和运行实例隔离

**原始故障的状态机问题**

旧代码把“Client 对象存在”和“某一次运行正在进行”混在一起：

```text
NewClient
  └─ 创建 stopCh 和 locator
Start
  └─ running = true，启动 watcher，注册 config watcher
Stop
  └─ close(stopCh)，关闭 locator，但保留 locator 指针
再次 Start
  └─ 复用已关闭 stopCh 和已关闭 locator
```

这会产生四个可观察故障：

1. **Stop → Start 失败**：watcher 同时监听已经关闭的 `stopCh`，启动后立即退出；健康监控也可能落在已经关闭的 pool 上。
2. **重复 Stop 可能 panic**：`stopCh` 只创建一次，第二次运行仍使用已经关闭的 channel，再次 `close` 会触发 `close of closed channel`。
3. **未 Start 的 Close 泄漏**：`Stop` 先判断 `running`，未运行时直接返回；但 `NewClient` 已经在构造阶段创建了节点连接，所以调用方无法通过 `Close` 释放这些资源。
4. **停止态仍会被回调唤醒**：config watcher 永久留在 ConfigManager 中。旧的 `handleConfigChange` 只检查 locator 是否为 nil；由于 Stop 没有清空 locator，停止后的配置更新可能再次创建 locator 和连接。

**本轮采用的生命周期模型**

Client 现在区分两个层次：

- **Client 对象生命周期**：包含 discovery、ConfigManager、NodeFactory、HealthChecker 等依赖，可以在多次 Start/Stop 之间复用。
- **Run 生命周期**：一次 Start 到对应 Stop 的短生命周期，拥有自己的 `context.WithCancel`、watcher goroutine、health-monitor goroutine 和 locator/节点连接。

每次运行的资源关系如下：

```text
Start(parentCtx)
  └─ runCtx, runCancel := context.WithCancel(parentCtx)
      ├─ watchServiceNodes(runCtx)
      ├─ startHealthMonitoringWhenReady(runCtx)
      └─ locator + HealthAwarePool + node connections

Stop()
  ├─ 标记 running=false，并把 locator 从 Client 状态中摘除
  ├─ 清空 runCancel，阻止旧 run 被再次使用
  ├─ 调用 runCancel，通知所有 run goroutine 退出
  └─ 在锁外关闭被摘除的 locator 和节点连接
```

**关键实现决策**

1. **用 per-run context 替代共享 stop channel**：`Start` 每次创建新的 `runCtx`；watchServiceNodes、processServiceWatch 和 health-monitor 只监听这个 context。旧 run 被取消不会影响下一次 Start。
2. **Start 在 locator 为空时重新初始化**：Stop 会把 `c.locator` 设为 nil，下一次 Start 通过 `initializeUnsafe` 创建全新的 locator、节点连接和 HealthAwarePool，不复用已关闭资源。
3. **Stop 先摘除、后关闭**：在 `c.mu` 保护下把 `running=false`、`locator=nil`、`runCancel=nil` 一次性提交，然后释放锁，再执行可能阻塞的 cancel/Close。这样迟到的 service-watch 或 config 回调看到的是停止状态，不会重新建 pool；同时 GetLocator/GetClientForKey 不会继续暴露即将关闭的 locator。
4. **使用 lifecycleMu 串行化 Start/Stop**：`c.mu` 保护普通状态读写，`lifecycleMu` 保护整个启动/停止事务，避免一个 goroutine 正在关闭旧 locator 时另一个 goroutine 已经开始新一轮 Start。
5. **配置 watcher 只注册一次**：ConfigManager 当前没有注销 watcher 的 API，因此 Client 通过 `configWatcher` 标志只添加一次 watcher；停止期间 `handleConfigChange` 直接返回。配置值仍由 ConfigManager 保存，下一次 Start 会使用最新配置初始化新 locator。
6. **允许重复 Close**：Stop 对“运行中”和“未运行但仍有 locator”两种状态统一处理；第一次释放 locator，后续调用发现 locator 已为 nil，安全返回。
7. **拒绝 nil context**：nil context 无法调用 `Done`，原实现会把 panic 推迟到后台 goroutine；Start 现在立即返回错误，让调用方在边界处得到确定反馈。

**涉及的 Go 生命周期和并发知识点**

1. **context 是运行实例的取消树**：父 context 表示调用方整体生命周期，`WithCancel` 子 context 表示一次 Start。Stop 只取消子 context，不污染 Client 下一次运行。
2. **不要复用已关闭 channel 表达新一轮运行**：关闭 channel 是永久状态，适合广播“本轮结束”，不适合作为可重启对象的成员。用 per-run context 可以自然获得新的 Done channel。
3. **状态提交与资源释放分离**：先在锁内发布“已停止且资源不可再访问”的状态，再在锁外执行 Close，避免慢速网络关闭阻塞 Get/Start，也避免回调拿到半关闭对象。
4. **迟到回调必须以状态为准**：取消 context 不能保证已经从 channel 取出的事件立即消失；所有会改变 locator 的回调仍需检查 `running`，否则可能在 Stop 后复活资源。
5. **生命周期锁和数据锁职责不同**：`mu` 解决字段一致性；`lifecycleMu` 解决跨多个步骤的 Start/Stop 事务互斥。只用一个短临界区无法保护“解锁后继续 Close”这类完整生命周期操作。
6. **watcher 注册是资源管理**：回调注册本身也是长期引用。没有注销 API 时，必须确保只注册一次，并在回调中明确停止态行为，否则每次重启都会累积副作用。

**本轮代码变更**

- `gyro/gyro.go`：移除 Client 级共享 `stopCh`；增加 `lifecycleMu`、per-run `runCancel` 和一次性 `configWatcher` 标志；重写 Start/Stop 的状态和资源交接；watch/config 回调在停止态退出。
- `gyro/client_lifecycle_test.go`：补齐生命周期测试桩接口，并增加未 Start Close、Stop→Start、重复 watcher 注册、停止后配置更新四组回归测试。

**验证结果**

- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro`：通过。
- 生命周期回归测试重复 50 次：通过。
- `go vet ./gyro`：通过。
- `go vet ./...`：通过。

**本轮明确未扩展的边界**

- 配置内容发生变化时如何原子替换旧 pool、关闭旧连接并恢复健康监控，仍属于下一个 P1“配置热更新”任务；本轮只保证停止态配置 watcher 不会复活 Client。
- DefaultHealthChecker 内部 worker 的 stop channel 泄漏和 context.Background 重启仍未处理。
- ConfigManager 仍没有真正的 watcher 注销 API；当前方案通过“一次注册 + 停止态短路”控制生命周期，后续若需要动态销毁 ConfigManager，应补充显式注销机制。

3. **配置热更新泄漏旧连接，并使健康故障转移失效**  
   位置：`gyro/gyro.go:682-710`。locator 配置变化时直接调用 `initializeUnsafe` 覆盖 `c.locator`，旧 `HealthAwarePool`、节点连接、checker 注册和后台 goroutine 未关闭。新 pool 也没有重新调用 `StartHealthMonitoring`。连接配置变化只打印 warning（`694-697`），实际连接池/超时永远不生效。  
   建议：构造新 pool 完成健康校验后，原子替换并关闭旧 pool；连接配置变化走同样的平滑重建/回滚流程；失败时保留旧配置和旧 pool。

#### P1 修复记录：配置热更新的原子替换和资源所有权

**原始故障时序**

旧的 `handleConfigChange` 在持有 `Client.mu` 时发现 locator 配置变化，然后直接调用 `initializeUnsafe`。`initializeUnsafe` 会创建一个新的 `ConsistentLocator`、为每个节点创建新的连接，并把新的 `HealthAwarePool` 赋值给 `c.locator`：

```text
配置更新
  └─ 创建新 locator 和新节点连接
      └─ c.locator = newPool
          ├─ oldPool 没有 Close
          ├─ old node connections 仍然存活
          ├─ old pool 的 health listener 仍然注册在 checker 上
          └─ newPool 没有 StartHealthMonitoring
```

这条路径同时产生两个独立问题：

1. **旧资源泄漏**：覆盖指针不会自动调用 Go 对象的 `Close`。旧 pool、旧 locator、旧节点连接和旧 checker 节点引用仍然存在；真实 Redis/gRPC 连接会继续占用 socket、文件描述符和后台资源。
2. **健康故障转移失效**：新 pool 只在构造函数中把节点标记为 healthy，但没有启动事件处理器和 checker 监控。节点实际故障后，新 pool 的 `healthyNodes` 不会收到状态变化，路由仍可能选择已经失效的节点。
3. **连接配置静默失效**：连接配置变化只打印 warning，原有 `NodeFactory` 仍使用旧配置创建节点。调用方以为超时或连接池参数已经生效，实际运行资源完全没有变化。
4. **部分失败会留下半更新状态**：如果新 locator 创建、节点连接建立或配置 watcher 执行失败，旧实现没有清晰的 prepare/commit 边界，配置管理器还可能保留已经失败的新配置。

**本轮采用的更新协议**

本轮把配置更新分成“准备”和“提交”两个阶段。运行中的旧 pool 只有在新资源完整构建成功后才会被替换：

```text
ConfigManager.UpdateConfig
  └─ 暂存新配置并通知 watcher
      └─ Client.handleConfigChange
          ├─ 判断 locator / connection / health checker 是否变化
          ├─ 准备新的 NodeFactory（不修改当前 factory）
          ├─ 构建完整的新 ConsistentLocator 和全部节点连接
          │   └─ 中途失败：关闭已经创建的节点和 locator，旧运行状态不变
          ├─ 应用 health checker 配置
          │   └─ 失败：关闭新 locator，旧运行状态不变
          ├─ HealthAwarePool.ReplaceLocator(newLocator)
          │   ├─ 切换当前 locator 和 healthy node 快照
          │   ├─ 从 checker 移除已删除节点
          │   ├─ 向 checker 注册新节点
          │   └─ 关闭旧 locator 和旧节点连接
          └─ 提交新的 NodeFactory 引用
```

这里没有创建第二个 `HealthAwarePool`，而是让当前 pool 原子替换其底层 locator。这样做有三个重要效果：

- 当前 pool 的健康事件处理器和 listener 只存在一份，不会每次配置热更新都额外注册一个永久回调。
- 当前 `Start` 创建的运行 context 继续控制同一个健康监控处理器，locator 替换后不需要重新启动一个脱离原 context 的监控 goroutine。
- Client 对外暴露的 `HealthAwarePool` 实例保持稳定，替换只发生在其受保护的底层 locator 和健康节点快照上。

**资源替换的关键顺序**

`ReplaceLocator` 的顺序不能简化为“先关闭旧 locator，再创建新 locator”，也不能简化为“直接覆盖指针”：

1. 新 locator 必须先完成构造。所有节点都成功创建并加入 ring 后，才允许触碰当前运行资源。
2. 新 locator 发布后，健康池的节点快照同步切换。新节点先按 optimistic healthy 状态加入，后续 checker 再根据真实探测结果更新状态。
3. 已从拓扑中删除的节点先从 checker 移除，避免 checker 继续探测已经不属于新拓扑的旧连接。
4. 最后关闭旧 locator。此时 Client 已经不再通过当前 pool 暴露旧 locator，旧节点只承担收尾关闭责任。
5. 所有可能失败的构建操作都在发布前完成；失败路径只关闭临时资源，不覆盖旧 pool。

这遵循资源所有权转移原则：在提交点之前，新资源属于“临时构建结果”；在提交点之后，新资源属于运行中的 pool，旧资源进入关闭流程。单纯给字段重新赋值不等于释放资源，Go 的垃圾回收也不会调用业务对象的 `Close` 方法。

**连接配置的显式工厂契约**

`NodeFactory` 原本只有 `CreateNode(NodeInfo)`，无法把新的 `ConnectionConfig` 传入已经存在的工厂。继续打印 warning 会让配置热更新产生错误的成功假象，因此增加了可选的 `ConnectionConfigurableNodeFactory`：

```go
type ConnectionConfigurableNodeFactory interface {
    NodeFactory
    WithConnectionConfig(config ConnectionConfig) (NodeFactory, error)
}
```

Redis 和 gRPC 工厂实现该接口时返回一个独立的 factory 副本，旧 factory 不会被并发修改。Client 在新 locator 构建成功后才提交这个副本；如果自定义 factory 不支持该能力，连接配置更新会明确返回错误，`ConfigManager` 恢复旧配置，而不是继续使用旧连接却报告更新成功。

**失败回滚语义**

- 节点创建失败：`buildLocatorUnsafe` 关闭已经加入临时 locator 的节点，并关闭临时 locator；当前 pool 和节点保持可用。
- 新 locator 配置非法：构造函数直接返回错误，当前 pool 不变。
- 新 health checker 配置失败：临时 locator 被关闭，当前 pool 不变。
- active locator 不支持原子替换：临时 locator 被关闭并返回错误。
- watcher 返回错误：`ConfigManager` 把配置快照恢复为 watcher 调用前的旧指针。

旧 locator 的 `Close` 错误会被记录到 pool logger。替换已经完成但旧资源关闭失败时，系统不会偷偷把配置快照回滚成旧值，也不会把新 pool 回滚成一个可能已经部分关闭的旧 pool；调用方可以根据日志和监控处理关闭失败。

**涉及的 Go 知识点**

1. **对象替换不会自动析构**：Go 的 GC 只负责回收不可达内存，不理解 socket、连接池、goroutine 等外部资源。覆盖 `c.locator` 前必须显式安排旧对象的 `Close`。
2. **Prepare/Commit 两阶段思维**：先构造完整的新状态，再在一个明确的提交点切换引用。这样构建失败不会污染当前运行状态。
3. **快照和资源所有权**：新 locator 的节点集合是一次性快照；发布后由 pool 负责使用和关闭，旧快照由替换流程负责收尾。
4. **锁保护的是引用一致性，不是外部资源本身**：`locatorMu` 保护 HealthAwarePool 当前底层 locator 的切换；真正耗时的节点关闭发生在完成引用切换之后，避免读路径继续拿到旧资源。
5. **共享 HealthChecker 的注册生命周期**：重新创建 pool 会重复注册 listener。复用同一个 pool 可以避免 listener 累积；拓扑变化时必须同步 checker 的节点集合，否则 checker 会继续持有并探测旧节点。
6. **上下文是运行边界**：健康事件处理器继续使用 Start 时创建的 run context，配置热更新不会创建脱离 Client 生命周期的 `context.Background()` 监控任务。
7. **配置失败必须有可观察语义**：不支持的连接配置更新应返回错误，而不是 warning 后继续运行旧参数；否则配置系统的“成功”结果与实际运行状态不一致。
8. **接口扩展与能力检测**：核心 `NodeFactory` 保持最小创建职责，连接配置更新通过显式能力接口检测。未实现能力时快速失败，比通过类型猜测或静默忽略更容易测试和排障。

**本轮代码变更**

- `gyro/gyro.go`：拆分临时 locator 构建；配置更新采用新资源准备、健康配置应用、pool 原子替换和旧资源关闭流程；增加 `ConnectionConfigurableNodeFactory`；移除连接配置变化时的静默 warning。
- `gyro/health.go`：增加底层 locator 的受保护访问和 `ReplaceLocator`；替换时同步 healthy node 快照、checker 节点集合并关闭旧 locator。
- `gyro/config.go`：watcher 返回错误时恢复旧配置快照。
- `gyro/grpc/grpc.go`、`gyro/redis/redis.go`：实现独立连接配置 factory，支持热更新时用新参数创建连接。
- `gyro/client_lifecycle_test.go`：增加配置重建关闭旧节点、重建后健康监控继续工作、不支持连接配置更新时失败回滚等回归测试。

**本轮验证结果**

- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run 'TestClient(ConfigReload|CloseBeforeStart|CanRestart|RegistersConfigWatcher|IgnoresConfigUpdates)' -count=30`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run TestClientConfigReloadKeepsHealthMonitoringActive -count=10`：通过。
- `go test ./gyro/grpc ./gyro/redis`：通过。

**本轮明确未扩展的边界**

- `DefaultHealthChecker` 自身的 worker stop channel 重启竞态和 context 脱离问题属于后续健康 checker 生命周期任务；本轮复用现有 checker，不把该问题伪装成已解决。
- `ConnectionConfigurableNodeFactory` 是连接配置生效的必要能力；不具备该能力的自定义 factory 会明确失败，不能自动猜测如何重建协议连接。
- 旧 locator 的关闭是同步收尾操作；如果底层协议 Close 本身失败，替换结果仍保持新 pool，但关闭错误会返回给配置调用方。

4. **便捷 Redis/gRPC 客户端绕过健康池，README 宣称的故障转移不成立**  
   位置：`gyro/redis/redis.go:167-186`、`gyro/grpc/grpc.go:193-215`。两个 `New*Client` 直接创建 `ConsistentLocator`，没有 `HealthAwarePool` 和 `HealthChecker`；节点宕机后 `GetClientForKey` 仍可能返回该节点的原生 client。  
   建议：便捷构造函数与 DI 路径使用同一套健康池；或者明确缩小 README 的承诺范围。优先统一实现，避免两套语义。

#### P1 修复记录：便捷客户端统一使用健康池

**原始语义分裂**

项目此前存在两条创建 Redis/gRPC 客户端的路径：

```text
依赖注入路径
  NewClient
    -> ConsistentLocator
    -> HealthAwarePool
    -> DefaultHealthChecker
    -> Start 后执行主动探测和故障转移

便捷路径
  NewRedisClient / NewGRPCClient
    -> ConsistentLocator
    -> 直接返回
```

两条路径最终都提供名为 `GetClientForKey` 的方法，但行为并不一致。DI 路径会先检查一致性哈希主节点在健康池中的状态，在主节点不健康时尝试同 key 的副本；便捷路径只调用裸 `ConsistentLocator.Get`，因此无论节点健康状态如何都会返回环上的主节点。README 的快速开始主要使用便捷入口，却把 Gyro 描述成具备健康检查和故障转移能力，导致文档承诺与最常用入口的实际行为相反。

节点对象自身的 `IsHealthy` 方法不会让裸 locator 自动故障转移。`ConsistentLocator` 的职责只是确定性地把 key 映射到节点，它既不主动执行探测，也不保存健康状态。只有 `HealthAwarePool` 把以下三部分组合起来以后，故障转移才真正成立：

1. `HealthChecker` 周期性调用 `Node.IsHealthy`。
2. health listener 把状态变化同步到 pool 的 `healthyNodes` 快照。
3. `HealthAwarePool.Get` 在主节点不健康时查询 replicas 并选择健康候选。

**统一后的便捷构造流程**

Redis 和 gRPC 便捷客户端现在使用与 DI Client 相同的核心组件：

```text
NewRedisClient / NewGRPCClient
  ├─ 校验地址和 HealthCheckerConfig
  ├─ 创建 ConsistentLocator
  ├─ 使用协议 NodeFactory 创建全部节点
  ├─ 创建 DefaultHealthChecker
  ├─ NewHealthAwarePoolWithChecker(locator, checker)
  ├─ 创建便捷客户端私有 healthCtx / cancel
  └─ StartHealthMonitoring(healthCtx)
```

便捷客户端仍然保留固定地址、无需显式调用 `Start` 的使用方式。区别是构造函数返回前已经完成健康池组装并启动监控，`GetClientForKey` 保存的 `locator` 接口实际指向 `*gyro.HealthAwarePool`。因此相同的 key 在主节点被 checker 判定为 unhealthy 后，会沿健康池的副本选择逻辑返回其他节点的原生 `*redis.Client` 或 `*grpc.ClientConn`。

**配置模型变化**

`RedisClientConfig` 和 `GRPCClientConfig` 都增加了 `HealthChecker` 字段：

```go
type RedisClientConfig struct {
    Locator       gyro.LocatorConfig
    HealthChecker gyro.HealthCheckerConfig
    Connection    gyro.ConnectionConfig
}
```

gRPC 使用相同结构。默认配置来自 `gyro.DefaultHealthCheckerConfig()`；调用方可以为便捷客户端配置探测间隔、超时、故障阈值和恢复阈值。项目尚未上线且不保留历史兼容，因此配置结构直接统一，不为旧的缺字段字面量提供隐式兼容分支。

健康配置在任何后台 goroutine 启动前通过 `gyro.ValidateHealthCheckerConfig` 校验。`Interval`、`Timeout`、`FailureThreshold` 或 `RecoveryThreshold` 非正数时，构造函数同步返回错误，避免非法 interval 延迟到后台 `time.NewTicker` 才 panic。

便捷构造函数复制传入的配置值并保存自己的快照，避免调用方在构造后修改原配置指针，从而使客户端记录的配置与节点工厂实际使用的配置产生偏差。即使关闭健康检查，当前仍要求配置字段合法；这样以后启用检查时不会携带无法运行的零值参数。

**便捷客户端的 context 和关闭所有权**

DI Client 的运行 context 由调用方通过 `Start(ctx)` 提供；便捷客户端没有单独的 Start 方法，因此它在构造时创建内部 `context.WithCancel(context.Background())`。这个 context 只拥有该便捷客户端的健康监控运行实例：

```text
constructor
  -> healthCtx + cancel
  -> HealthAwarePool.StartHealthMonitoring(healthCtx)

Close
  -> cancel()
  -> HealthAwarePool.Close()
      -> StopHealthMonitoring
      -> 等待事件处理器
      -> 关闭 ConsistentLocator
      -> 关闭全部协议连接
```

`context.Background()` 在这里不是泄漏，因为 cancel 被保存为客户端字段并由 `Close` 调用。取消 context 与关闭 pool 都可重复调用，因此便捷客户端重复 `Close` 不会二次关闭健康事件 channel。

**构造失败时的资源回滚**

原便捷构造函数在第 N 个节点创建或加入 ring 失败时会直接返回，此前创建的 N-1 个连接没有被关闭。本次统一健康池路径同时明确了构造所有权：

- 创建节点失败时关闭临时 locator，从而关闭此前已经加入的节点。
- 节点创建成功但 `AddNode` 失败时，先关闭尚未被 locator 接管的当前节点，再关闭 locator 中此前的节点。
- 只有全部节点构造成功后才创建并启动健康池。

**可测试性设计**

Redis/gRPC 的公开构造函数仍使用真实连接工厂。包内构造实现允许测试注入可控连接和 health checker，避免用固定端口、真实 Redis 或真实 gRPC 服务验证核心路由语义。回归测试使用三个节点，先寻找哈希主节点为 node-1 的 key，再把 node-1 的探测结果切换为失败；等待健康池收到 unhealthy 状态后，断言 `GetClientForKey` 返回其他节点的 native client。然后恢复 node-1 并等待 healthy 状态，验证相同 key 又回到原主节点。测试最后验证重复 `Close` 安全且三个连接都已关闭。

使用三个节点是有意的：当前 `HealthAwarePool.Get` 固定请求 3 个副本，而 `consistent-go` 在请求副本数超过成员数时返回错误。1/2 节点降级属于本报告第 8 项独立 P1，本轮没有顺带修改该行为。

**涉及的 Go 和架构知识点**

1. **装饰器组合**：`HealthAwarePool` 实现同一个 `Locator` 接口并包装 `ConsistentLocator`。调用方仍依赖 `Locator`，但获得了健康状态过滤和副本选择能力。
2. **构造函数即生命周期起点**：没有显式 `Start` API 的便捷对象必须在构造时启动其后台组件，并在 `Close` 中持有对称的取消与释放责任。
3. **context 所有权必须成对**：库内部创建 `WithCancel` 时必须保存并调用 cancel；否则 background context 会使监控 goroutine 超出客户端生命周期。
4. **接口值可以隐藏具体能力**：`GRPCClient.locator` 和 `RedisClient.locator` 的静态类型仍是 `gyro.Locator`，但动态值是 `*gyro.HealthAwarePool`。业务方法不需要依赖具体类型即可获得健康路由。
5. **构造失败回滚**：资源加入 locator 前由局部构造代码负责关闭，加入 locator 后所有权转移给 locator；失败路径必须按所有权边界释放。
6. **行为统一优先于代码表面对称**：两条 API 即使返回相同原生客户端类型，只要健康路由不同，就不是相同语义。统一底层组合比仅修改 README 更可靠。
7. **异步测试应等待状态而非固定 Sleep**：测试轮询 `HealthAwarePool.IsNodeHealthy` 并设置截止时间，直接等待需要观察的状态转换，减少机器速度导致的 flaky。

**本轮代码变更**

- `gyro/grpc/grpc.go`：便捷配置增加健康参数；构造函数创建并启动健康池；Client 保存 cancel；Close 对称取消；工厂支持包内连接注入；失败路径关闭临时资源。
- `gyro/redis/redis.go`：与 gRPC 路径保持相同的健康池、context、关闭和回滚语义。
- `gyro/health.go`：公开 `ValidateHealthCheckerConfig`，供核心热更新和适配器构造共用同一校验规则。
- `gyro/grpc/grpc_test.go`、`gyro/redis/redis_test.go`：增加真实健康池故障转移、资源关闭和非法健康配置测试。

**本轮验证结果**

- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro/grpc ./gyro/redis -count=100`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -count=1`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go vet ./...`：通过。
- `git diff --check`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./... -count=1`：核心、Redis 和 gRPC 适配器包通过；既有集成场景仍失败于 `node ... has no healthy native client`。该失败来自下一项 P1 描述的 gRPC 节点本地健康状态与 checker 阈值状态冲突，不是便捷客户端健康池测试失败。

**本轮明确未扩展的边界**

- 适配器节点本地 `healthy` 状态与 HealthChecker 阈值状态不一致的问题仍属于下一项 P1。本轮证明的是节点被健康池正式判定为 unhealthy 后能够切换，不把“第一次探测失败到达到阈值之间”的 nil native client 问题视为已解决。
- 1/2 节点集群请求固定 3 副本的问题仍属于第 8 项。
- 便捷客户端使用固定地址，不增加 ServiceDiscovery 和配置 watcher；需要动态拓扑或配置热更新时仍使用 DI Client。

5. **健康状态与适配器本地连接状态不一致，导致请求返回“无健康 client”**  
   位置：`gyro/health.go:384-418`、`gyro/grpc/grpc.go:132-162`、`gyro/redis/redis.go:105-135`。健康 checker 需要达到失败阈值才摘除节点，但一次探测失败已经把 `GRPCNode/RedisNode.healthy` 设为 false；pool 仍认为节点 healthy 时会直接返回该节点，适配器随后返回 nil native client。已在集成场景中复现 `node ... has no healthy native client`。  
   建议：由 HealthAwarePool 统一决定摘除时机；适配器的 `GetNativeClient` 不要重复维护与 checker 冲突的健康门控，或在 pool 选择前执行一致的状态查询。

6. **健康检查 worker 停止/重启会泄漏 goroutine，动态配置还脱离调用方 context**  
   位置：`gyro/health.go:146-204,261-309`。worker 和 monitoring loop 每轮读取可变字段 `hc.stopCh`；停止时关闭旧 channel 后立刻替换，新旧 goroutine 可能改读新 channel，旧 worker 永远退出不了。配置更新使用 `context.Background()` 重启，调用方取消 context 后仍可能继续运行。`Interval <= 0` 还会使 `time.NewTicker` panic。  
   建议：每次运行捕获不可变的 stop channel 和父 context，使用 run token/WaitGroup 等待退出；校验 interval、timeout、阈值为合法正值。

#### 第 5 项修复记录：统一健康状态的所有权

**问题本质**

修复前存在两个彼此独立、更新时机不同的健康状态源：

1. `DefaultHealthChecker` 维护 `NodeHealthStats.IsHealthy`，并通过 `FailureThreshold` 和 `RecoveryThreshold` 抑制短暂抖动；
2. `GRPCNode` 和 `RedisNode` 各自维护一个本地 `healthy` 字段，一次 `Ping` 失败就立即将其改为 `false`。

这两个状态都看似合理，但组合后违反了单一事实来源原则。假设失败阈值为 3，实际执行时序如下：

1. 健康池认为节点 A 健康，并把请求路由到 A；
2. 第一次后台 `Ping` 失败，checker 只累计一次失败，尚未达到阈值，因此健康池仍然认为 A 健康；
3. 同一次 `Ping` 把适配器节点的本地 `healthy` 立即改为 `false`；
4. 后续请求仍从健康池得到 A，但 `GetNativeClient` 因本地状态返回 `nil`；
5. 适配器最终返回 `node A has no healthy native client`，既没有使用 A，也没有进入健康池的副本故障转移分支。

这里的问题不是一次网络探测失败，而是“路由决策”和“连接是否可取”分别依据两套不同状态。阈值机制只在路由层生效，本地门控则绕过阈值提前拒绝请求，使 `FailureThreshold` 的语义失效。

**解决方法**

`GRPCNode` 和 `RedisNode` 不再保存探测结果。节点本地只保存一个生命周期状态 `closed`：

- `IsHealthy(ctx)` 负责执行一次带 context 的实时探测并返回结果，但不把结果持久化到节点；
- `HealthAwarePool` 通过 checker 的连续成功/失败计数，成为唯一负责决定节点何时摘除、何时恢复的组件；
- `GetNativeClient()` 只在节点已经关闭时返回 `nil`，不会因为尚未达到失败阈值的一次探测失败而否决健康池的路由结果；
- `Close()` 将 `closed` 设置为 `true`，并做成幂等操作，避免重复关闭底层连接。

`IsHealthy` 读取 `closed` 后立即释放读锁，再执行可能阻塞的网络 `Ping`。锁只保护进程内生命周期字段，不覆盖网络 I/O。这样 `Close` 不会因为探测持有读锁而额外等待整个网络超时。底层 Redis 和 gRPC 客户端本身允许关闭与正在进行的请求并发发生，而传入 `Ping` 的 context 仍负责及时取消探测。

**为什么不在 `GetNativeClient` 中再次查询 checker**

让节点反向依赖 checker 会形成不必要的双向依赖：checker 依赖 `Node.IsHealthy` 做探测，而 Node 又依赖 checker 决定是否暴露连接。它还会让 Node 无法脱离某一个 checker 实现使用。当前方案保持了清晰的职责边界：

- Node：连接探测和连接生命周期；
- HealthChecker：阈值、统计和健康状态转换；
- HealthAwarePool：依据健康状态选择主节点或副本；
- Redis/gRPC Client：把已选择节点转换为协议原生客户端。

**关闭状态与健康状态的区别**

`closed` 不是第二套健康状态。健康状态描述的是“当前探测结果是否经过阈值确认，可否参与路由”，可能在 unhealthy 和 healthy 之间反复转换；关闭状态描述的是“该对象生命周期是否永久结束”，关闭后不能恢复，也不能继续暴露 native client。将二者分开可以避免瞬时网络状态污染不可逆的资源生命周期。

**回归测试**

Redis 和 gRPC 适配器各新增一个测试，覆盖同一契约：第一次探测失败时 `IsHealthy` 返回 `false`，但 `GetNativeClient` 仍返回底层客户端；节点关闭后 `GetNativeClient` 才返回 `nil`。该测试直接锁定“单次失败不得绕过 health checker 阈值”的行为，不依赖真实 Redis/gRPC 服务。

**涉及的知识点**

1. **Single Source of Truth**：同一个业务决策只能有一个权威状态源。缓存或局部状态如果具有独立更新规则，就会从缓存退化成互相冲突的状态机。
2. **阈值与去抖动**：连续失败/恢复阈值用于避免瞬时抖动触发路由切换。任何位于阈值之外的提前门控都会破坏该保证。
3. **状态与生命周期分离**：可恢复的健康状态和不可恢复的关闭状态属于不同维度，不能共用一个布尔字段表达。
4. **锁不包围网络 I/O**：互斥锁应保护内存状态的读取或提交，不应覆盖延迟不可控的外部调用，否则关闭、配置更新等本地操作会被网络超时放大。
5. **幂等释放**：`Close` 可能被多个所有权层调用。以锁保护 `closed`，让第一次调用负责实际释放、后续调用直接成功，可以降低错误恢复路径的复杂度。

#### 第 6 项修复记录：以独立 run 管理健康检查生命周期

**原实现为什么会泄漏**

旧实现让所有 worker 和 ticker 循环在每次 `select` 时读取 `DefaultHealthChecker.stopCh` 字段。`StopMonitoring` 执行的操作是关闭当前 channel，随后立刻把字段替换成新 channel。goroutine 并没有捕获启动时的 channel，而是持续读取可变字段，因此存在如下竞态：

1. 旧 worker 尚未执行到下一次 `select`；
2. 停止逻辑关闭旧 channel；
3. 停止逻辑把 `hc.stopCh` 替换成尚未关闭的新 channel；
4. 旧 worker 再次求值 `hc.stopCh` 时读到新 channel；
5. 旧 worker 错过旧 channel 的关闭信号，与新 worker 一起继续存活。

反复调整 interval 会不断创建新 worker pool，使泄漏累积。与此同时，`UpdateConfig` 用 `context.Background()` 启动替代 worker，新 worker 与最初传给 `StartMonitoring` 的调用方 context 失去父子关系；调用方取消、Client 停止或请求域结束时，这些 worker 都不会自动退出。

**新的运行单元**

新增内部 `healthCheckRun`，每次启动都创建一个不可复用的运行单元，包含：

- `ctx`：从最初调用方 context 派生出的本轮 context；
- `cancel`：本轮唯一的主动停止入口；
- `queue`：只属于本轮 worker 的任务队列；
- `done`：只有本轮所有 worker 和 ticker 全部退出后才关闭的完成信号。

每个 worker 和 monitoring loop 在 goroutine 启动时捕获固定的 `run` 指针，不再读取可被重启逻辑替换的 stop channel 或任务队列。旧 run 和新 run 因此不可能串用停止信号或消费对方的任务。

**停止协议**

`stopRun` 使用严格的 cancel-and-wait 顺序：

1. 保存当前 run；
2. 调用 `run.cancel()` 广播取消；
3. 等待 `<-run.done`，确认 ticker 和全部 worker 已退出；
4. 最后才清除 `hc.run`，允许创建下一轮运行。

run 内部使用局部 `sync.WaitGroup` 统计 `maxWorkers` 个 worker 和一个 monitoring loop；另一个收尾 goroutine等待 WaitGroup 后关闭 `done`。因此 `StopMonitoring` 返回时具有明确保证：本轮健康检查后台 goroutine 已全部结束，不只是“已经发送停止请求”。

`lifecycleMu` 串行化 `StartMonitoring`、`StopMonitoring` 和 `UpdateConfig` 对 run 的操作，防止并发启动两轮、停止了错误的一轮，或在旧轮尚未退出时覆盖 `hc.run`。业务数据、配置和统计仍由原有 `mu` 保护，生命周期锁与数据锁职责分离。

**动态配置如何保留调用方 context**

`StartMonitoring(ctx)` 首次调用时保存 `parentCtx`。后续 interval 变化需要重启时，先完整停止旧 run，再从同一个 `parentCtx` 派生新 run，而不是使用 `context.Background()`。这带来三个保证：

- 原调用方取消后，当前 run 的 worker 与 ticker 会一起退出；
- 原调用方已经取消时，配置更新不会重新启动后台任务；
- checker 初始为 disabled 时也会保留父 context，之后热更新为 enabled 可以启动监控，但仍受原父 context 控制。

如果传入的 context 为 nil 或在调用前已经取消，`StartMonitoring` 会直接拒绝本次启动，不保存 `parentCtx`。因此一个无效的首次启动不会永久占用 checker 会话，调用方之后仍可使用有效 context 正常启动。

显式调用 `StopMonitoring` 会同时停止当前 run 并清除 `parentCtx`，表示这一完整监控会话已结束。之后如果确实需要重新使用 checker，调用方必须再次执行 `StartMonitoring(newCtx)`，由新会话提供新的生命周期边界。

**HealthAwarePool 对 enabled 切换的配套修复**

旧 `StartHealthMonitoring` 在 checker 初始 disabled 时直接返回，既不注册健康 listener，也不启动事件处理器。因此即使后续 `UpdateConfig` 把 checker 切换为 enabled，探测虽然可能运行，健康池却收不到状态转换。

现在 pool 在首次 `StartHealthMonitoring` 时无论 checker 当前是否 enabled，都会完成节点注册、listener 注册和事件处理器启动；是否创建探测 worker 仍由 checker 的配置决定。以后从 disabled 切换为 enabled 时，无需补建另一套 pool 管线。

pool 增加 `monitorStarted` 防止重复注册 listener 或重复启动事件处理器，并用 `monitorMu` 串行化启动与关闭，避免 `StartHealthMonitoring` 和 `Close` 交错造成 WaitGroup 的 `Add`/`Wait` 竞态。关闭顺序仍是先阻止新事件、停止 checker 并等待其 worker，再等待事件处理器，最后关闭 locator。

健康状态转换的 listener 改为有序的异步交付。checker 在数据锁内提交统计和状态、复制 listener 列表，并为本次通知记录前一个通知的完成信号；释放数据锁后启动通知 goroutine，该 goroutine先等待前一个通知完成，再依次调用本次 listener。这样连续 unhealthy/healthy 转换不会因为两个独立 goroutine 的调度顺序而倒置，同时 listener 可以安全回调 checker，甚至调用 `StopMonitoring`，不会等待仍位于自身调用栈上的 worker。worker 不等待外部 listener 完成，因此 checker 的停止保证只覆盖 worker 和 ticker；listener 自身不得无限阻塞。pool 的 listener 只做内存状态更新和一次非阻塞 channel 投递，pool 关闭后也会先检查 `closed`，不会在关闭后继续提交健康状态。

**配置合法性与 ticker 安全**

`UpdateConfig` 在修改运行状态前调用 `ValidateHealthCheckerConfig`，要求 interval、timeout、failure threshold 和 recovery threshold 都为正值。校验先于停止旧 run 和写入新配置，因此非法热更新不会破坏正在工作的旧配置，也不会把非正 interval 传给 `time.NewTicker` 引发 panic。

**回归测试**

新增生命周期测试覆盖以下行为：

1. `StopMonitoring` 返回时旧 run 的 `done` 已关闭，随后重启会得到不同的 run 和不同的任务队列；
2. interval 热更新会等待旧 run 完整退出并创建新 run；
3. 热更新创建的新 run 在原父 context 取消时退出，不会脱离为后台常驻任务；
4. checker 初始 disabled、之后 enabled 时会启动 run，并继续受最初父 context 控制；
5. pool 在 checker 初始 disabled 时启动，之后 enabled 仍能接收节点 unhealthy 事件并更新健康路由状态。
6. 连续 unhealthy/healthy 转换按产生顺序到达 listener，避免最终缓存状态被较早事件覆盖。
7. 已取消 context 的启动不会占用生命周期，之后传入有效 context 仍能正常启动。
8. health listener 可以在回调中停止 checker，不会与执行检查的 worker 形成自等待死锁。

**涉及的 Go 并发知识点**

1. **channel 身份不可替换**：关闭 channel 是对那个具体 channel 实例的广播。goroutine 若读取可变字段，就可能在停止前后观察到不同实例，因此每轮运行必须捕获固定 channel/context。
2. **取消不等于退出**：`cancel()` 只发出信号；只有等待所有 goroutine 完成，调用方才能安全地释放依赖资源或开始不重叠的新一轮运行。
3. **结构化并发**：子 goroutine 应从调用方 context 派生，生命周期不应超过拥有它的 Client/pool。用 `context.Background()` 重启会切断这条所有权链。
4. **每轮独立队列**：复用任务 channel 会让旧 worker 消费新一轮任务，即使停止信号正确也可能产生跨代执行。run-scoped queue 同时隔离任务和取消边界。
5. **WaitGroup 的 Add/Wait 规则**：可能与 `Wait` 并发发生的 `Add` 会造成错误用法。pool 用单独互斥锁把首次启动和关闭串行化，并且只允许启动一次。
6. **锁的分层**：生命周期变更使用 `lifecycleMu`，高频配置/节点/统计读取使用 `mu`；避免把等待 goroutine 退出的慢操作混入普通数据锁临界区。
7. **热更新是状态机转换**：enabled、disabled、running、parent canceled 不是一个布尔量可以完整表达的状态。显式保存 parent context 和当前 run，让每种转换的前置条件与结果可验证。

**本轮边界**

本轮只解决第 5、6 项。`DefaultHealthChecker.Check` 仍在持有 checker 数据锁时执行网络探测，这是第 15 项独立的锁粒度问题；它不会再造成 stop channel 串代或 context 脱离，但不遵守 context 的自定义 Node 仍可能让停止等待延长。后续处理第 15 项时，应把探测移到锁外，并在提交结果时确认节点仍是同一代对象。

有序异步通知假设 listener 会在有限时间内返回。永久阻塞的第三方 listener 会阻塞其后的通知链；这是回调接口无法强制中止外部代码的边界。内置 pool listener 只执行加锁内存更新和非阻塞 channel 发送，满足该约束。若未来允许不受信任的 listener，应改为容量有界、可取消并具有明确丢弃/合并策略的事件分发器。

**本轮验证结果**

- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run 'TestHealth(Checker|Listener|Pool)' -count=100`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro/grpc ./gyro/redis -count=50`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro/... -count=1`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go vet ./...`：通过。
- `git diff --check`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./... -count=1`：本轮涉及的核心、Redis 和 gRPC 包通过；全仓仍失败于既有集成测试把 native gRPC client 当作 `gyro.Node`，得到 `unknown` 地址。该问题已记录在第 21 项，不是第 5、6 项行为回归。
- 当前环境没有可用的 C compiler，且 `CGO_ENABLED=0`，因此没有执行 `go test -race`；并发行为由高次数生命周期测试和锁/所有权审阅覆盖，但 race detector 仍应在具备 CGO toolchain 的 CI 中运行。

7. **删除节点时忽略一致性库错误，可能造成 ring 与 node map 分裂**  
   位置：`gyro/locator.go:181-214`。`ring.Remove` 失败只记录 warning，随后仍删除 `cl.nodes` 并关闭节点。之后 ring 可能继续返回该 ID，而 map 查找报 `node not found in ring`。  
   建议：`ring.Remove` 失败立即返回并保留 map/连接；只有底层删除成功后再提交 map 删除和 Close。

8. **副本数固定为 3，1/2 节点集群的故障转移直接报错**  
   位置：`gyro/health.go:400-403`。底层 `consistent-go` 在请求副本数大于成员数时返回错误，pool 没有按当前节点数取最小值。  
   建议：使用 `min(3, len(nodes))`，并对 0/1/2 节点分别定义可测试的降级语义。

9. **服务发现变更接口不通知 watcher**  
   位置：`gyro/gyro.go:165-215`。`Register`、`Unregister`、`SetNodes` 只修改内存；只有 `UpdateNodes` 向 watcher 推送。调用方按 `ServiceDiscovery` 接口使用这些方法时，Client 永远看不到拓扑变化。  
    建议：抽出统一的发布函数，所有写操作都发送快照；发送的数据要复制，避免调用方修改内部 slice/map。当前 `UpdateNodes` 在缓冲满时丢弃事件，也应改为合并快照、背压或让 Client 在丢失事件后重新 Discover。

#### 第 7 项修复记录：ring 删除与 node map 使用同一个提交边界

**故障时序**

`ConsistentLocator` 同时维护两个必须一致的数据结构：底层 `consistent-go` ring 决定 key 应该映射到哪个 node ID，`nodes` map 再把该 ID 解析为实际连接对象。旧 `RemoveNode` 即使收到 `ring.Remove` 错误，也只写一条 warning，随后继续删除 map 项并关闭连接。失败时序如下：

1. ring 中仍然包含 node A，因为底层删除或重平衡失败；
2. `cl.nodes` 已删除 A，A 的连接也已关闭；
3. 后续 `LocateKey` 仍可能返回 A；
4. locator 在 map 中找不到 A，返回 `node A not found in ring`；
5. 此时调用方无法通过重试恢复，因为两个内部状态已经永久分裂。

**解决方法**

删除操作现在遵守“先准备、后提交”的顺序：

1. 在 locator 写锁内确认 node ID 存在；
2. 调用 `ring.Remove`；
3. 如果 ring 返回错误，使用 `%w` 包装并立即返回，保留 map 项和连接；
4. 只有 ring 删除成功后，才从 `nodes` map 删除节点；
5. 释放 locator 锁后关闭节点连接，避免慢 `Close` 阻塞所有定位操作。

ring 是路由事实来源，因此必须先成功更新 ring，再提交依赖它的 map。底层 `consistent-go` 自身已经保证失败的 `Remove` 不修改 ring；locator 在上层采用相同的失败原子性后，两层状态才能保持一致。

为了稳定覆盖错误分支，`ConsistentLocator.ring` 从具体 `*consistent.Consistent` 收窄为私有 `hashRing` 接口，只包含 locator 实际使用的 `LocateKey`、`LocateReplicas`、`Add` 和 `Remove`。生产环境仍注入可信的 `consistent-go` 实现；测试注入一个只让 `Remove` 返回指定错误的包装器。该接口不对外导出，不改变公共 API，也不替代一致性算法。

**回归测试**

`TestConsistentLocator_RemoveFailureKeepsNodeAndConnection` 注入确定性的 ring 删除错误，并验证：

- `RemoveNode` 返回的错误保留底层 cause，可通过 `errors.Is` 判断；
- node map 仍包含原来的两个节点；
- 待删除节点没有被关闭；
- 多个 key 在失败删除后仍能正常完成 ring 到 map 的解析，不出现 `node not found in ring`。

**涉及的知识点**

1. **跨结构不变量**：ring 返回的每个 ID 必须在 node map 中存在。单个数据结构线程安全不代表它们之间的关系安全。
2. **事务式提交顺序**：先执行可能失败、且自身具备回滚保证的底层操作；成功后再提交上层状态，避免手工反向补偿。
3. **错误链**：使用 `%w` 保存底层 cause，让调用方既能看到 node 上下文，也能通过 `errors.Is/As` 做可靠判断。
4. **资源所有权**：ring 删除成功前，连接所有权仍属于 locator，不能关闭；成功提交 map 删除后，才把连接移出锁并释放。
5. **窄接口测试替身**：依赖方只声明实际需要的方法，可以精确注入罕见错误，而不需要伪造整个开源一致性算法。

#### 第 8 项修复记录：按实际集群规模请求副本

**问题本质**

`HealthAwarePool.Get` 在主节点 unhealthy 时固定调用 `GetReplicas(ctx, key, 3)`。可信的 `consistent-go` 对“请求副本数大于成员数”返回 `ErrInsufficientMemberCount`，不会隐式截断。因此：

- 1 节点集群请求 3 个副本时直接报错，无法执行“没有替代节点则返回主节点”的既有降级语义；
- 2 节点集群同样直接报错，即使另一个健康节点完全可以接管请求；
- 只有节点数至少为 3 时，健康池的副本筛选逻辑才真正运行。

**解决方法与明确语义**

健康池在取得 primary 且确认其 unhealthy 后，读取当前 locator 的节点快照，把副本请求数设为 `min(3, len(nodes))`：

- **0 节点**：locator 在定位 primary 时立即返回 `no nodes available in ring`，健康池传播该错误；
- **1 节点**：请求 1 个副本，列表只有已知 unhealthy 的 primary；没有可替代节点时保持原有策略，返回 primary，让上层决定是否尝试或报错；
- **2 节点**：请求 2 个副本，跳过 unhealthy primary 后选择另一个健康节点；
- **3 个及以上节点**：最多检查 primary 加两个邻近候选，保持原有故障转移范围和成本。

节点数快照与 `GetReplicas` 是两个调用，动态拓扑可能在其间缩小。为此，健康池只在错误链匹配 `consistent.ErrInsufficientMemberCount` 时把请求数逐级减一并重试，直到成功或降到 1；context、ring 内部错误等其他错误仍立即返回，不会被重试掩盖。这让 3→2 或 2→1 的并发缩容继续遵守小集群降级语义。

这里没有把请求数移到 `ConsistentLocator.GetReplicas` 内部截断。底层 locator 继续严格执行调用者请求的副本数量，参数错误仍可见；只有明确采用“最多三个候选”策略的 `HealthAwarePool` 根据拓扑规模调整自己的请求。

**回归测试**

`TestHealthAwarePoolSmallClusterFailover` 覆盖 0、1、2 节点：零节点必须返回错误；单节点被标记 unhealthy 后不再收到“副本不足”错误，并按既有策略返回 primary；双节点 primary unhealthy 时必须返回另一个节点。测试使用可控 health checker 直接发出健康转换，不依赖定时器或真实网络。

**涉及的知识点**

1. **分层参数契约**：严格的底层 API 不应猜测调用方意图；上层策略负责生成满足底层前置条件的参数。
2. **优雅降级**：高可用不等于任何规模都能故障转移。单节点只能明确退回 primary，双节点则可以切换到唯一候选。
3. **边界值测试**：固定常量与集合大小相关时，必须覆盖 `0`、`1`、`N-1`、`N` 等边界，正常三节点测试无法发现该问题。
4. **快照语义**：副本数量来自当前 locator 快照；动态拓扑仍可能在两个调用间变化，locator 的错误继续向上传播，而不会被误判为健康状态。

#### 第 9 项修复记录：统一发布完整的最新服务快照

**旧行为为什么会让 Client 永久看不到变更**

`Client` 只通过 `ServiceDiscovery.Watch` 消费运行时拓扑变化。旧 `StaticServiceDiscovery` 有四条公开写路径，但只有 `UpdateNodes` 会向 watcher 发送数据：

- `Register` 新增或更新节点后直接返回；
- `Unregister` 删除节点后直接返回；
- `SetNodes` 替换完整列表后直接返回；
- `UpdateNodes` 会尝试发送，但 channel 满时直接丢弃新快照。

因此接口调用可以成功、`Discover` 也能读到新状态，但已经运行的 Client 永远停留在旧拓扑。即使调用方只使用 `UpdateNodes`，消费者短暂变慢也可能丢掉最后一次更新，且没有后续事件触发重新同步。

**统一发布路径**

新增私有 `publishLocked(serviceName)`，所有成功写操作都在同一个服务锁临界区内完成以下动作：

1. 提交新的 `services[serviceName]`；
2. 为每个 watcher 创建独立的完整快照；
3. 尝试把快照放入 watcher 的单槽 channel；
4. 如果已有待处理快照，取出旧快照并放入新快照。

`Register` 无论新增节点还是按 ID 更新节点都会发布；`Unregister` 只有真正找到并删除节点后发布；`SetNodes` 和 `UpdateNodes` 在替换列表后发布。失败的 `Unregister` 不改变状态，也不会产生虚假事件。

**为什么使用单槽 latest-snapshot mailbox**

watcher 消费的是完整拓扑状态，不是必须逐条重放的增量命令。如果连续发生 A、B、C 三次变更，而 Client 还没处理 A，保留 A、B、C 会增加延迟；只保留 C 仍包含处理下一步所需的全部信息。

channel 容量改为 1，并在满时原子地用新快照替换旧快照，形成 latest-value/coalescing 语义：生产者不会被慢 Client 阻塞，Client 醒来后最终一定能观察到当时最新的完整状态。发布和 watcher 移除都在同一把锁下完成，避免向已经关闭的 channel 发送。

`Watch` 注册 watcher 和放入初始快照也在同一个锁临界区完成。这样写操作不可能插入“已经注册但初始快照尚未生成”的窗口，避免初始状态覆盖更新状态或产生不确定顺序。

`Discover` 可能在服务名不存在时从 `default` 物化一个服务别名，因此它使用同一个写锁完成读取、别名创建和结果复制，不再先释放读锁再获取写锁。旧的锁升级窗口可能让并发 `SetNodes/Register` 已经写入的新拓扑被较早读取的 default 快照覆盖；单一临界区消除了这类 stale-write。

**快照隔离**

新增 `cloneNodeInfo/cloneNodeInfos`，复制 `NodeInfo` slice 的同时也复制每个 `Metadata` map。以下边界都使用深拷贝：

- `SetNodes` 和 `Register` 存入内部状态时，不保留调用方可变 map；
- `Discover` 返回结果时，不暴露内部 slice/map；
- `Watch` 的初始事件和每次发布都为消费者创建快照；
- 不同 watcher 不共享同一个可变 slice 或 metadata map。

这避免调用方或某个 watcher 在无锁情况下修改内部状态，也避免一个 watcher 的修改污染其他消费者。

**回归测试**

- `TestStaticServiceDiscoveryMutationsNotifyWatchers` 依次验证 Register 新增、Register 更新、SetNodes、Unregister 和 UpdateNodes，每一步都收到对应完整快照；
- `TestStaticServiceDiscoverySlowWatcherEventuallyReceivesLatestSnapshot` 在不消费中间事件的情况下连续写入 20 次，验证最终读取到第 20 次的最新地址；
- `TestStaticServiceDiscoverySnapshotsAreIsolated` 修改调用方输入和 watcher 收到的 metadata，验证内部 Discover 结果不受影响。
- `TestStaticServiceDiscoveryConcurrentDiscoverDoesNotOverwriteMutation` 反复竞争 default fallback 和显式 SetNodes，验证显式拓扑不会被旧默认快照覆盖。

**涉及的知识点**

1. **状态流与事件流**：完整快照允许安全合并中间更新；增量事件则通常必须保序、不可随意丢弃。选择 channel 策略前必须先定义数据语义。
2. **最终可见性**：非阻塞发送不能简单 `default` 丢弃最新值。单槽覆盖让慢消费者至少能得到最新状态。
3. **注册原子性**：watcher 注册与初始值发布需要和写操作串行化，否则会出现订阅建立时的检查-使用竞态。
4. **不可变快照**：复制 slice 只复制 map 指针；包含引用类型的结构体需要递归复制，才能让消费者真正拥有独立快照。
5. **锁与 channel 关闭协议**：发布者和取消清理都持有同一把 mutex 修改 watcher 列表；移除完成后再关闭 channel，确保不会发生 send-on-closed-channel。

**本轮边界**

- `UpdateNodes` 仍按地址下标生成 node ID，地址重排导致节点身份变化属于第 10 项；
- Client 应用节点快照时的 map 遍历顺序仍属于第 11 项；
- `Locator.AddNode/RemoveNode` 仍使用 `context.Background()`，可取消接口改造属于第 22 项；
- 当前 watcher 模型保证最终观察到最新快照，不承诺逐条观察每一个中间拓扑。

**本轮验证结果**

- 修改前的定向测试分别复现：ring 删除错误被吞掉；1/2 节点请求 3 副本返回 `insufficient number of members`；Register/Unregister/SetNodes 不发事件且慢 watcher 收不到最新状态。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run 'TestStaticServiceDiscovery|TestHealthAwarePoolSmallClusterFailover|TestConsistentLocator_RemoveFailureKeepsNodeAndConnection' -count=50`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro/... -count=20`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go vet ./...`：通过。
- `git diff --check`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./... -count=1`：核心、Redis、gRPC 和 fake server 包通过；既有集成场景仍失败于第 21 项记录的测试契约错误，即 helper 把 native gRPC client 当作 `gyro.Node` 后得到 `unknown` 地址。本轮已不再出现 1/2 节点副本数量错误。
- 当前环境没有可用 C compiler，且使用 `CGO_ENABLED=0`，因此未执行 race detector；新增的并发 Discover/SetNodes 回归测试通过 1000 次竞争迭代，race detector 仍应在具备 CGO toolchain 的 CI 中执行。

10. **节点 ID 按地址列表下标生成，重排会造成全量节点 churn**  
    位置：`gyro/gyro.go:50-77`。地址顺序变化会把同一地址映射成新的 `node-N`，Client diff 会删除/新增所有受影响节点，破坏稳定分区和增量重平衡。  
    建议：要求发现层提供稳定 ID；静态实现至少按地址保持 ID 映射，或拒绝重复/空地址。

11. **节点变更的 map 遍历顺序不确定，可能让不同进程得到不同分区结果**  
    位置：`gyro/gyro.go:579-605,624-664`。`nodesToAdd`/`nodesToUpdate` 从 map 构造并按随机迭代顺序调用 `ring.Add`；有界负载的增量放置可能依赖加入顺序。  
    建议：按稳定的 node ID 排序后应用变更，或对完整快照使用确定性的重建/事务提交。

12. **module path 阻塞外部发布**  
    位置：`go.mod:1`，README 已知限制。module 是 `gyro`，仓库地址却是 `github.com/focusandinsist/gyro`，外部无法直接 `go get`。  
    建议：上线前将 module path、README 示例、导入路径和 CI 一次性统一。

#### 第 10 项修复记录：静态地址使用稳定节点身份

**问题本质**

旧的 `NewStaticServiceDiscovery` 和 `UpdateNodes` 使用地址在 slice 中的下标生成 `node-1`、`node-2`。下标描述的是“本次快照中的位置”，不是后端实例身份。假设初始列表为 `[A, B, C]`，重排后为 `[C, A, B]`：

- A 的 ID 从 `node-1` 变为 `node-2`；
- B 的 ID 从 `node-2` 变为 `node-3`；
- C 的 ID 从 `node-3` 变为 `node-1`。

Client 按 ID 做增量 diff，因此会把三台地址未变的机器全部识别成连接更新。结果是关闭并重建全部连接、健康统计重置、ring 成员身份整体交换，并造成不必要的分区迁移。

**解决方法**

地址列表统一通过 `nodeInfosFromAddresses` 转换为 `NodeInfo`，静态发现生成的 ID 直接使用地址字符串。地址是这条便捷 API 唯一可用的稳定实例标识，因此同一地址无论位于列表哪个位置都得到同一 ID。显式使用 `Register/SetNodes` 的调用方仍可提供自己的稳定 ID，不受该策略限制。

转换函数同时按地址去重并保留第一次出现的顺序。重复地址描述的是同一个网络端点，不再生成两个最终会竞争同一连接身份的节点。地址真正变化时 ID 也变化，Client 会执行一次明确的旧节点删除和新节点添加，这符合静态地址 API 的语义。

**为什么不继续使用递增序号或保存下标映射**

在 discovery 对象内保存“地址到递增 ID”的历史映射虽然也能应对重排，但删除后重新加入、多个 discovery 实例和进程重启后可能得到不同 ID。直接从地址派生身份是纯函数，不依赖历史状态，因此多个进程看到相同地址集合时会独立得到相同 ID。

**回归测试**

- `TestStaticServiceDiscoveryAddressIDsSurviveReordering` 记录每个地址的初始 ID，重排地址后验证映射完全不变；
- `TestClientAddressReorderDoesNotRecreateNodes` 建立真实 Client/locator，重排相同地址集合后验证每个节点仍是原来的连接对象，而不是仅验证 ID 文本。

**涉及的知识点**

1. **Stable Identity**：实体身份必须来自实体本身的稳定属性，不能来自集合中的临时位置。
2. **幂等快照**：相同成员集合的不同排列应产生相同的逻辑拓扑，重复应用不能制造资源 churn。
3. **纯函数派生**：不依赖进程历史的 ID 派生能让重启、多实例和独立消费者得到一致结果。
4. **连接所有权**：ID 稳定后 Client 才能识别“连接仍属于同一节点”，保留连接、健康统计和 ring 成员。

#### 第 11 项修复记录：按 node ID 确定性应用拓扑

**问题本质**

Client 把当前节点和新快照转换为 Go map 后计算删除、添加和更新集合。Go 明确不保证 map 迭代顺序，同一份数据在不同进程、不同运行甚至相邻两次遍历中都可能产生不同顺序。

底层 `consistent-go` 使用有界负载和增量分区迁移。逐个 Add/Remove 的中间状态会影响后续负载调整，因此“最终成员集合相同”不足以保证“最终分区表相同”。两个 Client 如果按不同顺序加入相同节点，可能把同一 key 路由到不同后端，破坏客户端分片最重要的一致性前提。

**解决方法**

所有会改变 ring 的批量路径现在都按 node ID 升序执行：

- `buildLocatorUnsafe` 对首次 Discover 返回的节点排序后再创建连接和 AddNode，消除服务发现返回顺序差异；
- `handleServiceNodesChange` 对 `nodesToRemove` 使用 `sort.Strings`；
- 对 `nodesToAdd` 和 `nodesToUpdate` 按 `NodeInfo.ID` 排序；
- 操作阶段仍保持“删除组 → 添加组 → 更新组”的既有顺序，但每组内部不再依赖 map 顺序。

排序键使用稳定 node ID，而不是地址或快照下标。这样显式 discovery 实现和静态地址 discovery 都遵守同一确定性规则。

**回归测试**

- `TestClientAppliesTopologyChangesInNodeIDOrder` 使用记录型 locator，在 100 次独立运行中验证删除顺序为 B、D，添加顺序为 A、C，更新 E 的 remove/add 位于最后；
- `TestClientInitialRingIsDeterministicAcrossDiscoveryOrder` 用相同四个节点的两种排列构建两个真实 Client，并验证 1000 个 key 的最终 owner 完全一致；
- 前一项地址重排测试同时验证排序和稳定 ID 的组合不会触发删除/重建。

**涉及的知识点**

1. **Go map 随机迭代**：map 适合集合查找，不可作为需要可复现顺序的执行计划。
2. **确定性状态机**：当操作不是交换律或中间状态影响结果时，所有参与者必须使用相同的规范顺序。
3. **Canonical Ordering**：稳定 ID 排序把任意输入排列规范化为唯一执行序列。
4. **结果级测试**：只断言调用顺序不足以证明一致性；对大量 key 比较两个真实 ring 的 owner 才验证最终业务属性。

#### 第 12 项修复记录：对齐 Go module 与公开仓库路径

**问题本质**

Go module identity 是依赖解析、源码 import 和版本发布的统一坐标。旧 `go.mod` 声明 `module gyro`，但公开仓库位于 `github.com/focusandinsist/gyro`。外部项目执行 `go get github.com/focusandinsist/gyro/...` 时，下载到的 module 自声明为另一个路径；源码还使用 `gyro/gyro` 这类只在本地同名 module 中成立的 import，因此无法作为正常公开依赖使用。

**解决方法**

- `go.mod` module 改为 `github.com/focusandinsist/gyro`；
- 核心适配器导入统一为 `github.com/focusandinsist/gyro/gyro`；
- Redis/gRPC 包、单元测试、被 `.gitignore` 隐藏的集成测试和 testbed 全部更新为公开路径；
- README 与 quickstart 示例同步更新，删除“只能本地 clone”的已失效限制；
- quickstart 安装命令改为 `go get github.com/focusandinsist/gyro/gyro@latest`；
- 执行 `go mod tidy`，再用 `go list ./...` 验证所有包都解析到新的 module namespace。

仓库当前把核心包放在根目录下的 `gyro/` 子目录，因此公开核心 import 是 `github.com/focusandinsist/gyro/gyro`，适配器分别是 `/gyro/redis` 和 `/gyro/grpc`。本轮不移动目录，避免把发布路径修复扩大为包布局重构。

**涉及的知识点**

1. **Module identity**：`module` 指令必须与版本标签所在仓库的 canonical import prefix 一致。
2. **Internal self-import**：同一 module 内的跨包 import 也必须使用完整 module path，不能依赖本机 GOPATH 或目录名巧合。
3. **发布原子性**：go.mod、源码、测试和文档示例必须在同一次变更中切换，否则仓库会处于内部可编译但用户示例失效，或反之的半迁移状态。
4. **包路径与 module 路径不同**：module 对齐仓库根不代表核心包自动位于根；实际子目录仍是 import path 的组成部分。

**本轮验证结果**

- 修改前，`TestStaticServiceDiscoveryAddressIDsSurviveReordering` 复现地址 `127.0.0.1:8003` 因重排从 `node-3` 变为 `node-1`；`TestClientAppliesTopologyChangesInNodeIDOrder` 复现 `node-c` 先于 `node-a` 加入 ring。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro -run 'TestStaticServiceDiscoveryAddressIDsSurviveReordering|TestClient(AddressReorder|InitialRing|AppliesTopology)' -count=50`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro/... -count=20`：通过，输出包路径全部为 `github.com/focusandinsist/gyro/...`。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go vet ./...`：通过。
- `git diff --check`：通过。
- `go list -m` 返回 `github.com/focusandinsist/gyro`；`go list ./...` 成功解析核心、Redis、gRPC、fake server、integration scenarios 和 testbed。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./... -count=1`：核心、Redis、gRPC 和 fake server 包通过；既有集成测试仍失败于第 21 项记录的 `unknown` 地址契约错误，与稳定 ID、排序或 module 解析无关。
- 当前环境没有可用 C compiler，且使用 `CGO_ENABLED=0`，因此未执行 `go test -race`。

**提交注意事项**

当前 `.gitignore` 仍忽略 `docs/` 和 `test/`。本轮已同步修改 quickstart、审查文档、功能文档及集成测试 import，但普通 `git add .` 不会包含这些文件；在第 20 项修复 `.gitignore` 前，需要显式使用 `git add -f docs/... test/...` 才能让 module 迁移在干净 checkout 中保持完整一致。

### P2：正确性边界、并发性能和工程质量

13. **静态发现和配置对象只做浅拷贝，外部可绕过锁修改内部状态**  
    位置：`gyro/gyro.go:85-91,100-120,207-215`、`gyro/config.go:58-79,87-99`。`NodeInfo.Metadata` map、NodeInfo slice 和 ClientConfig 指针都可能与调用方共享；`GetConfig` 的 JSON 深拷贝还忽略 marshal/unmarshal 错误。  
    建议：实现显式深拷贝；ConfigManager 内部只保存副本，watcher 收到不可变快照；错误必须返回而非丢弃。

14. **配置 watcher 失败后新配置已经提交，系统处于半更新状态**  
    位置：`gyro/config.go:87-99`。先写入 `cm.config`，再依次调用 watcher；任一 watcher 报错都无法回滚，后续 watcher 也不执行。  
    建议：采用 prepare/commit 两阶段，或收集全部错误并明确“配置已提交但部分组件失败”的状态。

15. **健康检查在持有全局锁时执行网络探测**  
    位置：`gyro/health.go:79-118`。`Check` 持有 `hc.mu` 调用 `node.IsHealthy`，最多阻塞到 timeout，期间 Add/Remove、统计和配置读取都会被阻塞。  
    建议：锁内复制配置和旧状态，锁外执行探测，锁内按 node ID 提交结果并确认节点仍存在。

16. **节点更新只比较 Address，Metadata/Weight 变化会被静默丢弃**  
    位置：`gyro/gyro.go:358-366`。这与 NodeInfo 对外暴露的字段和动态配置承诺不一致。  
    建议：定义 NodeInfo 的可变字段语义；需要重建的字段全部纳入比较，权重则传递到一致性库或明确暂不支持。

17. **初始化和适配器构造失败时没有回滚已创建连接（已修复）**  
    位置：`gyro/gyro.go` 的 locator 构造路径以及 Redis/gRPC 适配器构造路径。现在临时 locator 由集中式资源所有权管理；任一 factory 或 AddNode 失败都会关闭已创建节点和 locator，适配器也会清理已加入的节点。回滚路径已由回归测试覆盖。

18. **关闭 locator 时持锁调用外部 Close（已修复）**  
    `ConsistentLocator.Close` 现在在锁内摘取并清空节点集合、重建空 ring，然后在锁外逐个关闭节点并聚合错误。慢网络关闭或节点重入不会再阻塞 Get/Add/Remove；关闭后的 locator 也不会保留旧 ring 成员。并发回归测试验证了 Close 期间仍可完成新的 locator 操作。

19. **gRPC 连接配置参数被完全忽略（已修复）**  
    `NewGRPCConnection` 现在使用 `ConnectTimeout` 约束健康检查触发的惰性建连，使用 `ReadTimeout`/`WriteTimeout` 约束 unary/stream RPC，并将 `IdleTimeout` 传入 keepalive 配置；负超时会立即返回错误。gRPC ClientConn 本身是多路复用传输，不存在 Redis 式连接池，因此 `MaxActiveConns`/`MaxIdleConns` 不再被假装映射为无效参数，代码和文档明确记录了这一边界。TLS/credentials 仍需通过后续可注入 dial-option/credentials 扩展处理，不再声称当前配置已支持 TLS。

20. **文档和集成测试被 `.gitignore` 忽略（已修复）**  
    已移除 `docs/` 和 `test/` 的忽略规则，只保留 `consistent/` 等生成物规则。干净 checkout 会包含 README 引用的文档和动态行为集成测试，后续 CI 可以直接发现并执行这些文件。

21. **集成测试本身存在契约错误和未完成场景（已修复）**
    新增 `Client.GetNodeForKey` 以及 Redis/gRPC 适配器的同名节点观察 API。集成测试不再把 `GetClientForKey` 返回的 native client 反向断言为 `gyro.Node`，地址和路由一致性直接通过节点元数据验证；Redis 场景移除了 TODO，改为稳定 key 的重复路由契约测试。故障场景使用状态轮询替代固定 Sleep，剩余的 gRPC 健康故障转移行为仍属于已有 P1 健康路由问题。

22. **上下文取消没有传递到节点增删（已修复）**
    `Locator` 新增 `AddNodeContext`/`RemoveNodeContext`，底层 ring 操作直接接收调用方 context；Client 的 service-watch 更新路径也把运行 context 传入。无 context 的方法仅作为显式使用 `context.Background()` 的便捷包装，新的可取消路径已覆盖回归测试。

23. **SetLogger 的注释与实际行为不符（已修复）**
    `Client.SetLogger` 现在同步传播到当前 HealthAwarePool 和底层 locator；HealthAwarePool 替换 locator 时也会重新注入当前 logger。传入 nil 仍恢复 discard logger，后续重建资源不会退回旧 logger。

24. **Health.LastHealthCheck 不是实际检查时间（已修复）**
    `DefaultHealthChecker` 暴露最近一次真实 probe 时间，`Client.Health` 使用该时间；尚未执行检查时返回零值，不再在读取 API 时伪造时间戳。

### P3：维护性改进

25. `gyro/gyro.go` 同时承载静态发现、Client 生命周期、重试、配置协调和统计，属于 Divergent Change；建议拆成 discovery、runtime、reconcile、metrics 模块。

26. Redis 与 gRPC 适配器在连接、Node、Client、replica/all-client 遍历上有大量重复逻辑，属于 Duplicated Code；可抽共享路由/生命周期模板，协议差异保留在连接工厂。

27. `Node` 的 ID、Address、serviceName 全部使用裸 `string`，属于 Primitive Obsession；应集中校验或引入轻量领域类型。`LoadBalancer` 目前只有未使用接口（`gyro/health.go:584-589`），在没有实际策略前应删除或记录为明确扩展点。

28. `cl`、`hc`、`hap`、`ssd`、`rn`、`gn` 等缩写降低公开实现的可读性；在重构相关模块时使用完整名称，并补充并发/生命周期注释。

## 推荐执行顺序（AI Coding Agent 多轮对话计划）

下面的每一项代表一次独立的 AI Coding Agent 对话，不是交给用户手工执行的任务。每轮开始时，AI 必须重新读取当前代码和上一轮的验证结果；每轮只完成对应范围，运行该轮测试，并在对话结束时报告改动、剩余风险和下一轮前置条件。不要跨轮提前做结构性重构。

1. **对话 1：修复 P0 健康池关闭协议。** AI 只处理 `HealthAwarePool`、健康事件 channel 和 checker worker 的停止/等待/幂等关闭；新增并发 Close、取消 context、重复关闭和关闭期间健康事件测试。完成条件：无 send-after-close、无 closed-channel busy loop，测试可重复通过。
2. **对话 2：修复 Client 生命周期。** AI 只处理 `Client.Start/Stop/Close`、运行 context、watcher 注册/注销和资源所有权；明确支持可重启，或实现一次性对象的明确错误语义。完成条件：未 Start 可 Close、重复 Close、Start→Stop→Start、context 取消均无 panic 或 goroutine 泄漏。
3. **对话 3：修复 locator 和拓扑变更原子性。** AI 处理 `ring.Remove` 错误传播、初始化/更新失败回滚、确定性 node ID、排序后的 Add/Update，以及 1/2 节点副本数量。完成条件：ring 与 node map 不会分裂，拓扑快照相同则路由结果稳定。
4. **对话 4：统一健康路由和适配器行为。** AI 让 Redis/gRPC 便捷构造函数使用健康池，并消除适配器本地 `healthy` 状态与阈值 checker 的冲突；覆盖失败、恢复、主节点不健康、所有副本不健康。完成条件：集成测试不再出现 `no healthy native client`，故障转移行为与文档一致。
5. **对话 5：实现可回滚的配置热更新。** AI 处理 locator/health/connection 配置的事务式应用、旧 pool 关闭、新 pool 健康监控重建、watcher 失败回滚，以及 interval/timeout/threshold 校验。完成条件：配置更新不泄漏连接或 goroutine，失败更新不留下半更新状态。
6. **对话 6：修复 ServiceDiscovery 事件和数据隔离。** AI 统一 `Register/Unregister/SetNodes/UpdateNodes` 的 watch 发布，深拷贝 slice/map，处理 Discover 的并发写入、事件丢失、背压和 context 取消。完成条件：每个成功拓扑变更最终都能被 Client 观察到，外部修改快照不会改变内部状态。
7. **对话 7：修复适配器配置、回滚和安全边界。** AI 处理 gRPC credentials/dial options、连接超时配置、空地址校验，以及 Redis/gRPC 构造失败时的资源回滚；补充连接配置和 TLS 相关测试或明确接口限制。完成条件：传入配置不会被静默忽略，部分构造失败不会泄漏连接。
8. **对话 8：修复测试与发布链路。** AI 修正集成测试把 native client 当作 `gyro.Node` 的错误假设，替换不是真正 gRPC/Redis 协议的 fake server，清理固定端口和异步 Sleep 依赖；同时修正 module path、`.gitignore` 和 CI 命令。完成条件：干净 checkout 包含 docs/test，`go test ./...`、`go test -race ./...`、`go vet ./...`、`gofmt -l` 均达到项目门禁要求。
9. **对话 9：做低风险结构重构。** 只有前 8 轮行为和测试稳定后，AI 才拆分 `gyro.go` 的职责、抽取 Redis/gRPC 重复生命周期逻辑、收敛裸 string 领域概念、处理未使用的 `LoadBalancer` 接口和命名问题。完成条件：重构不改变已验证的生命周期、路由、健康和配置行为。

每轮对话的固定输出应包含：修改文件、已解决的审查条目、运行的验证命令及结果、未解决风险、是否满足下一轮前置条件。若某轮测试失败，下一轮不得默认失败是环境问题，必须先定位并记录根因。

## 验证记录

#### P2-13 至 P2-16 修复摘要

- P2-13：ConfigManager 和静态发现现在统一保存、返回调用方隔离的显式副本。
- P2-14：配置 watcher 改为全部成功后才提交新快照，失败时不会发布半更新配置。
- P2-15：健康探测移到全局锁外执行，并用节点代际校验避免旧探测覆盖新节点状态。
- P2-16：节点重concile 现在比较 Address、Metadata 和 Weight，任一字段变化都会重建节点。

- `go test -c ./gyro` 和 `go test -c ./test/integration/scenarios`：编译通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test ./gyro`：通过。
- `go vet ./...`：通过。
- 默认 `go test ./...` 在当前执行环境因 `GOOS=linux` 生成 ELF 测试二进制、PowerShell 无法执行而报 `%1 is not a valid Win32 application`，不是可用于判断业务行为的结果。
- 在 Windows 目标下运行集成场景时，多个 gRPC 场景失败于 `node ... has no healthy native client`；该结果与第 5 项健康状态冲突相符。另有测试 helper 把 native client 当作 `gyro.Node` 的独立问题。
- `gofmt -l` 仍列出 `gyro/health_test.go`、`gyro/locator_test.go`、`gyro/mocks_test.go` 及多个集成测试文件，建议纳入任务 8 的 CI 门禁。

#### P2-17 至 P2-20 修复摘要

- P2-17：Client 初始化和 Redis/gRPC 适配器构造统一使用临时资源回滚；factory 或 AddNode 失败不会遗留已经创建的节点连接。
- P2-18：locator Close 先在锁内摘取节点并重建空 ring，再在锁外关闭节点并用 `errors.Join` 聚合关闭错误；Close 期间不会阻塞普通 locator 操作。
- P2-19：gRPC 连接应用拨号、RPC 读写和 keepalive 超时配置；明确 gRPC 多路复用连接不支持 Redis 式连接池字段，避免配置静默失效。
- P2-20：`.gitignore` 不再忽略 `docs/` 和 `test/`，只保留生成物目录和构建产物规则。

- `go test ./gyro ./gyro/grpc ./gyro/redis`：通过。
- 新增初始化失败回滚和 locator 并发关闭回归测试：通过。
- `gofmt -w` 已应用于本轮 Go 修改文件；完整 `go test ./...` 仍受仓库既有集成场景和当前 Windows/构建环境约束，未将其失败误归因于本轮 P2 修复。

#### P2-21 至 P2-24 修复摘要

- P2-21：新增节点元数据路由查询 API，修正所有 integration scenario 的 native-client 断言；Redis 场景移除 TODO，故障等待改为可诊断的状态轮询。
- P2-22：新增 `AddNodeContext`/`RemoveNodeContext` 并将 service-watch context 传入一致性 ring 的增删操作。
- P2-23：logger 传播到当前 pool、locator 及后续替换 locator，并补充传播回归测试。
- P2-24：Health 使用 health checker 的真实最近 probe 时间；无 probe 时返回零时间，并补充边界测试。

- `go test ./gyro ./gyro/grpc ./gyro/redis`：通过。
- `go test -run '^$' ./...`：所有包编译通过。
- `go test ./test/integration/scenarios -run '^TestHappyPath$' -count=1`：通过。
- `go test ./test/integration/scenarios -run 'TestFailoverScenario/Redis_Failover' -count=1`：通过。
- `go vet ./...`：通过。

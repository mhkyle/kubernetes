# kube-scheduler Pod 队列实现原理

**Session ID:** ses_e8a3288e1ffeb7MP2fxbJNc68Y
**Created:** 10/26/2026, 9:22:42 AM
**Updated:** 10/27/2026, 10:26:05 PM

---

## User

kube-scheduler 的 Pod 队列是怎么实现的？

---

## Assistant

**Tool: glob**

**Tool: grep**

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: read**

**Tool: read**

---

## Assistant

**Tool: grep**

**Tool: grep**

**Tool: glob**

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: read**

**Tool: read**

**Tool: read**

---

## Assistant

**Tool: grep**

**Tool: read**

**Tool: read**

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: grep**

**Tool: grep**

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: grep**

**Tool: read**

---

## Assistant

**Tool: read**

---

## Assistant

**Tool: grep**

---

## Assistant

**Tool: grep**

---

## Assistant

**Tool: read**

---

## Assistant

从这份仓库的实现看，kube-scheduler 的 Pod 队列**不是一个单独的 FIFO**，而是 `PriorityQueue` 管理的几个状态：

```text
新 Pod ──→ activeQ ──Pop──→ 调度中
             ↑                  │ 失败
             │                  ↓
             └── backoffQ ←── unschedulableEntities
                    ↑               │
                    └── 相关集群事件触发重试 ──┘
```

- **`activeQ` 是优先级堆**：调度器从这里取下一个实体。默认 `PrioritySort` 先按 Pod 优先级降序、再按入队时间升序排序；队列为空时，`Pop()` 用条件变量等待新实体。[active_queue.go:167](pkg/scheduler/backend/queue/active_queue.go#L167)、[active_queue.go:310](pkg/scheduler/backend/queue/active_queue.go#L310)、[priority_sort.go:40](pkg/scheduler/framework/plugins/queuesort/priority_sort.go#L40)
- **`unschedulableEntities` 是 map**：保存尝试调度后仍不可调度的实体，并记录哪些插件拒绝了它。它们不会一直占着 `activeQ` 的队头。[unschedulable_entities.go:24](pkg/scheduler/backend/queue/unschedulable_entities.go#L24)、[types.go:615](pkg/scheduler/framework/types.go#L615)
- **`backoffQ` 是按退避到期时间排序的堆**：防止失败的 Pod 立即反复重试。默认退避从 1 秒开始，指数增长，上限为 10 秒；代码还把普通不可调度失败和异常错误放在两个堆中处理。[backoff_queue.go:82](pkg/scheduler/backend/queue/backoff_queue.go#L82)、[backoff_queue.go:219](pkg/scheduler/backend/queue/backoff_queue.go#L219)、[scheduling_queue.go:74](pkg/scheduler/backend/queue/scheduling_queue.go#L74)

**重试的关键是事件驱动。** Node、Pod 等对象变化时，事件处理器通知队列；队列结合导致上次失败的插件及其 `QueueingHint`，判断这个事件是否值得让 Pod 重试。值得重试的 Pod 根据剩余退避时间进入 `backoffQ` 或直接进入 `activeQ`。为了避免漏掉恰好发生在调度期间的事件，队列也会记录 *in-flight* Pod 和这期间收到的事件。另有定时兜底：退避完成的实体会被转入 `activeQ`，在不可调度集合中停留过久的实体默认每满 5 分钟可被重新考虑。[scheduling_queue.go:577](pkg/scheduler/backend/queue/scheduling_queue.go#L577)、[scheduling_queue.go:1041](pkg/scheduler/backend/queue/scheduling_queue.go#L1041)、[scheduling_queue.go:1308](pkg/scheduler/backend/queue/scheduling_queue.go#L1308)

主循环调用 `SchedulingQueue.Pop()` 取实体，失败后通过 `AddUnschedulablePodIfNotPresent()` 放回相应状态。[scheduler.go:417](pkg/scheduler/scheduler.go#L417)、[schedule_one.go:64](pkg/scheduler/schedule_one.go#L64)、[scheduling_queue.go:1082](pkg/scheduler/backend/queue/scheduling_queue.go#L1082)

一个版本细节：此仓库启用了 `SchedulerPopFromBackoffQ` 默认特性时，**若 `activeQ` 为空，也可以直接从普通 `backoffQ` 取实体**；因此上面的箭头是主要生命周期示意，并不表示每次都必须先搬回 `activeQ`。[active_queue.go:320](pkg/scheduler/backend/queue/active_queue.go#L320)、[kube_features.go:1995](pkg/features/kube_features.go#L1995)

---

## User

What is the purpose of `isInPlacePodVerticalScalingSchedulerPreemptionEnabled` in PriorityQueue?

---

## Assistant

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: read**

**Tool: grep**

---

## Assistant

`isInPlacePodVerticalScalingSchedulerPreemptionEnabled` caches whether the **`InPlacePodVerticalScalingSchedulerPreemption` feature gate** is enabled.

**Its specific purpose inside `PriorityQueue` is to queue deferred-resize Pods individually, even when they belong to a PodGroup.**

The relevant logic is in [`isPodGroupMember()`](pkg/scheduler/backend/queue/scheduling_queue.go#L542):

```go
if p.isInPlacePodVerticalScalingSchedulerPreemptionEnabled {
  if resource.IsPodResizeDeferred(pod) {
    return false
  }
}
return p.isGenericWorkloadEnabled && pod.Spec.SchedulingGroup != nil
```

### Why this is needed

Normally, a Pod with a scheduling group is queued within a `QueuedPodGroupInfo`, so it participates in group scheduling.

A **deferred in-place resize** is different:

- The Pod is already assigned to a node.
- Its requested resource change has been deferred.
- The scheduler evaluates whether the resize can fit on that existing node, potentially using preemption to free resources.
- The resource adjustment is individual to that Pod; it should not trigger another gang-scheduling cycle for its entire group.

Returning `false` makes the queue treat it as a standalone `QueuedPodInfo`. This also avoids making its resize processing wait for the PodGroup object to be observed.

### Where the rest happens

The flag in `PriorityQueue` only controls this **individual-versus-group classification**. The broader feature is implemented elsewhere:

| Component | Role |
|---|---|
| [Pod event handlers](pkg/scheduler/eventhandlers.go#L154) | Add an already-assigned Pod to the scheduling queue when its resize becomes deferred; remove it when no longer deferred. |
| [DeferredPodScheduling plugin](pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go#L128) | Restrict evaluation to the Pod’s assigned node and enforce node resize-preemption policy. |
| Kubelet | Ultimately actuate the resize. |

A dedicated [compatibility test](pkg/scheduler/backend/queue/scheduling_queue_test.go#L10294) verifies that deferred-resize group members are queued individually, while ordinary group members remain grouped.

---

## User

scheduling_queue.go 中的主要流程是什么？

---

## Assistant

**Tool: read**

---

## Assistant

`scheduling_queue.go` 的主线可以概括为：**接收 Pod → 选取下一次调度对象 → 失败后暂存 → 根据事件决定何时重试。** 它负责队列状态管理，具体的节点筛选、打分、抢占由调度框架执行。

下面按当前仓库的代码，先讲普通 Pod，再讲 PodGroup 分支。

## 1. 整体状态流转

```text
                         Add()
                           │
                      PreEnqueue
                       /       \
                    通过       被阻塞
                     │           │
                     ▼           ▼
                  activeQ    unschedulableEntities
                     │           │
                   Pop()         │ 相关事件 / 超时兜底
                     │           ▼
                     ▼       QueueingHint 判断
                  in-flight      │
                  /       \      ├── 不值得重试 → 留在原处
                成功      失败    ├── 需要退避 → backoffQ
                 │         │     └── 可立即重试 → activeQ
               Done()      │                         ▲
                           └── 决定重新入哪个队列      │
                                      backoffQ ──退避结束──┘
```

`in-flight` 表示已经被取出、仍在调度或绑定中的 Pod，不是第四个待调度队列。

## 2. 初始化与后台任务

### `NewPriorityQueue()`：组装队列

入口：[scheduling_queue.go:421](pkg/scheduler/backend/queue/scheduling_queue.go#L421)

主要初始化：

| 结构 | 作用 |
|---|---|
| `activeQ` | 可以被调度器取出的优先级堆 |
| `backoffQ` | 等待退避的实体 |
| `unschedulableEntities` | 暂时不可调度的实体 map |
| `preEnqueuePluginMap` | 入队前检查插件 |
| `queueingHintMap` | 集群事件与重试判断函数的映射 |
| PodGroup 相关结构 | 维护组对象和等待中的成员 Pod |

### `Run()`：启动两个后台任务

入口：[scheduling_queue.go:500](pkg/scheduler/backend/queue/scheduling_queue.go#L500)

- **约每秒**执行 `flushBackoffQCompleted()`：把退避完成的实体移到 `activeQ`。
- **每 30 秒**执行 `flushUnschedulableEntitiesLeftover()`：检查长期滞留的实体，默认超过 **5 分钟**后重新考虑入队。

第二条是兜底机制，避免事件或 QueueingHint 不完整导致 Pod 长期无法重试。重新考虑入队仍要经过必要的 `PreEnqueue` 检查。

## 3. 新 Pod 入队：`Add()`

入口：[scheduling_queue.go:878](pkg/scheduler/backend/queue/scheduling_queue.go#L878)

普通 Pod 的调用链：

```text
Add()
  └─ addPod()
       ├─ 创建 QueuedPodInfo
       ├─ 登记 nominated pod 信息
       └─ moveToActiveQ()
            ├─ 执行 PreEnqueue 插件
            ├─ 被阻塞 → unschedulableEntities
            └─ 通过 → activeQ，并唤醒 Pop()
```

这里容易忽略的一点是：**`Add()` 并不保证 Pod 最终进入 `activeQ`。** 例如带有 scheduling gates 的 Pod，可以在真正尝试调度前就被留在不可调度集合。

## 4. 取出 Pod：`Pop()` / `Done()`

入口：[scheduling_queue.go:1355](pkg/scheduler/backend/queue/scheduling_queue.go#L1355)

这两个方法主要委托给 `active_queue.go`：

```text
Pop()
  ├─ 从 activeQ 取队首
  ├─ Attempts++
  ├─ schedulingCycle++
  └─ 登记 in-flight Pod，以及它在事件链表中的起点
```

调度器拿到 Pod 后，在队列锁之外执行调度。处理结束时调用 `Done(UID)`，移除 in-flight 记录，并清理不再需要的事件。

**当前版本的特殊路径：** 开启 `SchedulerPopFromBackoffQ` 时，如果 `activeQ` 为空，可以直接从普通 `backoffQ` 取出实体，即使退避尚未结束；异常错误对应的 backoff 堆不会走这个提前取出路径。

## 5. 调度失败：`AddUnschedulablePodIfNotPresent()`

入口：[scheduling_queue.go:1084](pkg/scheduler/backend/queue/scheduling_queue.go#L1084)

这是失败回队的核心流程：

```text
AddUnschedulablePodIfNotPresent()
  ├─ 检查是否已在某个队列中，防止重复入队
  ├─ 更新失败计数
  │    ├─ 插件拒绝：UnschedulableCount++
  │    └─ 异常错误：ConsecutiveErrorsCount++
  ├─ 更新时间戳，清除缓存的退避到期时间
  ├─ 检查本次 in-flight 期间发生的事件
  ├─ Done()，先结束旧的 in-flight 状态
  └─ requeueEntityWithQueueingStrategy()
```

最后统一按三种策略决定去向：

| 策略 | 处理 |
|---|---|
| `queueSkip` | 留在 `unschedulableEntities` |
| `queueAfterBackoff` | 退避未完成则进入 `backoffQ`；已完成则尝试进入 `activeQ` |
| `queueImmediately` | 跳过退避，尝试进入 `activeQ` |

统一分流函数在 [scheduling_queue.go:1787](pkg/scheduler/backend/queue/scheduling_queue.go#L1787)。

### 为什么要检查 in-flight 期间的事件？

避免下面的竞态：

```text
Pod 开始调度
    ↓
Node 资源发生变化，事件到达
    ↓
Pod 按旧状态判断调度失败
    ↓
如果直接放进 unschedulableEntities，就错过了刚才的唤醒事件
```

因此，失败回队前会通过 `determineSchedulingHintForInFlightPod()` 回看这些事件，判断是否应该重新尝试。

## 6. 集群事件触发重试：`MoveAllToActiveOrBackoffQueue()`

入口：[scheduling_queue.go:1777](pkg/scheduler/backend/queue/scheduling_queue.go#L1777)

Node 新增、资源变化、其他 Pod 删除等事件会进入这条路径：

```text
MoveAllToActiveOrBackoffQueue()
  └─ moveAllToActiveOrBackoffQueue()
       ├─ 是否有插件关心这个事件？
       ├─ collectEntitiesToEvaluate()：收集候选实体
       └─ moveEntitiesToActiveOrBackoffQueue()
            ├─ 排除与 gating 插件无关的事件
            ├─ isEntityWorthRequeuing()
            │    └─ isPodWorthRequeuing()
            │         └─ 调用相关失败插件的 QueueingHintFn
            ├─ 根据策略重新入队
            ├─ 为 in-flight Pod 记录事件
            └─ 唤醒等待中的 Pop()
```

**名字虽然叫 `MoveAll`，并不意味着所有 Pod 都会被移走。** 它只移动被判断为值得重试的实体。

`QueueingHint` 的主要判断规则是：

- 普通失败插件返回 `Queue`：重试，但遵守退避。
- `PendingPlugins` 中的插件返回 `Queue`：立即重试，跳过退避。
- 返回 `QueueSkip`：这个事件不足以触发重试。
- Hint 函数报错：保守地按 `Queue` 处理，避免 Pod 卡住。

开启 `SchedulerPreQueueingHints` 时，还可以先缩小候选 Pod 集合，减少逐个判断的开销。

## 7. Pod 自身变化：`Update()` / `Delete()` / `Activate()`

### `Update()`

入口：[scheduling_queue.go:1373](pkg/scheduler/backend/queue/scheduling_queue.go#L1373)

根据当前所在位置分别处理：

| Pod 所在位置 | 更新行为 |
|---|---|
| in-flight | 记录更新事件，避免重复入队 |
| `activeQ` | 更新 Pod 信息，调整堆 |
| `backoffQ` | 更新 Pod 信息 |
| `unschedulableEntities` | 更新后用 QueueingHint 判断是否值得重试 |
| 各结构中均不存在 | 按新 Pod 加入 |

### `Delete()`

从对应队列或 PodGroup 中移除 Pod，并清理 nomination 等关联信息。

### `Activate()`

供插件显式激活 Pod：尝试从 `unschedulableEntities` 或 `backoffQ` 移到 `activeQ`，仍需满足必要的 `PreEnqueue` 检查。若 Pod 正在 in-flight，则记录激活事件，供失败回队时处理。

## 8. PodGroup 是在主流程上增加的一层

普通 Pod 直接作为 `QueuedPodInfo` 入队；组成员通常被包装进根组的 `QueuedPodGroupInfo`，作为整体参与队列排序。

```text
成员 Pod 到达
  ├─ 根组对象还未观察到 → incompletePodGroupPods
  ├─ 组已在队列中       → 加入现有组
  ├─ 组正在调度中       → pendingPodGroupPods
  └─ 首个成员           → 创建组队列实体并入队
```

组调度尝试结束后，通过 `AddAttemptedPodGroupIfNeeded()` 汇总剩余成员并重新入队。

结合前面的问题：**启用原地扩缩容抢占功能后，deferred-resize Pod 会绕过这个组分支，单独排队处理。**

---

阅读这个文件时，优先沿着这三条调用链看：

```text
入队：Add → addPod → moveToActiveQ

失败：AddUnschedulablePodIfNotPresent
        → determineSchedulingHintForInFlightPod
        → requeueEntityWithQueueingStrategy

唤醒：MoveAllToActiveOrBackoffQueue
        → isPodWorthRequeuing
        → requeueEntityWithQueueingStrategy
```

其中 **`requeueEntityWithQueueingStrategy()` 是状态分流中心，`isPodWorthRequeuing()` 是事件是否值得触发重试的决策中心。**

---

## User

nominator 主要是用来做什么的？

---

## Assistant

**Tool: read**

---

## Assistant

**Tool: grep**

**Tool: grep**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: read**

---

## Assistant

**`nominator` 用来记录“某个 Pod 被提名到哪个 Node”，让后续调度把这些尚未绑定的 Pod 也考虑进去。** 最典型的用途是抢占：高优先级 Pod 已经选定目标节点，但还在等待被抢占的 Pod 退出。

它的实现位于 [nominator.go](pkg/scheduler/backend/queue/nominator.go#L31)，嵌入在 `PriorityQueue` 中。

## 1. 为什么需要它？

假设：

```text
Node A：CPU 已满，运行着低优先级 Pod L

高优先级 Pod H 需要 4 CPU
    ↓
调度失败，抢占逻辑选中 Node A
    ↓
发起删除 L，提名 H → Node A
    ↓
等待 L 退出，H 尚未绑定
```

这段等待期间，H 还不算 Node A 上实际运行的 Pod。如果其他 Pod 只看节点当前资源，就可能把为 H 腾出的空间占掉。

`nominator` 记录这份调度意图：

```text
Node A → [H]
```

之后调度其他 Pod 时，会参考这条记录，保护同等或更高优先级被提名 Pod 的调度机会。

**它是调度器内部的“软占位”，不是节点资源的硬预留，也不保证 H 最终一定绑定到 Node A。**

## 2. 内部存什么？

有两个索引：[nominator.go:46](pkg/scheduler/backend/queue/nominator.go#L46)

```go
// 按节点查：哪些 Pod 被提名到这里？
nominatedPods map[string][]podRef

// 按 Pod 查：它被提名到了哪个节点？
nominatedPodToNode map[types.UID]string
```

当前实现保存的是轻量级 `podRef`。调用 `PriorityQueue.NominatedPodsForNode()` 时，再解析出对应的 `PodInfo`，返回深拷贝。

注意：**这些是索引，不是另一条调度队列。** Pod 仍由 `activeQ`、`backoffQ` 等结构管理。

## 3. 后续调度怎样使用？

核心使用方是：

[`RunFilterPluginsWithNominatedPods()`](pkg/scheduler/framework/runtime/framework.go#L1298)

对待调度 Pod P 检查 Node A 时：

1. 查询被提名到 Node A 的 Pod。
2. 选择优先级 **大于等于 P** 的被提名 Pod，排除 P 自己。
3. 临时将它们加入节点快照和 PreFilter 状态。
4. 执行 Filter，检查考虑这些 Pod 后，P 是否仍可调度。

所以：

```text
H 的优先级 >= P
    → P 需要考虑 H 的占位

H 的优先级 < P
    → P 不需要为 H 让路
```

### 为什么有时 Filter 会执行两遍？

第一遍假设这些 nominated Pod 已在节点上；第一遍通过后，再按它们未加入的状态检查一次。

原因是不同约束的方向不同：

- **资源、反亲和性**：加入 nominated Pod 后可能更难满足。
- **亲和性**：加入 nominated Pod 后反而可能更容易满足。

两种状态都通过，才能避免依赖一个尚未真正落地的 Pod。

## 4. 为什么不直接读取 `status.nominatedNodeName`？

因为 **API 更新和 informer 同步存在延迟**。

抢占选出目标节点后，调度器先更新本地 nominator，再发送 Pod 状态更新请求。这样下一个调度周期不用等待 API/informer 同步，就能看到最新提名。

对应代码说明在 [nominator.go:63](pkg/scheduler/backend/queue/nominator.go#L63)。

`UpdateNominatedPod()` 还会处理这种情况：

```text
本地已经记录 H → Node A
    ↓
收到一条较旧的 Pod 更新，其中 nominatedNodeName 仍为空
    ↓
保留本地提名，避免被旧状态覆盖
```

## 5. 主要操作

| 方法 | 用途 |
|---|---|
| `AddNominatedPod()` | 新增、覆盖提名；显式指定空节点名可以清除提名 |
| `UpdateNominatedPod()` | 更新 Pod 关联信息，并处理本地提名与 informer 状态的同步 |
| `DeleteNominatedPodIfExists()` | 清理提名 |
| `NominatedPodsForNode()` | 获取某节点上的被提名 Pod，供调度和抢占逻辑使用 |

一句话：**队列决定“接下来调度谁”，nominator 记录“哪些 Pod 已经被提名到哪些节点”，防止后续调度忽略这些尚未完成的安排。**

---

## User

Scheduler 中 Framework 的主要作用是什么？为了实现 extend plugin 吗？

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: read**

---

## Assistant

**是的，支持插件扩展是 Framework 的核心目的；更准确地说，它是调度策略插件的运行时，负责把插件组织起来，并按统一规则执行。** Kubernetes 默认的调度策略本身也大量通过插件实现。

可以把三层职责理解为：

```text
Scheduler：控制整个调度流程
    │
    └─ Framework：管理、调用插件，处理执行结果
           │
           └─ Plugin：实现具体调度策略
```

## 1. Scheduler 与 Framework 的分工

| 组件 | 主要职责 |
|---|---|
| `Scheduler` | 从队列取 Pod、更新缓存快照、组织选节点流程、处理失败、启动异步绑定 |
| `Framework` | 初始化插件、维护各扩展点的插件列表、调用插件、汇总状态和分数、管理等待与回滚等执行语义 |
| `Plugin` | 判断资源是否足够、亲和性是否满足、节点如何打分、如何抢占或绑定等具体策略 |

例如，Scheduler 不需要直接写：

```go
checkNodeResources(...)
checkNodeAffinity(...)
checkTaints(...)
```

而是通过 Framework 执行对应扩展点的插件：

```go
RunFilterPlugins(...)
```

Framework 再调用配置好的 `NodeResourcesFit`、`NodeAffinity`、`TaintToleration` 等插件。

接口定义在 [framework/interface.go:165](pkg/scheduler/framework/interface.go#L165)，具体实现是 [runtime/framework.go:55](pkg/scheduler/framework/runtime/framework.go#L55) 的 `frameworkImpl`。

## 2. 它提供哪些扩展点？

普通 Pod 的主要流程可以简化为：

```text
PreEnqueue → QueueSort → 从队列取出 Pod
                              │
                          PreFilter
                              │
                            Filter
                           /      \
                      无可用节点   有可用节点
                         │           │
                     PostFilter   PreScore
                     如尝试抢占      │
                                   Score
                           归一化、加权汇总
                                     │
                                  选出节点
                                     │
                           Assume → Reserve
                                     │
                                   Permit
                                     │
                         ── 异步绑定阶段 ──
                                     │
                                  PreBind
                                     │
                                    Bind
                                     │
                                  PostBind
```

其中：

| 扩展点 | 作用 | 示例 |
|---|---|---|
| `PreEnqueue` | 判断 Pod 是否可以进入可调度队列 | SchedulingGates |
| `QueueSort` | 决定 Pod 排队顺序 | PrioritySort |
| `PreFilter` | 预计算、缩小候选节点范围 | 预先统计资源请求 |
| `Filter` | 判断某个节点能不能运行该 Pod | NodeResourcesFit、NodeAffinity |
| `PostFilter` | 没有可用节点时尝试补救 | DefaultPreemption |
| `PreScore` / `Score` | 给可用节点评分 | NodeResourcesFit、PodTopologySpread |
| `Reserve` / `Unreserve` | 维护、回滚插件自己的预留状态 | VolumeBinding |
| `Permit` | 允许、拒绝或暂缓进入后续绑定 | 等待外部条件满足 |
| `PreBind` / `Bind` / `PostBind` | 绑定准备、执行及收尾 | VolumeBinding、DefaultBinder |

**插件可以实现多个扩展点。** 比如 `NodeResourcesFit` 同时参与资源过滤和节点评分，不必把相关逻辑拆成两个独立插件。

## 3. Framework 不只是一个插件列表

它还负责插件执行的公共机制。

### ① 插件的装配与配置

根据 scheduler profile，决定：

- 启用哪些插件；
- 各扩展点的执行顺序；
- 插件参数；
- Score 插件权重。

同一个 kube-scheduler 进程可以有多个 profile，每个 profile 对应一套 Framework。Pod 通过 `spec.schedulerName` 选择 profile。

### ② 统一执行规则

不同扩展点的执行语义不同，例如：

- `Filter` 拒绝一个节点后，可以停止该节点后续检查；
- `Score` 汇总各插件经过归一化、加权后的分数；
- `Permit` 返回 `Wait` 后，维护等待状态和超时；
- 后续失败时，调用 `Unreserve` 清理预留状态；
- 记录插件耗时、执行结果等指标。

因此插件主要关注策略，公共执行逻辑由 Framework 处理。

### ③ 为插件提供运行环境

插件通过 `framework.Handle` 获取调度器能力，例如：

- 节点、Pod 的快照视图；
- Kubernetes client 和 informer；
- 前面讨论的 `PodNominator`；
- 激活其他 Pod 的能力；
- 等待中的 Pod 和并行执行工具。

同一次调度尝试中的插件，则通过 `CycleState` 共享计算结果：

```text
PreFilter：计算 Pod 的资源请求，写入 CycleState
    ↓
Filter：对多个节点复用这份结果
```

## 4. 和 Scheduler Extender 有什么区别？

如果你说的 “extend plugin” 是泛指扩展插件，那么理解是对的。但 Kubernetes 中还有一个专门叫 **Scheduler Extender** 的机制，两者需要区分：

| | Framework Plugin | Scheduler Extender |
|---|---|---|
| 执行位置 | kube-scheduler 进程内 | 外部 HTTP 服务 |
| 调用方式 | Go 接口调用 | HTTP 请求 |
| 扩展范围 | 覆盖调度生命周期的多个阶段 | 主要为过滤、优先级打分、抢占、绑定 |
| 自定义代码接入 | 通常编译进自定义 scheduler 二进制 | 配置外部服务地址 |

Framework 插件通常不是运行时加载的 `.so`：**配置负责启用已注册的插件，实现代码仍需在 scheduler 二进制中。**

总结：**Scheduler 定义调度流程，Framework 提供可配置的策略执行框架，Plugin 实现具体策略。插件扩展是目标，而 Framework 同时承担了让这些插件正确协作的运行时职责。**

---

## User

对于 GPU 显卡来说，nvidia.com/gpu 从哪里来的？Scheduler 怎么为这个特定的 label/marker 来打分调度？

---

## Assistant

**Tool: grep**

**Tool: grep**

**Tool: read**

**Tool: glob**

---

## Assistant

**Tool: read**

**Tool: grep**

**Tool: read**

**Tool: grep**

**Tool: read**

---

## Assistant

**Tool: grep**

**Tool: grep**

**Tool: read**

**Tool: read**

**Tool: read**

---

## Assistant

**Tool: read**

---

## Assistant

**`nvidia.com/gpu` 通常来自 NVIDIA Device Plugin，是一种扩展资源（Extended Resource），不是 Node label。** Scheduler 对它主要做两件事：

1. **Filter：节点剩余的 GPU 数量够不够？**
2. **Score：若配置了 GPU 评分，再比较候选节点的 GPU 分配比例。默认不会专门按 GPU 打分。**

下面按传统的 NVIDIA Device Plugin 路径说明。

## 1. `nvidia.com/gpu` 从哪里来？

```text
节点上的 NVIDIA GPU + 驱动
           │
           ▼
NVIDIA Device Plugin（通常以 DaemonSet 部署）
  ├─ 发现 GPU，报告设备 ID 和健康状态
  └─ 向 kubelet 注册资源名 nvidia.com/gpu
           │
           ▼
kubelet Device Manager
           │
           ▼
更新 Node.status.capacity / allocatable
           │
           ▼
Scheduler 通过 informer/cache 看到资源数量
```

比如一台节点暴露了 8 个 GPU 资源单位：

```yaml
status:
  capacity:
    nvidia.com/gpu: "8"
  allocatable:
    nvidia.com/gpu: "8"
```

Kubelet 根据 Device Plugin 报告的设备生成这些数量，相关代码在 [`ManagerImpl.GetCapacity()`](pkg/kubelet/cm/devicemanager/manager.go#L479)。

这里的 `allocatable: 8` 表示**可供 Pod 分配的总量**，并不表示当前还空闲 8 个。Scheduler 会另外统计已分配 Pod 的请求量。

> 普通独占模式下，一个资源单位通常对应一块 GPU；启用 MIG、time-slicing 等机制后，资源名或单位含义可能变化，取决于 NVIDIA 插件配置。

## 2. Pod 怎么申请？

在容器资源中声明：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-job
spec:
  containers:
  - name: worker
    image: your-gpu-image
    resources:
      limits:
        nvidia.com/gpu: 2
```

只设置 GPU `limits` 时，Kubernetes 会将其作为对应的 `requests`。也可以同时显式设置，两者必须相等。

于是 Scheduler 理解的是：

```text
这个 Pod 需要 2 个 nvidia.com/gpu 资源单位
```

它不需要理解 CUDA、显卡型号或 NVIDIA 驱动，只需按资源名匹配并做数量检查。

## 3. Filter：GPU 够不够？

由 **`NodeResourcesFit` 插件**处理：

```text
PreFilter：汇总 Pod 的资源请求
    ↓
Filter：对每个节点检查可用资源
```

入口是 [`Fit.Filter()`](pkg/scheduler/framework/plugins/noderesources/fit.go#L600)，它调用 `helper.FitsRequest()`。

对于传统扩展资源，核心判断可以简化为：

```text
Pod 请求量 <= Node Allocatable - 节点上已请求量
```

假设 Pod 请求 2 个 GPU：

| 节点 | GPU Allocatable | 已请求 GPU | 剩余 | Filter |
|---|---:|---:|---:|---|
| A | 8 | 7 | 1 | 不通过 |
| B | 8 | 4 | 4 | 通过 |
| C | 未暴露该资源 | 0 | 0 | 不通过 |

不足时会出现类似：

```text
Insufficient nvidia.com/gpu
```

**这里没有针对 `"nvidia.com/gpu"` 的专门 NVIDIA 判断分支。** 它按通用的扩展资源逻辑处理，换成 `example.com/fpga` 也是同样的数量匹配机制。

## 4. Score：默认会按 GPU 剩余数量打分吗？

**默认不会。**

当前代码中，`NodeResourcesFit` 默认评分策略是 `LeastAllocated`，默认评分资源只有：

```yaml
resources:
- name: cpu
  weight: 1
- name: memory
  weight: 1
```

对应代码：

- [默认资源列表](pkg/scheduler/apis/config/v1/defaults.go#L33)
- [默认评分策略](pkg/scheduler/apis/config/v1/defaults.go#L237)

因此，默认流程是：

```text
先按 GPU 数量过滤掉不满足要求的节点
    ↓
对剩余节点，按 CPU、内存、亲和性、拓扑分布等已配置规则评分
    ↓
选择总分最高的节点
```

一个节点空闲 4 个 GPU，另一个空闲 2 个 GPU，只要都满足请求，**并不会仅因 GPU 余量不同而自动获得不同的 GPU 分数**。

## 5. 如何让 GPU 参与打分？

可以配置现有 `NodeResourcesFit`，不必专门写 NVIDIA 插件。例如，希望把任务集中到 GPU 已使用比例更高的节点，以减少碎片：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: MostAllocated
        resources:
        - name: cpu
          weight: 1
        - name: memory
          weight: 1
        - name: nvidia.com/gpu
          weight: 10
```

这里 GPU 的权重是 **`NodeResourcesFit` 插件内部的资源权重**；该插件的最终分数，还会与其他 Score 插件一起加权汇总。

### GPU 评分示例

`MostAllocated` 的单项分数大致为：

```text
100 × (已请求 GPU + 当前 Pod 请求 GPU) / GPU Allocatable
```

假设当前 Pod 请求 2 个 GPU：

| 节点 | 总量 | 已请求 | 加入当前 Pod 后 | GPU 单项分数 |
|---|---:|---:|---:|---:|
| A | 8 | 4 | 6/8 | 75 |
| B | 8 | 0 | 2/8 | 25 |

`MostAllocated` 更偏好 A，倾向集中使用；`LeastAllocated` 则更偏好剩余比例大的节点，倾向分散。

实现可看：

- [读取扩展资源及请求量](pkg/scheduler/framework/plugins/noderesources/resource_allocation.go#L173)
- [`MostAllocated` 计算](pkg/scheduler/framework/plugins/noderesources/most_allocated.go#L30)

这些分数依据的是**资源请求量，而非 GPU 实时利用率或显存使用率**。

## 6. GPU 型号、具体哪块卡由谁决定？

这是三个不同层次：

| 需求 | 通常由谁处理 |
|---|---|
| 需要 2 个 GPU 资源单位 | `NodeResourcesFit` 做数量过滤，可配置资源评分 |
| 必须是 A100 等指定型号 | 节点标签配合 `nodeSelector` / `NodeAffinity` |
| 在选定节点上分配具体 GPU 设备 | kubelet Device Manager 与 NVIDIA Device Plugin |

例如，若节点通过 GPU Feature Discovery 等组件打上了型号标签：

```yaml
spec:
  nodeSelector:
    nvidia.com/gpu.product: NVIDIA-A100-SXM4-40GB
```

这里的 `nvidia.com/gpu.product` **才是 label**，由节点亲和性逻辑匹配。标签值应以集群实际发布的值为准。

最终流程是：

```text
型号标签/亲和性约束 + GPU 数量过滤
                 ↓
              节点评分
                 ↓
         Scheduler 选节点并绑定
                 ↓
 kubelet + Device Plugin 分配具体设备
                 ↓
          容器获得 GPU 访问能力
```

**总结：`nvidia.com/gpu` 是厂商插件发布的“可计数资源”；Scheduler 通用插件负责判断数量是否足够，GPU 是否参与评分需要看配置，而显卡型号匹配和具体设备分配分别属于另外两层。**

---

## User

针对于`nvidia.com/gpu`, scheduler是如何对齐进行实现调度的，把调度链列出来。，

---

## Assistant

**Tool: grep**

**Tool: grep**

**Tool: grep**

**Tool: grep**

---

## Assistant

**Tool: grep**

**Tool: grep**

**Tool: read**

**Tool: grep**

**Tool: read**

---

## Assistant

**Tool: read**

**Tool: read**

**Tool: grep**

**Tool: read**

**Tool: grep**

---

## Assistant

对于 `nvidia.com/gpu`，**Scheduler 通过同名扩展资源，把“Pod 请求量”和“Node 可分配量”对齐，再走通用的过滤、评分、绑定流程。** 标准 Device Plugin 模式下，Scheduler 选择节点，kubelet 与 NVIDIA Device Plugin 分配具体 GPU。

下面以 **Pod 请求 2 个 GPU、普通独占 Device Plugin 模式、默认调度器**为例列出完整调用链。

## 1. 总体链路

```text
【资源上报】
NVIDIA Device Plugin 发现 GPU
  → 向 kubelet 注册 ResourceName="nvidia.com/gpu"
  → ListAndWatch 上报设备及健康状态
  → kubelet 更新 Node.status.capacity / allocatable
  → Scheduler informer 更新节点缓存

【Pod 调度】
Pod 声明 nvidia.com/gpu: 2
  → API Server 保存 Pod，完成 requests 默认化
  → Scheduler Pod informer
  → SchedulingQueue.Add()
  → activeQ
  → ScheduleOne()
  → 根据 schedulerName 选择 Framework
  → 更新 NodeInfoSnapshot
  → PreFilter：汇总 Pod GPU 请求
  → Filter：检查节点剩余 GPU 是否足够
  → Score：对候选节点评分，GPU 是否参与取决于配置
  → 选定 Node
  → AssumePod：在 Scheduler 缓存中先计入这 2 个 GPU 请求
  → Reserve / Permit
  → 异步 PreBind / Bind / PostBind
  → API Server 将 Pod 绑定到 Node

【设备分配】
目标节点 kubelet 接收已绑定 Pod
  → Device Manager 选择具体 GPU 设备 ID
  → 调用 NVIDIA Device Plugin Allocate()
  → 将设备访问配置交给容器运行时
  → 启动容器
```

## 2. 第一步：节点端发布同名资源

NVIDIA Device Plugin 向 kubelet 注册：

```text
ResourceName = "nvidia.com/gpu"
```

通过 `ListAndWatch` 报告设备列表和健康状态。kubelet 最终发布：

```yaml
# Node
status:
  capacity:
    nvidia.com/gpu: "8"
  allocatable:
    nvidia.com/gpu: "8"
```

相关入口：

- [`server.Register()`](pkg/kubelet/cm/devicemanager/plugin/v1beta1/server.go#L159)
- [`PluginListAndWatchReceiver()`](pkg/kubelet/cm/devicemanager/manager.go#L298)
- [`GetCapacity()`](pkg/kubelet/cm/devicemanager/manager.go#L479)

**Scheduler 接收到的是资源名称和数量，不是 NVIDIA 设备 ID 列表。**

## 3. 第二步：Pod 提交同名请求

```yaml
spec:
  containers:
  - name: worker
    image: your-gpu-image
    resources:
      limits:
        nvidia.com/gpu: 2
```

只声明 limits 时，对应 requests 会被默认设置为相同值。调度时得到：

```text
Pod requests["nvidia.com/gpu"] = 2
```

Scheduler 的两个关键数据来源是：

| 数据 | 来源 | 内部表示 |
|---|---|---|
| 节点可分配 GPU 总量 | `Node.status.allocatable` | `NodeInfo.Allocatable.ScalarResources` |
| 节点已被请求的 GPU 数量 | 节点上已分配及 assumed Pod 的请求 | `NodeInfo.Requested.ScalarResources` |
| 当前 Pod 请求 GPU 数量 | Pod 的有效资源请求 | PreFilter 计算的 `Resource.ScalarResources` |

节点转换入口为 [`NodeInfo.SetNode()`](pkg/scheduler/framework/types.go#L508)，Pod 请求累计在 [`NodeInfo.update()`](pkg/scheduler/framework/types.go#L461)。

**三者使用同一个 key：`nvidia.com/gpu`。这就是资源匹配的核心。**

## 4. 第三步：Pod 入队，启动调度周期

普通 Pod 的调用链：

```text
Scheduler.addPod()
  → addPodToSchedulingQueue()
  → PriorityQueue.Add()
  → addPod()
  → moveToActiveQ()
      → PreEnqueue 检查
      → activeQ
```

消费侧：

```text
Scheduler.Run()
  → ScheduleOne()
      → NextEntity()                  // 绑定到 SchedulingQueue.Pop
      → scheduleOnePod()
          → frameworkForPod()         // 根据 spec.schedulerName 找 Framework
          → schedulingCycle()
              → Cache.UpdateSnapshot()
              → schedulingAlgorithm()
                  → SchedulePod()
```

入口：[schedule_one.go:66](pkg/scheduler/schedule_one.go#L66)。

这一步没有 GPU 专用队列。GPU Pod 与其他 Pod 一样，默认按优先级等规则排队。

## 5. 第四步：PreFilter 汇总 GPU 请求

```text
SchedulingAlgorithm.SchedulePod()
  → findNodesThatFitPod()
  → prefilterNodes()
  → Framework.RunPreFilterPlugins()
  → NodeResourcesFit.PreFilter()
      → computePodResourceRequest()
      → 写入 CycleState
```

例如计算后：

```go
podRequest.ScalarResources["nvidia.com/gpu"] = 2
```

入口：[`Fit.PreFilter()`](pkg/scheduler/framework/plugins/noderesources/fit.go#L280)。

这里会按 Kubernetes 的 Pod 资源请求规则汇总，不能简单地只读取第一个容器的 limits。

## 6. 第五步：Filter 检查 GPU 数量

```text
findNodesThatPassFilters()
  → Framework.RunFilterPluginsWithNominatedPods()
  → Framework.RunFilterPlugins()
  → NodeResourcesFit.Filter()
  → helper.FitsRequest()
      → 遍历 ScalarResources
      → 检查 nvidia.com/gpu
```

核心判断位于 [resource_fit.go:179](pkg/scheduler/framework/plugins/helper/resource_fit.go#L179)。普通新 Pod 可以简化为：

```go
requestedGPU <= allocatableGPU - alreadyRequestedGPU
```

例如当前 Pod 请求 2：

```text
Node A：8 - 7 = 1  → 不通过
Node B：8 - 4 = 4  → 通过
Node C：4 - 2 = 2  → 通过
```

不足则返回：

```text
Unschedulable: Insufficient nvidia.com/gpu
```

同一阶段还会检查 CPU、内存、taint、节点亲和性等条件。GPU 够，只代表通过了其中一项约束。

如果存在同等或更高优先级的 nominated Pod，过滤过程还会把它们临时计入检查，避免忽略它们的资源需求。

## 7. 第六步：Score 选节点

如果只剩一个候选节点，直接选中；有多个候选节点时：

```text
SchedulePod()
  → prioritizeNodes()
      → Framework.RunPreScorePlugins()
      → Framework.RunScorePlugins()
          → NodeResourcesFit.Score()
          → 其他 Score 插件
      → 归一化、加权汇总
  → 选择最高分节点
```

入口：

- [`SchedulePod()`](pkg/scheduler/algorithm.go#L144)
- [`prioritizeNodes()`](pkg/scheduler/schedule_one.go#L565)
- [`Fit.Score()`](pkg/scheduler/framework/plugins/noderesources/fit.go#L638)

**默认 GPU 只参与数量过滤，不参与 `NodeResourcesFit` 资源评分。** 默认评分资源是 CPU、内存。

若配置：

```yaml
scoringStrategy:
  type: MostAllocated
  resources:
  - name: nvidia.com/gpu
    weight: 1
```

则按 GPU 请求比例评分：

```text
GPU 分数 ≈ 100 ×（已请求 GPU + 当前 Pod 请求 GPU）/ Allocatable GPU
```

这仍是**资源请求量评分**，不是 GPU 实时利用率评分。

## 8. 第七步：Assume 计账，再绑定

选出 Node B 后：

```text
prepareForBindingCycle()
  → AssumeAndReserveInCache()
      → prepareAssumedPod()
      → Cache.AssumePod()
      → RunReservePluginsReserve()
  → RunPermitPlugins()
```

[`AssumeAndReserveInCache()`](pkg/scheduler/algorithm.go#L618) 中先调用 `Cache.AssumePod()`。

这一步很关键：

```text
绑定前：
Node B GPU 已请求量 = 4

Assume 当前 Pod 后：
Node B GPU 已请求量 = 6
```

**不必等 API Server 的绑定结果经 informer 回来，下一个调度周期就能看到这 2 个 GPU 已被计入。** 这是防止连续调度时重复使用同一份可用数量的关键机制；此时还没有分配具体显卡。

然后进入异步绑定：

```text
runBindingCycle()
  → bindingCycle()
      → WaitOnPermit()
      → RunPreBindPlugins()
      → bind()
          → RunBindPlugins()
          → DefaultBinder.Bind()
              → 提交 Pod Binding 到 API Server
      → RunPostBindPlugins()
```

[`DefaultBinder.Bind()`](pkg/scheduler/framework/plugins/defaultbinder/default_binder.go#L51) 绑定的是：

```text
Pod → Node B
```

而不是：

```text
Pod → GPU UUID-123、GPU UUID-456
```

## 9. 第八步：kubelet 分配具体 GPU

目标节点 kubelet 开始处理 Pod 后：

```text
Device Manager.Allocate()
  → devicesToAllocate()
      → 选择满足数量、可用性及相关拓扑策略的设备 ID
      → 可能调用插件 GetPreferredAllocation()
  → endpoint.allocate()
      → NVIDIA Device Plugin Allocate()
  → 保存分配结果
  → 将设备、挂载、环境变量或 CDI 等配置用于容器创建
```

代码入口：

- [`ManagerImpl.Allocate()`](pkg/kubelet/cm/devicemanager/manager.go#L405)
- [`devicesToAllocate()`](pkg/kubelet/cm/devicemanager/manager.go#L632)
- [`endpointImpl.allocate()`](pkg/kubelet/cm/devicemanager/endpoint.go#L109)

## 10. GPU 不足时的失败链

```text
NodeResourcesFit.Filter()
  → 所有候选节点均不满足
  → FitError
  → PostFilter
      → DefaultPreemption 尝试通过抢占腾出资源
      → 若找到目标，记录 nominated node，等待受害 Pod 退出
  → handleSchedulingFailure()
  → AddUnschedulablePodIfNotPresent()
  → unschedulableEntities / backoffQ

其他 Pod 删除、节点 GPU 可分配量增加等事件
  → MoveAllToActiveOrBackoffQueue()
  → NodeResourcesFit 的 QueueingHint 判断是否值得重试
  → activeQ / backoffQ
  → 新一轮调度
```

**整条链的职责边界：**

```text
NVIDIA Device Plugin：发布“有什么 GPU”
Scheduler：按资源数量和调度策略决定“去哪个节点”
kubelet + Device Plugin：决定并落实“使用哪几个 GPU 设备”
```

标准 `nvidia.com/gpu` 调度不需要额外编写 NVIDIA 专用 Scheduler Plugin；通用的 `NodeResourcesFit` 已经支持这种扩展资源。

---


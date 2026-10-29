# kube-scheduler 实现笔记

本文根据当前仓库的实现整理调度队列、提名、调度框架和传统 Device Plugin GPU 调度流程。特性门控和默认值可能随版本变化；生产环境应以实际版本和配置为准。

## 调度队列

`PriorityQueue` 不只是一个 FIFO。它管理普通 Pod 和 PodGroup 等调度实体，将可以尝试调度的实体与暂时不可调度的实体分开。

| 状态 | 用途 |
|---|---|
| `activeQ` | 可供调度器取出的优先级堆。默认 `PrioritySort` 按 Pod 优先级降序、入队时间升序排列。 |
| `unschedulableEntities` | 暂时不可调度的实体集合，记录拒绝或阻塞该实体的插件。 |
| `backoffQ` | 等待重试的实体，按退避到期时间管理；普通不可调度失败和异常错误使用不同的堆。 |
| in-flight | 已被取出、正在处理的 Pod 及其期间发生的事件；不是另一个待调度队列。 |

入口：[scheduling_queue.go](../pkg/scheduler/backend/queue/scheduling_queue.go)、[active_queue.go](../pkg/scheduler/backend/queue/active_queue.go)、[backoff_queue.go](../pkg/scheduler/backend/queue/backoff_queue.go)、[PrioritySort](../pkg/scheduler/framework/plugins/queuesort/priority_sort.go)。

### 入队、取出和失败回队

```text
新 Pod → Add → PreEnqueue ──通过──→ activeQ → Pop → in-flight
                    │                                  │
                  被阻塞                              失败
                    ↓                                  ↓
       unschedulableEntities ←──── 根据事件决定回队策略
                    │                 │
                    └─ 相关事件 ──────┴→ backoffQ → activeQ
```

这是主要流程示意，不是所有实体都必须经过每个状态：

1. `Add()` 创建队列信息并执行 `PreEnqueue`。例如，带 scheduling gate 的 Pod 可能在第一次调度前就被阻塞，因此 `Add()` 不保证进入 `activeQ`。
2. `Pop()` 取出队首，记录调度尝试和 in-flight 状态；没有可取实体时等待。处理完成后，`Done()` 清理 in-flight 记录及不再需要的事件。
3. 调度失败时，`AddUnschedulablePodIfNotPresent()` 检查重复入队，更新失败计数和时间戳，检查调度期间发生的事件，再结束 in-flight 状态并决定回队位置。错误和插件拒绝分别维护计数。
4. `requeueEntityWithQueueingStrategy()` 根据决策将实体留在不可调度集合、在退避后重试，或跳过退避立即重试；进入 `activeQ` 仍需经过必要的 `PreEnqueue` 检查。

in-flight 事件记录用于避免竞态：假如 Pod 调度期间节点资源发生变化，而调度器基于旧快照判定失败，回队前检查这些事件可以避免错过唤醒机会。

### 事件与退避

节点、Pod 等对象变化时，事件处理器调用 `MoveAllToActiveOrBackoffQueue()`。队列根据上次拒绝或阻塞 Pod 的插件及其 `QueueingHint` 判断是否值得重试，而不是把所有 Pod 一律移走。

| 决策 | 结果 |
|---|---|
| `queueSkip` | 留在不可调度集合。 |
| `queueAfterBackoff` | 若仍在退避则进入 `backoffQ`，否则尝试进入 `activeQ`。普通拒绝插件对事件返回 `Queue` 通常走此路径。 |
| `queueImmediately` | 跳过退避，尝试进入 `activeQ`。`PendingPlugins` 中的插件对事件返回 `Queue` 可触发此路径。 |

Hint 函数出错时会保守地考虑重新排队。`SchedulerPreQueueingHints` 开启时，还可以先缩小事件需要评估的候选集合。

对单个 Pod，默认退避从 1 秒开始指数增长，最大为 10 秒；配置可以调整。PodGroup 的最大退避时间还与组大小有关。后台任务约每秒检查已结束的退避，每 30 秒检查长期滞留的不可调度实体；默认停留超过 5 分钟后会被重新考虑，作为事件驱动的兜底。

当前仓库的 `SchedulerPopFromBackoffQ` 默认开启：`activeQ` 为空时，调度器可以直接从**普通** `backoffQ` 取实体，即使退避尚未结束；异常错误对应的退避堆不走这条提前取出路径。开启时，普通退避堆在同一时间窗口内还使用 activeQ 的排序规则。详见 [backoff_queue.go](../pkg/scheduler/backend/queue/backoff_queue.go) 和 [特性默认值](../pkg/features/kube_features.go)。

### 更新、删除与显式激活

- `Update()` 按 Pod 所在状态更新信息：更新 activeQ 堆、更新 backoffQ、对不可调度 Pod 评估事件，或为 in-flight Pod 记录事件。未知 Pod 可按新 Pod 加入。
- `Delete()` 从相应队列或组中删除 Pod，并清理关联的提名信息。
- `Activate()` 允许插件显式激活 Pod；若 Pod 正在 in-flight，则记录事件供回队时处理。

### PodGroup 与 deferred resize

启用 `GenericWorkload` 后，有 `spec.schedulingGroup` 的普通组成员作为组调度实体的一部分排队。组对象尚未观察到时，成员可能暂存在 `incompletePodGroupPods`；组正在调度时，新成员可能暂存在 `pendingPodGroupPods`。一次组调度结束后，`AddAttemptedPodGroupIfNeeded()` 处理剩余成员。

`PriorityQueue.isInPlacePodVerticalScalingSchedulerPreemptionEnabled` 缓存同名特性门控的状态。开启后，`isPodGroupMember()` 对 resize deferred 的 Pod 返回 false，即使它属于 PodGroup，也按单个 Pod 排队：它已经在节点上运行，需要单独评估资源调整，而不是再次进行组调度。队列中的标志只控制分类；[Pod 事件处理器](../pkg/scheduler/eventhandlers.go)负责相关入队行为，[DeferredPodScheduling 插件](../pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go)负责限制候选节点等检查，实际 resize 由 kubelet 执行。

## Nominator：记录节点提名

`nominator` 记录 Pod 到 Node 的提名，最常见于抢占：高优先级 Pod 选定一个节点后，仍需等待被抢占 Pod 退出，尚未完成绑定。提名是调度器内部的**软占位**，不保证 Pod 最终绑定到该节点，也不是硬资源预留。

[nominator.go](../pkg/scheduler/backend/queue/nominator.go) 维护两个索引：节点到被提名 Pod 的引用，以及 Pod UID 到节点。它不替代调度队列。调度器在发起 Pod 状态更新前先更新本地提名，避免 API 和 informer 延迟造成后续调度周期遗漏；更新时也处理旧 informer 状态覆盖本地提名的情况。

在 [`RunFilterPluginsWithNominatedPods()`](../pkg/scheduler/framework/runtime/framework.go) 中，调度器针对候选节点临时加入优先级大于或等于当前 Pod 的被提名 Pod，再执行 Filter。如果加入了 nominated Pod 且第一遍通过，还会在未加入它们的状态下再执行一次：资源和反亲和性可能因加入它们而失败，亲和性也可能因它们尚未实际运行而失败。两种情况下都通过才是保守选择。

主要操作包括 `AddNominatedPod()`、`UpdateNominatedPod()`、`DeleteNominatedPodIfExists()` 和 `NominatedPodsForNode()`。

## Scheduling Framework

Scheduler 控制调度流程；Framework 装配和调用插件、处理结果及执行语义；插件实现资源、亲和性、拓扑、抢占、绑定等具体策略。默认调度策略本身也大量由插件实现。

普通 Pod 的典型流程如下：

```text
PreEnqueue → QueueSort → Pop
  → PreFilter → Filter ──无可用节点──→ PostFilter（例如抢占）
                   └──有可用节点──→ PreScore → Score → 选择节点
                                      → Assume → Reserve → Permit
                                      → 异步 PreBind → Bind → PostBind
```

| 扩展点 | 作用 | 示例 |
|---|---|---|
| `PreEnqueue` / `QueueSort` | 判断是否可以入队、决定队列顺序 | SchedulingGates、PrioritySort |
| `PreFilter` / `Filter` | 预计算请求或限制候选范围，检查节点是否满足约束 | NodeResourcesFit、NodeAffinity |
| `PostFilter` | 无可用节点时尝试补救 | DefaultPreemption |
| `PreScore` / `Score` | 准备评分并对候选节点评分 | NodeResourcesFit、PodTopologySpread |
| `Reserve` / `Unreserve` | 维护插件预留状态及失败时回滚 | VolumeBinding |
| `Permit` | 允许、拒绝或等待后续绑定 | 等待插件定义的条件 |
| `PreBind` / `Bind` / `PostBind` | 绑定准备、执行与收尾 | VolumeBinding、DefaultBinder |

一个插件可以实现多个扩展点。Framework 按 scheduler profile 配置插件顺序、参数和 Score 权重，统一处理状态、等待、回滚和指标；Pod 通过 `spec.schedulerName` 选择 profile。插件通过 `framework.Handle` 使用快照、client、informer、nominator 等能力，通过 `CycleState` 在同次调度尝试中共享数据。

Framework 插件通常是编译并注册在 scheduler 进程中的 Go 实现；Scheduler Extender 则是通过 HTTP 调用的外部服务，主要参与过滤、评分、抢占或绑定。参见 [Framework 接口](../staging/src/k8s.io/kube-scheduler/framework/interface.go) 和 [运行时实现](../pkg/scheduler/framework/runtime/framework.go)。

## `nvidia.com/gpu` 调度链

这里讨论传统 NVIDIA Device Plugin 发布扩展资源的路径，不讨论 DRA 设备分配。`nvidia.com/gpu` 是**扩展资源名**，不是 Node label。Scheduler 按资源名和数量选择节点，不理解具体 GPU 设备 ID、CUDA 或实时利用率。

### 1. 节点发布资源，Pod 请求同名资源

```text
NVIDIA Device Plugin 发现设备
  → 向 kubelet 注册 ResourceName="nvidia.com/gpu"
  → ListAndWatch 报告设备和健康状态
  → kubelet 更新 Node.status.capacity / allocatable
  → scheduler informer 和缓存观察到节点资源
```

例如节点 `allocatable.nvidia.com/gpu: "8"` 表示可供 Pod 分配的总量，不是实时空闲数量。已分配和 assumed Pod 的请求由 scheduler 另行统计。设备上报见 kubelet [Device Manager](../pkg/kubelet/cm/devicemanager/manager.go)。独占模式下一个单位通常对应一块 GPU；MIG 或 time-slicing 等配置可能改变资源名或单位含义。

Pod 通过容器资源声明请求：

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

只设置 GPU `limits` 时，对应的 `requests` 默认使用相同值；如果同时声明，两者必须相等。Scheduler 计算 Pod 的有效资源请求，包括适用的 Pod 资源汇总规则，而不是只读取第一个容器。

### 2. 排队、过滤和评分

```text
Pod informer → SchedulingQueue.Add → PreEnqueue → activeQ → ScheduleOne
  → 选择 scheduler profile / Framework → 更新节点快照 → SchedulePod
  → NodeResourcesFit.PreFilter：汇总 Pod 请求
  → NodeResourcesFit.Filter：按资源名检查每个候选节点
  → PreScore / Score：对多个可行节点进行评分
  → 选择节点
```

GPU Pod 没有专用队列。对于普通新 Pod 的传统扩展资源，[资源过滤](../pkg/scheduler/framework/plugins/helper/resource_fit.go)的判断可以简化为：

```text
Pod 请求 GPU ≤ Node allocatable GPU − 节点上已请求 GPU
```

例如请求 2 个 GPU 时，`8 - 7 = 1` 的节点不通过，`8 - 4 = 4` 的节点通过；未发布该资源的节点也不能满足请求。失败原因通常包含 `Insufficient nvidia.com/gpu`。节点还必须通过 CPU、内存、污点、亲和性等其他已启用的检查；nominated Pod 也可能参与过滤。

如果只有一个可行节点，直接选中；多个可行节点则执行评分。当前 [默认配置](../pkg/scheduler/apis/config/v1/defaults.go)中，`NodeResourcesFit` 使用 `LeastAllocated`，默认评分资源只有 CPU 和内存：GPU 默认参与数量过滤，但不单独参与该插件的资源评分。其他 Score 插件仍可能影响最终节点选择。

可以在 scheduler 配置中显式加入 GPU，例如使用 `MostAllocated` 倾向把 GPU 请求集中到已使用比例较高的节点：

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

GPU 单项分数大致为 `100 × (节点已请求量 + 当前 Pod 请求量) / allocatable`。例如请求 2 个 GPU，两个 8 GPU 节点已请求量分别为 4 和 0，则 GPU 单项分数分别为 75 和 25。资源权重用于插件内部计算；插件最终分数还会与其他 Score 插件的分数按插件权重汇总。对于未请求该扩展资源的 Pod，`NodeResourcesFit` 跳过该资源的评分。这些计算基于请求量，而不是 GPU 实时利用率或显存使用量。参见 [resource_allocation.go](../pkg/scheduler/framework/plugins/noderesources/resource_allocation.go) 和 [most_allocated.go](../pkg/scheduler/framework/plugins/noderesources/most_allocated.go)。

### 3. Assume、绑定和设备分配

选定节点后，scheduler 在缓存中 `AssumePod()`，先把该 Pod 的请求计入节点，然后运行 `Reserve` 和 `Permit`。这样后续调度不必等待 API 绑定结果经 informer 返回，就能看到已假定的资源请求，避免重复使用相同容量。随后进入异步绑定阶段，经过 `PreBind`、`Bind` 和 `PostBind`；默认 Binder 向 API Server 提交 Pod 到 Node 的绑定。参见 [algorithm.go](../pkg/scheduler/algorithm.go)、[schedule_one.go](../pkg/scheduler/schedule_one.go) 和 [DefaultBinder](../pkg/scheduler/framework/plugins/defaultbinder/default_binder.go)。

绑定的是 Pod 到 Node，而非具体 GPU ID。目标节点 kubelet 随后通过 Device Manager 选择设备 ID，可调用插件的 `GetPreferredAllocation()`，再调用 Device Plugin 的 `Allocate()`，将返回的设备、挂载、环境变量或 CDI 等配置用于容器创建。

如果没有可行节点，调度器可通过 `PostFilter` 尝试抢占；找到目标时可以提名节点并等待受害 Pod 退出。失败的 Pod 回到队列，之后 Pod 删除或节点资源变化等相关事件可通过 QueueingHint 触发重试。抢占并不保证一定能找到可行节点。

GPU 型号与具体设备是另外两层：型号通常通过节点 label 加 `nodeSelector` 或 `NodeAffinity` 约束，标签值取决于集群；具体 GPU 由 kubelet 和 Device Plugin 在选定节点上分配。传统 `nvidia.com/gpu` 数量调度不需要专门的 NVIDIA Scheduler 插件。

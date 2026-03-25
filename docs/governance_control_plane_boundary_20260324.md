# AIWF Governance Control Plane Boundary

日期: 2026-03-24

## 结论

AIWF 现在已经不是“只有一个控制面”的仓库。

明确判断：

- `apps/base-java` 仍然是 **job lifecycle control plane** 的正式 owner
- `apps/glue-python` 已经是 **governance state control plane** 的事实 owner
- 这意味着 `glue-python` 现在是 **事实上的第二控制面**

这不是语气强化，而是代码事实。

## 证据

`apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`

- 创建 job
- 保证 job workspace
- 维护 step fail / audit
- 组装 `GlueRunFlowReq`
- 传输 `job_context`

这说明 `base-java` 仍拥有：

- job lifecycle
- orchestration API
- `job_context` transport

`apps/glue-python/app.py`

当前 `/governance/...` 路由已经覆盖：

- quality rule sets
- workflow sandbox rules
- workflow sandbox autofix state
- workflow apps
- workflow versions
- manual reviews
- workflow runs / audit events
- run baselines

这说明 `glue-python` 已拥有：

- governance state persistence
- governance read / write API
- workflow authoring artifacts 的实际后端宿主

## 边界声明

### `base-java` 拥有

- job lifecycle control plane
- job / step / artifact orchestration
- `job_context` transport
- 对 glue-python / runtime 的调用编排

### `glue-python` 拥有

- governance state control plane
- local-first governance store host
- workflow authoring artifacts 的治理后端
- governance query / update surface

### `accel-rust` 拥有

- operator semantics
- operator metadata authority
- operator capability truth

### `apps/dify-desktop` / `apps/dify-native-winui` 拥有

- authoring UX
- workflow editing surface
- local shell / operator consumption UI

## 明确禁止

1. 不允许把新的 job lifecycle semantics 塞进 `glue-python`
2. 不允许把 governance state owner 重新塞回 desktop local store 作为新默认
3. 不允许在 `base-java` 与 `glue-python` 中双写同一份治理状态真相
4. 不允许把 Rust operator authority 复制成 Python 或 desktop 自有 registry truth

## 当前暂时接受

接受 `glue-python` 作为 governance state control plane host，但必须承认这是一种架构现实，而不是继续把它描述成“只是 flow runtime”。

## 下一步

1. 若继续保留双控制面，必须长期维持如下拆分：
   - `base-java`: lifecycle plane
   - `glue-python`: governance plane
2. 若未来要收敛为单控制面，必须先决定：
   - governance state 是否迁回 `base-java`
   - 或承认 `glue-python` 为正式 governance control plane
3. 在做出正式收口决策前，所有新的 governance API 都必须继续显式标注：
   - state owner
   - lifecycle owner
   - control plane role

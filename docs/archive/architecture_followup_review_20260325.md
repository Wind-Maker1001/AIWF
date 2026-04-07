# AIWF Architecture Follow-Up Review (2026-03-25)

> Historical snapshot. Retained for review lineage; not current implementation authority.

## 这份文档回答什么

这不是重复 `docs/project_review_20260325.md`。
那份文档回答 2026-03-25 这个时间点的全局架构判断。
这份 follow-up 只回答三件事：

1. 双控制面边界现在到底稳定到什么程度
2. node-config authority 当前准确停在哪个阶段
3. 接下来最应该继续推进的高杠杆收口动作是什么

说明：

- 这份文档在 2026-03-28 回看仓库已落地状态后整理
- 结论以当前仓库真实实现为准，而不是以中间过程中的理想目标为准

本次判断交叉参考：

- `docs/project_review_20260325.md`
- `docs/project_review_20260324.md`
- `docs/architecture_followup_review_20260324.md`
- `docs/architecture_convergence_plan_20260320.md`
- `docs/governance_control_plane_boundary_20260324.md`
- `docs/governance_control_plane_enforcement_20260325.md`
- `docs/node_config_contract_authority_20260324.md`
- `docs/architecture_gate_inventory_20260324.md`
- `docs/architecture_gate_inventory_addendum_20260325.md`
- `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
- `apps/glue-python/app.py`
- `apps/glue-python/aiwf/governance_surface.py`
- `apps/glue-python/aiwf/governance_workflow_apps.py`
- `apps/glue-python/aiwf/governance_workflow_versions.py`
- `apps/glue-python/aiwf/node_config_contract_runtime.py`
- `apps/accel-rust/src/operator_catalog_data.rs`
- `contracts/workflow/workflow.schema.json`
- `contracts/desktop/node_config_contracts.v1.json`
- `contracts/desktop/node_config_contract_fixtures.v1.json`
- `contracts/desktop/node_config_validation_errors.v1.json`
- `contracts/rust/operators_manifest.v1.json`
- `apps/dify-desktop/renderer/workflow/workflow-contract.js`
- `apps/dify-desktop/workflow_contract.js`
- `ops/scripts/check_governance_control_plane_boundary.ps1`
- `ops/scripts/check_node_config_schema_coverage.ps1`
- `ops/scripts/check_node_config_runtime_parity.ps1`
- `ops/scripts/check_operator_catalog_sync.ps1`
- `ops/scripts/check_frontend_convergence.ps1`
- `ops/scripts/ci_check.ps1`

## 1. 双控制面边界现在是否已经稳定

明确判断：

- **在 owner / route / gate 层，已经稳定。**
- **在 authoring artifact 语义层，仍然是受控但未最终收口的中间态。**

### `base-java` 当前明确拥有的语义

`apps/base-java/src/main/java/com/aiwf/base/service/JobService.java` 仍然承载：

- job 创建
- job workspace 准备
- `GlueRunFlowReq` 组装
- `job_context` 传输
- step fail / audit
- artifact / step / job 查询

所以 `base-java` 仍然是 **job lifecycle control plane**。

### `glue-python` 当前明确拥有的语义

`apps/glue-python/app.py` 与 `apps/glue-python/aiwf/governance_surface.py` 当前明确承载：

- `/governance/quality-rule-sets`
- `/governance/workflow-sandbox/*`
- `/governance/workflow-apps`
- `/governance/workflow-versions`
- `/governance/manual-reviews`
- `/governance/workflow-runs`
- `/governance/workflow-audit-events`
- `/governance/run-baselines`
- `/governance/meta/control-plane`

而且 `governance_surface.py` 现在已经显式写明：

- `glue-python` = `governance state control plane`
- `base-java` = `job lifecycle control plane`
- `lifecycle_mutation_allowed = false`

所以今天更准确的说法不是“像第二控制面”，而是：

- `glue-python` 已经是被代码、manifest、generated asset、gate 同时承认的 **governance state control plane**

### 仍然可能越界的点

当前最接近越界风险的地方不是 route owner 漂移，而是 authoring artifact 语义：

1. `workflow_run_audit` 虽然当前归在 governance / observability 侧，但如果它开始回写 step/job lifecycle，就会与 Java lifecycle plane 贴边。
2. `workflow_apps` / `workflow_versions` 现在保存的是 authoring artifacts，但它们还没有完全收口到“按同一套 contract-covered node-config semantics 拒绝非法 graph”。
3. desktop 如果重新把 governance state 回落成默认 local owner，会直接破坏这轮双控制面边界。

### 结论

双控制面边界现在已经**在 owner 层稳定**，但稳定并不意味着单控制面回归。
真实状态是：

- 承认双控制面现实
- 然后通过 metadata、generated asset、gate、package/release wiring 去约束它

这个方向是对的，但代价是治理层会更重。

## 2. `node config` authority 现在到底停在哪个阶段

明确判断：

- 当前阶段不是“纯 JS authority”
- 也不是“runtime 只是消费完整 contract authority”
- 当前准确阶段是：**contract-backed rule authorship + duplicated runtime interpreters + partial governance adoption**

### 已经收口到 `contracts/desktop` 的部分

`contracts/desktop/node_config_contracts.v1.json` 当前已经承载：

- schema-covered node type 列表
- quality tier
- per-node validator descriptors

并且 desktop CJS / ESM generated contract modules 都已经从这份 JSON 生成：

- `apps/dify-desktop/workflow_node_config_contract.generated.js`
- `apps/dify-desktop/renderer/workflow/node_config_contract.generated.js`

这意味着：

- **per-node rule authorship** 已经离开 handwritten JS map

### 仍然留在 runtime 的语义

`apps/dify-desktop/renderer/workflow/workflow-contract.js` 仍然拥有：

- validator kind 的执行语义
- path 解析逻辑
- enum / object / array / nested object / aggregate defs / window functions / conditional rules / paired rules / allowed-op rules 的解释器

`apps/glue-python/aiwf/node_config_contract_runtime.py` 现在也拥有一套 Python 解释器。

所以当前真实情况是：

- contract 拥有“规则数据”
- JS 和 Python 仍分别拥有“规则执行器”

### 当前已经做到的事情

这轮已经真实落地的部分是：

- 新增 `apps/glue-python/aiwf/node_config_contract_runtime.py`
- 新增 shared fixtures 与 validation error contract：
  - `contracts/desktop/node_config_contract_fixtures.v1.json`
  - `contracts/desktop/node_config_validation_errors.v1.json`
- 新增 parity / coverage / runtime gate：
  - `ops/scripts/check_node_config_schema_coverage.ps1`
  - `ops/scripts/check_node_config_runtime_parity.ps1`
- desktop 与 glue-python 现在都消费同一份 contract-backed rule set

### 当前还没有做到的事情

今天必须明确写清楚一件事：

- `apps/glue-python/aiwf/governance_workflow_apps.py`
- `apps/glue-python/aiwf/governance_workflow_versions.py`

当前仍然只做：

- 顶层 graph shape 校验
- 已注册 node type 校验

它们**没有**在保存入口上完整调用 Python 版 contract-covered node-config semantics 去拒绝所有 contract-invalid config。

这与当前测试是一致的：

- workflow app/version route 现在会拒绝顶层 graph contract 错误
- 也会拒绝未注册 node type
- 但仍允许“desktop-owned node-config semantics”继续在治理平面保存

所以这轮真正准确的结论不是“governance plane 已经完全收口 node-config semantics”，而是：

- parity infrastructure 已经建起来
- governance save path 仍然是部分 adoption 状态

### 结论

node-config authority 已经明显收敛，但它还没有进入最终态。
真正剩余的硬问题已经从：

- “规则写在哪”

变成：

- “解释器是否会分叉”
- “governance save path 何时真正接入 contract-covered node-config rejection”

## 3. generated asset 链是否已经形成稳定纪律

明确判断：

- **分链条看，纪律已经清楚。**
- **从仓库全局看，还缺统一 inventory 与长期治理简化。**

### 当前已经清楚的 authority 链

#### governance capability 链

- source authority: `apps/glue-python/aiwf/governance_surface.py`
- export: `ops/scripts/export_governance_capabilities.ps1`
- manifest: `contracts/governance/governance_capabilities.v1.json`
- generated consumers:
  - `apps/dify-desktop/workflow_governance_capabilities.generated.js`
  - `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/GovernanceCapabilities.Generated.cs`
- gate: `ops/scripts/check_governance_control_plane_boundary.ps1`

#### node-config contract 链

- source authority: `contracts/desktop/node_config_contracts.v1.json`
- generated consumers:
  - `apps/dify-desktop/workflow_node_config_contract.generated.js`
  - `apps/dify-desktop/renderer/workflow/node_config_contract.generated.js`
- shared runtime consumers:
  - `apps/dify-desktop/renderer/workflow/workflow-contract.js`
  - `apps/dify-desktop/workflow_contract.js`
  - `apps/glue-python/aiwf/node_config_contract_runtime.py`
- gates:
  - `check_node_config_schema_coverage.ps1`
  - `check_node_config_runtime_parity.ps1`

#### Rust operator metadata 链

- source authority: `apps/accel-rust/src/operator_catalog_data.rs`
- manifest: `contracts/rust/operators_manifest.v1.json`
- generated consumers:
  - `apps/dify-desktop/workflow_chiplets/domains/rust_operator_manifest.generated.js`
  - `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js`
- gate: `ops/scripts/check_operator_catalog_sync.ps1`

### 当前最容易分叉的点

1. generated consumers 仍有多份输出
2. manifest-adjacent 手工逻辑仍存在于 presentation / palette / pinned order 层
3. node-config 仍是双解释器系统

### 结论

generated asset discipline 现在已经不再是附属物，而是关键基础设施。
当前风险不是“没有纪律”，而是：

- 链条级纪律已经形成
- 但仓库级 inventory 与治理层简化还没有完成

## 4. 当前最该继续推进的动作是什么

如果只选一个高杠杆动作，今天最该推进的是：

- **把 governance authoring save path 与 contract-covered node-config semantics 的关系说清楚，然后要么显式接入拒绝，要么显式声明仍是 desktop-owned semantics。**

原因：

当前最危险的不是缺 route owner，而是语义 owner 的中间态：

- parity / fixtures / error-item contract 已经落地
- 但 governance save path 仍未完全采用 Python-side contract-covered node-config rejection

如果这件事继续模糊，`glue-python` 会继续成为：

- 持有 authoring artifact
- 但不完整拥有 authoring artifact 语义收口

这会让它长期停在“弱约束 shadow authority”位置。

## 最后的结论

这轮 follow-up 最应该留下的真实判断不是“收敛已经完成”，而是：

- 双控制面边界已经稳定
- contract-backed node-config parity infrastructure 已经落地
- 但治理平面对 node-config semantics 的收口还没有走到终点

如果后续继续推进，重点应该从“再加一层 gate”转向：

- 明确语义 owner
- 删除不必要的中间态
- 防止 duplicated interpreters 成为长期现实

# AIWF Architecture Follow-Up Review (2026-03-24)

> Historical snapshot. Retained for review lineage; not current implementation authority.

## 这份文档回答什么

这不是重复 `docs/project_review_20260324.md`。
那份文档指出了风险转移。
这份 follow-up 只回答两件事：

1. 现在 AIWF 的真实架构状态到底是什么
2. 基于这个状态，下一阶段最该推进的高杠杆方向是什么

本次判断直接基于以下代码与文档：

- `docs/project_review_20260324.md`
- `apps/glue-python/app.py`
- `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
- `apps/dify-desktop/renderer/workflow/workflow-contract.js`
- `contracts/workflow/workflow.schema.json`
- `apps/accel-rust/src/operator_catalog_data.rs`
- `contracts/rust/operators_manifest.v1.json`
- `ops/scripts/*.ps1` 现有架构 gate

## 1. `glue-python` 现在到底是什么

明确判断：

- `glue-python` 已经不是“仍然只是 flow runtime”
- 它也不只是“临时治理宿主”
- 它现在是 **事实上的第二控制面**

更准确地说：

- `base-java` 仍然是 **job lifecycle control plane**
- `glue-python` 已经是 **governance state control plane**

证据：

`apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`

- 创建 job
- 准备 job workspace
- 管理 step fail / audit
- 组装 `GlueRunFlowReq`
- 传输 `job_context`

`apps/glue-python/app.py`

路由面已经直接暴露：

- `/governance/quality-rule-sets`
- `/governance/workflow-sandbox/rules`
- `/governance/workflow-sandbox/autofix-state`
- `/governance/workflow-apps`
- `/governance/workflow-versions`
- `/governance/manual-reviews`
- `/governance/workflow-runs`
- `/governance/workflow-audit-events`
- `/governance/run-baselines`

并且这些路由对应的 store owner 常量都来自 `glue-python` 自己的治理模块。

结论不能再模糊：

- `glue-python` 是治理状态的事实控制面
- 如果不承认这一点，后续就会继续把控制面语义散落在 Java / Python / desktop 三处

## 2. workflow contract 的真正 authority 在哪里

明确判断：

- **顶层 workflow contract authority** 仍主要在 `contracts/workflow/*.json`
- **高频 node config 的真实 authority** 现在主要在 desktop JS helper
- 所以当前是 **分层 authority 混合态**

证据：

`contracts/workflow/workflow.schema.json`

- 只明确顶层 `workflow_id` / `version` / `nodes` / `edges`
- 对 `node.config` 基本没有高价值 schema

`apps/dify-desktop/renderer/workflow/workflow-contract.js`

- 包含 `NODE_CONFIG_SCHEMA_IDS`
- 包含 `NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE`
- 按 node type 写了大量 typed / enum / nested_shape_constrained 规则
- 这是当前最完整、最可执行的 node config validation truth

`apps/glue-python/aiwf/governance_workflow_versions.py`

- `validate_workflow_graph()` 只做顶层检查：`workflow_id`、`version`、`nodes`、`edges`
- 没有承载 desktop 那一层高频 node config schema

结论：

- `contracts/` 目前不是“完整 workflow schema authority”
- 它是 **top-level workflow authority**
- node config authority 已经明显漂移进 `renderer/workflow/workflow-contract.js`

这是当前最需要正视的 authority 漂移。

## 3. Rust operator metadata 是否已经真正单源化

明确判断：

- 还 **没有** 完成“真正单源化”
- 但已经从“纯手工双写”进入“Rust authority + manifest export + generated consumption”的中段状态

已经实现 authority 驱动的部分：

- `apps/accel-rust/src/operator_catalog_data.rs` 是 published operator metadata 源头
- `contracts/rust/operators_manifest.v1.json` 是导出的 machine-readable authority artifact
- desktop generated modules 已经消费 manifest：
  - `apps/dify-desktop/workflow_chiplets/domains/rust_operator_manifest.generated.js`
  - `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js`
- `check_operator_catalog_sync.ps1` 已能拦截 manifest / generated module / defaults catalog / routing drift

仍然是 manifest-adjacent 手工逻辑的部分：

- `renderer/workflow/rust-operator-presentations.js`
- `renderer/workflow/rust-operator-palette-policy.js`
- `renderer/workflow/defaults-catalog.js`

这些文件不再重写 operator identity，但仍手工维护：

- 展示名
- 描述文案
- palette section
- pinned order

所以当前更准确的判断是：

- Rust 已接近 metadata authority
- 但 desktop 仍保留大量 **manifest-adjacent presentation logic**
- 生成链已足够稳定到可以拦 drift
- 还不等于“新增 operator 时桌面端完全零手工接触点”

## 4. 现有 gates 是否已经碎片化

明确判断：

- 是的，**已经开始出现治理层复杂度反噬的前兆**
- 但还没到必须粗暴合并的程度

核心边界 gate：

- `check_workflow_contract_sync.ps1`
- `check_operator_catalog_sync.ps1`
- `check_fallback_governance.ps1`
- `check_frontend_convergence.ps1`

这些分别对应：

- workflow contract
- Rust metadata authority
- fallback 退出纪律
- WinUI 主前端决策

它们不应该为了“少几个脚本”而被合并掉。

已经形成同类簇、未来适合聚合但现在不宜硬并的 gate：

- `check_governance_store_schema_versions.ps1`
- `check_local_workflow_store_schema_versions.ps1`
- `check_template_pack_contract_sync.ps1`
- `check_local_template_storage_contract_sync.ps1`
- `check_offline_template_catalog_sync.ps1`

这些都在保护 durable artifact / store versioning。
它们的语义很接近，但失败原因还不同。
现在仓库仍在快速收敛阶段，保留细粒度失败信息比强行合并更有价值。

最容易被误判的 gate：

- `check_node_config_schema_coverage.ps1`

它确实重要，但它保护的主要是 JS helper 已承载的 node config truth。
它不是“contracts/` 已经成为 node config authority”的证据。

## 5. WinUI 主前端决策是否已经进入工程现实

明确判断：

- **决策已经落仓**
- **工程现实开始收敛，但还没完全收敛**

落仓证据：

- `docs/frontend_convergence_decision_20260320.md`
- `check_frontend_convergence.ps1`
- `release_frontend_productize.ps1`
- `package_native_winui_bundle.ps1`
- `package_native_winui_msix.ps1`

工程现实已经开始收敛的证据：

- WinUI bundle / MSIX 路径已经是显式主发布链
- Electron 已被明确降为 compatibility release path
- package / release / CI gate 都开始围绕 WinUI primary frontend 运转

但还没有完全收敛的证据：

- 代码活动量依然大量集中在 `apps/dify-desktop`
- 很多新的 contract / gate / authoring logic 仍先落在 Electron compatibility surface
- 这说明“决策”比“维护负载”领先了一步

所以真实判断是：

- 决策是真的
- 收敛也是真的
- 但维护重量尚未完全从 Electron 主逻辑中脱身

## 当前最危险的 1 个真问题

**最危险的问题不是文件太大，也不是 gate 太多。**

最危险的问题是：

- `glue-python` 已经成为事实上的第二控制面
- 但这个事实直到现在都没有被正式 codify

如果继续回避这个判断，未来最容易发生的是：

- Java 继续保留“正式控制面”语言
- Python 继续扩张“实际治理控制面”能力
- desktop 继续把这两者都消费成默认后端

最终结果不是一个清晰的平台，而是两个半控制面。

## 当前最容易被误判的问题

最容易被误判的是：

- “contracts/` 已经成为 workflow schema authority”

这不是真的。

更真实的说法是：

- `contracts/workflow/*.json` 目前主要是顶层 contract authority
- 高价值 node config truth 仍主要活在 desktop JS helper

如果把当前状态误判成“schema authority 已收敛完成”，后面就会放松对 JS helper authority 漂移的警惕。

## 这次我选择推进的方向

### 主方向：Direction 2 `control plane boundary 明确化`

原因：

1. `project_review_20260324.md` 里最危险的新风险就是 `glue-python` 第二控制面化
2. 这个问题一旦不显式化，后面无论继续加 schema 还是加 gate，都会只是把第二控制面越包越厚
3. 最近几轮 schema/gate 收敛已经做了不少真实工作，这一轮优先把“谁拥有治理控制面”说清楚，杠杆更高

### 次方向：Direction 3 `gate 收敛（先做 inventory，不急着粗暴合并）`

原因：

1. 现在 gate 的问题不是“数量多所以一定要合并”
2. 真问题是没人能快速解释哪些是核心边界、哪些是 durable artifact 子边界
3. 先做 inventory 比现在就强行并脚本更稳

## 2026-03-24 继续落地：schema authority 第一刀

在完成控制面边界显式化之后，又继续推进了一步 schema authority 收敛：

- 新增 `contracts/desktop/node_config_contracts.v1.json`
- 新增 `ops/scripts/export_node_config_contracts.ps1`
- 新增 generated helper：
  - `apps/dify-desktop/workflow_node_config_contract.generated.js`
  - `apps/dify-desktop/renderer/workflow/node_config_contract.generated.js`
- `workflow_contract.js` / `renderer/workflow/workflow-contract.js` 已开始对一批高价值 node config 规则消费 contract-backed definitions，而不是继续把这批规则只留在 JS helper 里
- `check_node_config_schema_coverage.ps1` 现已把 contract JSON 与 generated helper sync 一并纳入检查

这还不是 node config authority 最终完成态，但它至少把一批高价值规则从“纯 JS authority”推进到了“contracts -> generated module -> runtime validator”的链条里。

## 本次真实推进了什么

### 1. 给 `glue-python` 治理 API 增加了显式控制面边界自描述

新增：

- `apps/glue-python/aiwf/governance_surface.py`

作用：

- 显式声明 `glue-python` 是 governance state control plane owner
- 显式声明 `base-java` 是 job lifecycle control plane owner
- 显式声明 `accel-rust` 是 operator semantics authority
- 把每个 `/governance/...` surface 的 route / owner / schema version / source_of_truth 变成统一描述层

修改：

- `apps/glue-python/app.py`

新增能力：

- `/capabilities` 现在带 `governance_surface` 与 `control_plane_boundary`
- 新增 `/governance/meta/control-plane`

这意味着“第二控制面边界”已经不只在文档里，而是进入了运行时 API。

### 2. 增加正式边界文档

新增：

- `docs/governance_control_plane_boundary_20260324.md`

这份文档明确写死：

- `base-java` = job lifecycle control plane
- `glue-python` = governance state control plane
- `glue-python` = 事实上的第二控制面

### 3. 先做了 gate inventory，而不是立刻粗暴并 gate

新增：

- `docs/architecture_gate_inventory_20260324.md`

目的：

- 区分核心边界 gate 与支持性 durable-artifact gate
- 标出未来可聚合的逻辑簇
- 避免当前阶段为了“少几个脚本”而损失失败可解释性

## 为什么没有优先继续推进 schema authority 收敛

不是因为 schema authority 不重要。
而是因为最近几轮已经真实推进了很多这类工作：

- governance/local store schema_version gates
- template pack artifact contract
- local template storage contract
- offline template catalog contract

当前更危险的短板已经不是“没有 schema”，而是：

- 这些 schema/gate 正在围绕一个未正式承认的第二控制面扩张

所以这次先把控制面边界显式化，杠杆更高。

## 下一步怎么接

### 下一优先步

把 workflow / node config authority 进一步从 JS helper 抽到可复用 contract assets。

这仍然是必须做的下一大步，因为：

- top-level workflow authority 还在 `contracts/`
- node config authority 还主要在 `renderer/workflow/workflow-contract.js`

### 再下一步

把当前 durable-artifact gates 在展示层按类别聚合，而不是先物理合并脚本。

建议最终展示分组：

- core boundary
- durable artifacts
- frontend convergence
- fallback governance

## 最后的判断

基于 2026-03-24 的代码事实，我的更新判断是：

1. `glue-python` 已经是事实上的第二控制面
2. workflow contract authority 仍是分层混合态，且 node config truth 明显偏向 desktop JS helper
3. Rust metadata authority 已进入中段收敛，但还没完全摆脱 manifest-adjacent 手工 presentation logic
4. gate 已经开始出现治理层复杂度反噬的前兆，但现在应先做 inventory，不应急于粗暴合并
5. WinUI 主前端决策已经进入工程现实，但 Electron 仍承载了过多收敛成本，负载尚未完全退坡

最重要的一句话：

AIWF 现在最大的问题已经不是缺少架构，而是必须防止“围绕第二控制面增长的治理层复杂度”成为新的架构中心。

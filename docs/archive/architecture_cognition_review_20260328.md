# AIWF Architecture Cognition Review (2026-03-28)

> Historical snapshot. Retained for review lineage; not current implementation authority.

## 范围与前提

这份文档不是代码修复记录，也不是温和建议书。
它是一次针对 AIWF 当前真实系统形态的高强度架构认知审查。

本次判断基于以下来源：

- 当前线程上下文
- 仓库目录结构
- `apps/` 下关键运行时与前端代码
- `contracts/` 下 workflow、desktop、rust、governance 契约资产
- `ops/scripts/` 下 CI、release、gate、export 脚本
- `docs/` 下现有 review、boundary、authority、convergence、fallback 文档

限制说明：

- 我没有跨对话长期记忆
- 以下结论仅基于当前线程与当前仓库状态
- 若某条证据不足以支撑硬结论，会明确标注“这是推断”

## 一句话总判断

你当前最大的架构问题不是不会设计，而是持续把本该被删除的复杂性制度化，所以 AIWF 已经实质进入多 control plane / 多 authority / 多 interpreter 形态，但仓库仍大量依赖 contract、gate 和治理文档把这种现实包装成“正在单一化”。

## 最值得警惕的 5 个真相

### 1. 你最擅长的是治理复杂性，不是删除复杂性

真相：

你现在的主要能力不是把复杂性从主路径中消灭，而是把复杂性迁移到 contract、generated artifact、gate、inventory、review 文档这一整层治理系统中。

为什么这会长期伤害架构能力：

如果“新增治理层”比“删除运行时和影子真相”更容易发生，系统最终会形成第二层元系统。
之后每次修复 drift，都要先维护这层元系统，主路径复杂性并没有降低，只是被重新分配了。

仓库里的证据：

- `ops/scripts/ci_check.ps1` 已把一整簇架构 gate 编排进主检查流，包括：
  - `check_frontend_convergence.ps1`
  - `check_governance_control_plane_boundary.ps1`
  - `check_node_config_schema_coverage.ps1`
  - `check_node_config_runtime_parity.ps1`
  - `check_fallback_governance.ps1`
- `docs/architecture_gate_inventory_20260324.md` 讨论的是 gate 的分类、职责与未来聚合，而不是如何消灭背后的对象与路径
- `docs/architecture_followup_review_20260325.md` 已经把 `source authority -> manifest -> generated consumers -> gate` 当成稳定链路
- 当前仓库中存在大量治理资产：
  - `33` 个 `check_*.ps1`
  - `106` 个 `ops/scripts` 文件
  - `16` 份带明显架构/治理意图的 review、boundary、authority、convergence、fallback 文档

我真正应该怎么改认知：

不要再把“加了 gate / schema / inventory / generated module”自动记为架构进步。
只有当它同时删除了一条运行路径、一个解释器、一个 fallback、一个影子 authority，或一类中间对象时，才算真正收敛。

### 2. 你的 single source of truth 在 node-config 语义层并不成立

真相：

`node config` 现在不是单一真相源，而是一个 `contract-backed multi-interpreter system`。
你有一个声明层 authority，但真正可执行语义仍然同时活在多个 runtime 中。

为什么这会长期伤害架构能力：

架构里最难维护的不是 contract 文件，而是解释 contract 的语义执行器。
只要 JS 和 Python 都在解释同一批规则，你就不是“单一真相源”，你是在维护一个“被 parity gate 约束的双语义系统”。

仓库里的证据：

- `contracts/desktop/node_config_contracts.v1.json` 被声明为 authority
- 桌面端解释器在：
  - `apps/dify-desktop/renderer/workflow/workflow-contract.js`
  - 其中 `validateContractBackedNodeConfig` 位于该文件中部
- Python 端解释器在：
  - `apps/glue-python/aiwf/node_config_contract_runtime.py`
  - 其中 `validate_contract_backed_node_config`
  - 以及 `validate_workflow_graph_node_configs`
- 你专门为这件事增加了两层 gate：
  - `ops/scripts/check_node_config_schema_coverage.ps1`
  - `ops/scripts/check_node_config_runtime_parity.ps1`
- `docs/node_config_contract_authority_20260324.md` 已明确写出：
  - contract authority 已经推进
  - 但实现语义仍是 implementation concern，而不是 rule ownership 本身

我真正应该怎么改认知：

单一真相源不是“有一个 contract JSON”。
单一真相源是“只有一个可执行语义 owner，其他运行时只消费其结果而不是重写解释器”。
如果还存在多解释器，你的目标应该是最终消灭解释器分叉，而不是继续增强 parity 检查。

### 3. AIWF 已经是多 control plane / 多 authority 系统，不是传统单后端系统

真相：

AIWF 的真实形态已经不是“一个后端 + 一个前端”的普通三层应用。
它已经是至少包含 job lifecycle、governance state、operator semantics、authoring UX 多重 authority 的平台系统。

为什么这会长期伤害架构能力：

如果你继续用单体/三层语言思考，所有越界都会被误判成“实现细节漂移”。
你会继续把 lifecycle、governance、operator semantics、frontend authoring 都混成“平台后端逻辑”，最终丢失真正的 owner 边界。

仓库里的证据：

- `job lifecycle truth` 在 `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
  - job 创建
  - workspace 准备
  - `GlueRunFlowReq` 组装
  - `job_context` 传输
  - step / artifact 查询
- `governance state truth` 在 `apps/glue-python/app.py` 与 `apps/glue-python/aiwf/governance_surface.py`
  - `/governance/workflow-apps`
  - `/governance/workflow-versions`
  - `/governance/manual-reviews`
  - `/governance/run-baselines`
  - 以及 control-plane metadata route
- `apps/glue-python/aiwf/governance_surface.py` 已直接写明：
  - `base-java` 是 `job lifecycle control plane`
  - `glue-python` 是 `governance state control plane`
- `operator semantics truth` 仍在 Rust：
  - `apps/accel-rust/src/operator_catalog_data.rs`
  - 导出为 `contracts/rust/operators_manifest.v1.json`
  - 再生成消费模块 `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js`
- `docs/governance_control_plane_boundary_20260324.md` 已把这些边界正式写成 boundary 文档

我真正应该怎么改认知：

停止用“backend”这个模糊词概括一切。
你接下来应该按以下语言设计系统：

- 谁拥有状态真相
- 谁拥有变更权
- 谁拥有语义 authority
- 谁只能消费、缓存、镜像或适配

如果这套语言不稳定，系统边界就不会稳定。

### 4. 你让 workflow graph 承担了过多身份

真相：

`workflow graph` 现在同时承担 authoring artifact、publish payload、version snapshot、template 内容、runtime validation 对象等多种角色。

为什么这会长期伤害架构能力：

一个对象承担越多生命周期，它的结构演化成本就越高。
以后任何 graph 结构变更，都会同时冲击编辑器、发布、版本、模板、运行与审计边界。
这不是复用，而是对象身份混叠。

仓库里的证据：

- `apps/glue-python/aiwf/governance_workflow_apps.py`
  - `graph` 被作为 workflow app 的核心负载保存
- `apps/glue-python/aiwf/governance_workflow_versions.py`
  - `graph` 又被作为 workflow version snapshot 保存
- `apps/dify-desktop/main_ipc_workflow.js`
  - 保存 workflow 时直接对 graph 做 contract assert
- `apps/dify-desktop/workflow_ipc_queue_apps.js`
  - publish workflow app 时再次对 graph 做 assert 与发布封装
- `apps/dify-desktop/renderer/workflow/template-storage-contract.js`
  - 本地模板又继续保存和校验 workflow graph

我真正应该怎么改认知：

不要再把 “workflow graph” 当通用货币对象。
至少应把以下对象显式分开：

- `WorkflowDefinition`
- `PublishedWorkflowApp`
- `WorkflowVersionSnapshot`
- `RunRequest`
- `TemplateArtifact`

不是为了面向对象漂亮，而是为了让边界演化不再互相绑定。

### 5. 你正在比删除兼容路径更快地给它们建立长期纪律

真相：

你已经很擅长给兼容路径、缓存壳、存储壳、模板壳补 `schema_version`、补 gate、补迁移与补 inventory。
这在纪律层面是进步，但它也在把一些本不该长期存在的对象制度化。

为什么这会长期伤害架构能力：

一旦一个对象被 contract 化、version 化、scorecard 化，它就更难被删除。
之后即便业务上不再需要，它也会因为“已经有契约、有测试、有治理”而继续存活。

仓库里的证据：

- `apps/dify-desktop/workflow_ipc_state.js` 对本地容器定义了整套版本边界：
  - `workflow_task_queue_store.v1`
  - `workflow_queue_control.v1`
  - `workflow_node_cache_store.v1`
  - `workflow_node_cache_metrics.v1`
  - `template_marketplace_store.v1`
- `apps/dify-desktop/renderer/workflow/template-storage-contract.js` 又把 local template storage 与 entry schema 明确长期化
- `contracts/desktop/template_pack_artifact.schema.json` 把 template pack artifact 继续稳定化
- `docs/architecture_convergence_plan_20260320.md` 明确把：
  - backend-owned governance store
  - local workflow store
  - template pack artifact
  - local template storage
  - offline template catalog
  都纳入 version discipline 与 gate
- 同时，虽然 `apps/dify-desktop/workflow_governance.js` 已经拒绝 `local_legacy`
  - `apps/dify-desktop/workflow_app_registry_store.js`
  - `apps/dify-desktop/workflow_version_store.js`
  - `apps/dify-desktop/workflow_quality_rule_store.js`
  - `apps/dify-desktop/workflow_run_baseline_store.js`
  这些 store 仍然保留了 `desktop.local_legacy` 分支语义
- 前端上，`docs/frontend_convergence_decision_20260320.md` 已宣布 WinUI 为主前端、Electron 为 compatibility shell
  但当前 Electron 侧源码体量仍远高于 WinUI 主体

我真正应该怎么改认知：

`schema_version` 不是荣誉勋章。
只有被确认是长期领域对象的东西，才值得被 version 化。
对于缓存壳、迁移壳、兼容壳和本地镜像壳，默认策略应该是删除，而不是先把它们治理成正式居民。

说明：

“这些壳对象中有一部分本不该长期存在”这一句，是推断，不是硬结论。
但从仓库当前的治理倾向看，这个推断的风险已经足够高。

## 我做对了但还没做完的事

### 1. 你已经开始承认真正的 owner，而不是继续假装只有一个控制面

这是对的。

证据：

- `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
- `apps/glue-python/aiwf/governance_surface.py`
- `docs/governance_control_plane_boundary_20260324.md`

但收口还没完成：

- 系统描述语言还常常退回“backend / frontend / adapter”这样的模糊说法
- 这不足以支撑长期多 authority 系统的设计

### 2. 你已经把一部分手写 truth 往 contract / manifest 推进

这是对的。

证据：

- `contracts/desktop/node_config_contracts.v1.json`
- `contracts/rust/operators_manifest.v1.json`
- `apps/dify-desktop/workflow_contract.js`

但收口还没完成：

- contract 已推进
- 可执行语义仍未彻底单点化
- 多解释器仍然存在

### 3. 你已经开始退役 local_legacy 与默认双主线叙事

这是对的。

证据：

- `apps/dify-desktop/workflow_governance.js`
- `docs/frontend_convergence_decision_20260320.md`
- `docs/archive/fallback_governance_20260320.md`

但收口还没完成：

- 复杂度中心仍主要落在 Electron 侧
- 一些 store 与兼容语义仍保留影子分支

## 我接下来最该停止做的 3 件事

### 1. 停止把新增 gate、inventory、generated module、scorecard 当成默认进步

只有在它同时删除运行路径、解释器、fallback 或影子 authority 时，它才算架构进步。
否则它只是治理层膨胀。

### 2. 停止让 workflow graph 继续横穿 authoring、publish、version、template、run 多个生命周期

这是当前最明显的对象身份混叠源头之一。
继续这样做，所有 schema 演化都会变成跨层联动。

### 3. 停止在战略已经决定之后，继续给第二主路径保留新增语义空间

包括但不限于：

- Electron 主路径能力继续扩张
- `desktop.local_legacy` 影子分支继续存活
- `offline_local / base_api` 超出 adapter 层的语义差异继续扩张

## 我接下来最该强化的 3 个设计原则

### 1. 一个语义域只能有一个可执行 authority

contract、schema、manifest 只能声明它。
其他 runtime 只能消费，不应各自重写解释器。

### 2. 拥有变更权的 plane 才拥有 truth

cache、mirror、generated consumer、local store、audit view 都只能是派生物。
它们不能被悄悄扶正成 owner。

### 3. 先拆对象身份，再谈 versioning

只有先明确：

- definition
- publication
- run request
- audit record
- cache envelope

这些对象的边界，后续 schema 演化才不会变成系统级联动灾难。

## 最后的结论

AIWF 现在最核心的问题，不是“边界还不清”，而是：

- 你已经很会定义边界
- 也很会治理边界
- 但还不够狠地删除边界背后的冗余现实

如果这个问题不改，你接下来会越来越像一个“高级治理者”，而不是一个“高杠杆简化者”。

对你架构能力提升最大的一步，不是再增加一层纪律，而是开始系统性地删除：

- 第二解释器
- 第二语义路径
- 第二主前端语义空间
- 不配成为长期对象的 versioned 壳

这才是真正的架构收敛。

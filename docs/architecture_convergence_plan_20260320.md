# AIWF 90 天架构收敛计划

日期范围：2026-03-20 到 2026-06-18

## 1. 这次计划解决的不是“文件太大”

AIWF 当前更接近一个多运行时平台仓库，而不是普通多语言 monorepo。

当前主要结构性问题不是目录是否分层，而是：

- workflow contract 已经存在，但没有统治 desktop import / save / run 主路径。
- Rust 已经有 operator catalog，但 desktop 仍手工维护 catalog 与 routing，缺少单一事实来源。
- `offline_local`、`base_api`、自动 fallback 同时存在，但 contract 没有先于 adapter 统一。
- 多端都在重复定义 capability、payload、operator identity，靠字符串、JSON 和人工约定同步。
- 持久化对象和本地状态仍存在 schema drift 风险，CI gate 尚未覆盖关键漂移点。

结论：

- 本次收敛的主线必须是“权威契约进入主路径并进入 gate”，而不是继续把拆文件当成核心产出。

## 2. 收敛原则

1. 单一事实来源优先于约定同步。
2. contract 先于 adapter，adapter 只负责适配执行拓扑，不拥有语义真相。
3. 能力归属必须明确，跨运行时重复定义必须可被 gate 拦住。
4. compatibility fallback 允许临时存在，但必须有 owner、移除时间和 kill condition。
5. 新 contract、新持久化对象、新 operator metadata 默认必须 versioned。
6. gate 必须进入 CI / release 入口，而不是停留在文档或手工脚本。

## 3. 权威边界与能力归属模型

权威边界摘要：

- Rust 拥有 operator 语义、operator metadata、published capabilities、operator-level contract。
- desktop 拥有 authoring UX、workflow editing、local shell、run adapter orchestration；不拥有 Rust operator truth。
- base-java 拥有 job lifecycle、control plane orchestration、`job_context` transport。
- glue-python 拥有 flow composition、preprocess/runtime composition、runtime catalog；不拥有全平台 operator registry truth。
- `offline_local` 与 `base_api` 是 adapter，不是两套语义源。

详细矩阵见：

- `docs/capability_ownership_matrix_20260320.md`
- `docs/fallback_retirement_backlog_20260322.md`

## 4. Phase 1 已落地的硬边界

本次工作已在仓内落地以下 Phase 1 foundations：

### 4.1 workflow 顶层 `version` 进入 desktop 主路径

- renderer 默认 graph 显式带顶层 `version`
- renderer store import 会对未版本化 graph 做显式迁移并记录 migration note
- run payload 显式携带 `workflow.version` 与顶层 `workflow_version`
- main workflow normalization 同样会显式补齐 `version` 并把迁移记录写入 `workflow_contract`
- save / publish / run 路径不再接受明显非法 graph 静默通过

### 4.2 focused workflow contract enforcement 进入 import / save / run

已新增 focused helper：

- `apps/dify-desktop/renderer/workflow/workflow-contract.js`
- `apps/dify-desktop/workflow_contract.js`

当前 enforcement 范围：

- 顶层 required 字段：`workflow_id`、`version`、`nodes`、`edges`
- 顶层 `nodes` 非空要求进入 save / run
- 节点 `id` / `type` 与边 `from` / `to` 基本完整性校验
- import 对缺失 `version` 执行显式迁移并记录
- save / publish 对缺失 `version` 或空 graph 直接拒绝

### 4.3 drift gate 进入仓库脚本与 CI / release 入口

已新增：

- `ops/scripts/check_workflow_contract_sync.ps1`
- `ops/scripts/check_operator_catalog_sync.ps1`

已接入：

- `ops/scripts/ci_check.ps1`
- `ops/scripts/release_productize.ps1`
- `ops/scripts/package_offline_bundle.ps1`

### 4.4 desktop operator drift 开始被 gate 约束

本次已经落地第一版 Rust operator manifest foundation，并建立第二道硬边界：

- Rust published operator catalog 必须被 desktop catalog 与 Rust routing 识别
- desktop Rust routing 不允许引用 Rust workflow truth 中不存在的 operator
- desktop catalog 不允许暴露没有 runtime backing 的节点

同时，本次删除了一个已失去 Rust truth 的陈旧节点：

- `evidence_conflict_v1`

并把 `ds_refine` 从“仅 UI alias”补成了有 runtime backing 的 alias，避免 catalog 与执行面继续漂移。

### 4.5 authoring / preflight 节点目录边界

2026-03-24 新增的 Phase 1 硬边界：

- desktop `nodeType` authoring 输入已绑定当前节点目录 truth。已注册类型会显示 ownership、identity rule、editing boundary；未注册类型会在 authoring 阶段直接禁用“添加所选节点”。
- `unknown_node_type` 不再只停留在底层 validation / preflight support。预检 UI 会把它显示成显式的目录收敛问题，明确提示主路径已禁止导入、添加和运行，并要求同步 Rust manifest / local node policy 或替换节点类型。
- 这两条边界把“节点目录是否已注册”从字符串约定推进成了 authoring surface 与 preflight surface 的共同约束，避免继续把未知类型留到 run-time 才暴露。

### 4.6 backend-owned governance store `schema_version` 边界

2026-03-24 新增的 Phase 2 起步边界：

- `workflow_app_registry_store`、`workflow_quality_rule_store`、`workflow_manual_review_store`、`workflow_version_store`、`workflow_run_baseline_store`、`workflow_run_audit_store`、`workflow_sandbox_rule_store`、`workflow_sandbox_autofix_store` 已被收口进统一的 `schema_version` gate。
- 其中之前最容易漂移的几类对象已补齐主路径版本字段：workflow run entry / audit event / timeline / failure summary，sandbox rules / rule versions / compare payload / rollback payload，sandbox autofix state / action history。
- 新增 `ops/scripts/check_governance_store_schema_versions.ps1` 后，这些 backend-owned store 的 source marker 与 runtime normalized output 都开始被 CI / release-ready scorecard 约束，而不再只是实现细节。

### 4.7 local `workflow_store/` container `schema_version` 边界

2026-03-24 新增的 Phase 2 起步边界：

- desktop 本地 `workflow_store/` 下仍然保留的高价值 JSON 容器已开始显式 versioned：`workflow_task_queue.json`、`workflow_queue_control.json`、`workflow_node_cache.json`、`workflow_node_cache_metrics.json`、`template_marketplace.json`。
- `workflow_ipc_state.js` 现在对这些容器统一写入 `schema_version`，同时保留对历史未版本化对象的读取迁移路径，避免把本地用户状态直接打坏。
- 新增 `ops/scripts/check_local_workflow_store_schema_versions.ps1` 后，这些本地持久化对象的 source marker、runtime file shape 与 legacy read path 都开始被 CI / release-ready scorecard 约束。

### 4.8 template pack artifact contract 边界

2026-03-24 新增的 Phase 2 起步边界：

- template pack 已不再只是一个任意 JSON blob。现在区分 `template_pack_entry.v1`（desktop marketplace 条目）与 `template_pack_artifact.v1`（导入/导出文件 contract）。
- legacy 未版本化 template pack artifact 在 install 时会显式迁移，并把 migration 信息带回主路径；export 时则始终强制写出 `template_pack_artifact.v1`。
- 新增 `apps/dify-desktop/workflow_template_pack_contract.js` 与 `ops/scripts/check_template_pack_contract_sync.ps1` 后，template pack import migration、marketplace entry schema、artifact export schema 都开始被 CI / release-ready scorecard 约束。

### 4.9 local template storage contract 边界

2026-03-24 新增的 Phase 2 起步边界：

- renderer `localStorage` 下的 custom templates 已不再是裸数组；现在显式区分 `local_template_storage.v1`（storage envelope）与 `local_template_entry.v1`（单条本地模板）。
- legacy bare-array localStorage payload 在读取时会显式迁移并回写成 versioned envelope；`saveCurrentAsTemplate` 也已切到 versioned save path。
- 新增 `renderer/workflow/template-storage-contract.js` 与 `ops/scripts/check_local_template_storage_contract_sync.ps1` 后，legacy migration、entry schema 与 versioned save path 都开始被 CI / release-ready scorecard 约束。

### 4.10 offline template catalog contract 边界

2026-03-24 新增的 Phase 2 起步边界：

- `rules/templates/office_themes_desktop.json`、`rules/templates/office_layouts_desktop.json`、`rules/templates/cleaning_templates_desktop.json` 已开始显式 versioned。
- 现在区分 `office_theme_catalog.v1`、`office_layout_catalog.v1`、`cleaning_template_registry.v1` 三类 desktop offline template contract，并为它们落仓了对应 schema。
- `offline_engine_config.js` 已接入迁移读取逻辑：既能消费新的 versioned catalog，也不会把历史未版本化 catalog 直接打坏。
- 新增 `apps/dify-desktop/offline_template_catalog_contract.js` 与 `ops/scripts/check_offline_template_catalog_sync.ps1` 后，这些 desktop offline template assets 也开始被 CI / release-ready scorecard 约束。
- `ops/scripts/template_pack_manager.ps1` 也已从裸 `manifest.json` 收口到 versioned pack manifest，避免 release/template_packs 这条旧出口继续游离在新 contract 体系之外。

## 5. 30 / 60 / 90 天路线图

### Phase 1：2026-03-20 到 2026-04-19

目标：

- 让 workflow contract 与 operator metadata 从“文档约定”进入“主路径 + gate”
- 建立 capability ownership 与 fallback governance 基本制度
- 在不做大规模 UI 重构的前提下，先钉住最关键漂移点

交付物：

- desktop workflow graph 顶层 `version` 主路径收敛
- focused workflow contract enforcement
- workflow contract drift gate
- operator metadata drift gate
- capability ownership 文档
- fallback governance 文档
- 对应单测与 gate 执行验证

风险：

- 旧 workflow 文件与旧测试样例会暴露出历史未版本化问题
- desktop alias 节点与 runtime backing 可能不一致
- 当前 gate 仍是 focused validation，不是完整 JSON Schema validator

退出标准：

- desktop workflow graph 顶层 `version` 已进入主路径
- 未版本化或明显非法 workflow 不再静默通过
- 至少 1 个 workflow contract drift gate 与 1 个 operator metadata drift gate 已可执行
- capability ownership 已书面明确
- 代码、测试、文档三者都已落仓

### Phase 2：2026-04-20 到 2026-05-19

目标：

- 把 Rust operator metadata 从“手工双写”推进到“单源驱动”
- 把 `offline_local` / `base_api` 的 payload 约定收敛成同一 validated envelope
- 把长期持久化对象显式 versioned
- 建立 fallback inventory 与双前端收敛决策输入

工作项：

1. Rust operator metadata 单源化
   - 输出 machine-readable manifest，建议路径：`contracts/rust/operators_manifest.v1.json`
   - 为 manifest 提供 versioned schema，建议路径：`contracts/rust/operators_manifest.schema.json`
   - manifest 至少覆盖 operator id、domain、source module、capability flags、desktop exposable、workflow exposable
   - desktop catalog 与 Rust routing 改为 manifest 驱动或 manifest 校验

2. run payload 与 adapter 收敛
   - 统一 validated workflow envelope
   - 明确区分 top-level workflow contract、chiplet envelope contract、backend/operator payload contract
   - `offline_local` 与 `base_api` 只在 adapter 层分叉

3. 持久化对象版本化
   - 为 `workflow_store/` 中高价值对象补 `schema_version`
   - 区分长期恢复状态与审计日志
   - 增加 migration fixtures 与 regression tests

4. fallback inventory
   - 盘点历史 compatibility fallback
   - 为每个 fallback 补 owner、reason、remove_by、kill_condition
   - 无 owner 或无退出条件的一律列为架构违规

5. frontend convergence execution
   - 决策已明确：WinUI 为主前端，Electron 为次级兼容壳层
   - 下一步不是继续做路线对比，而是落主次边界、迁移顺序与退役计划

6. CI / gate 扩展
   - 新增 gate 进入 quick / full / release 的合适入口
   - 重 gate 至少进入 full CI 或 release gate

风险：

- 生成型 manifest 需要跨 Rust / desktop 边界统一格式
- `offline_local` / `base_api` 当前差异可能暴露更多隐性 contract
- 持久化对象版本化会带来 fixtures 与迁移样例维护成本

退出标准：

- Rust operator metadata 对 desktop 不再是纯手工双写
- `offline_local` 与 `base_api` 已共享 validated workflow envelope
- 长生命周期本地持久化对象开始 versioned
- fallback inventory 已成文
- 双前端正式收敛选项成文

### Phase 3：2026-05-20 到 2026-06-18

目标：

- 删除一批主路径 fallback
- 把 schema enforcement 从顶层推进到高频 `node.config`
- 让“漂移会被 gate 自动挡住”成为默认状态
- 把前端边界从讨论推进到落仓

工作项：

1. fallback retirement
   - 删除已被替代且观测稳定的 fallback
   - 删除必须伴随回归测试

2. `node.config` schema 化
   - 先覆盖 top 10 高频 / 高风险节点
   - 严格收紧 `additionalProperties`
   - 让 form / normalize / run payload / adapter 一致

3. generated or validated catalogs 成熟化
   - Phase 2 若仍是对比型 gate，本阶段推进到 manifest 驱动或半生成
   - 新增 operator 的人工接触点显著减少

4. contract-first CI
   - gate 至少拦住：
   - workflow 缺失 `version`
   - desktop catalog 与 Rust metadata 漂移
   - 未注册 node type 进入运行链
   - fallback 缺少 owner 或 remove_by
   - 高优先级持久化对象缺失 `schema_version`

5. frontend convergence 落仓
   - 明确主前端与副前端边界
   - 结束默认双主线

6. 长期制度化
   - 把单一事实来源、versioning、fallback governance、schema evolution 写成持续规则

风险：

- 删除 fallback 需要真实观测和回归样例
- 节点级 schema 化如果没有优先级控制，会扩张过快
- 双前端决策需要产品 / 交付节奏共同参与

退出标准：

- 至少一批主路径 fallback 被删除
- 高频节点 config 已开始 schema 化
- 主要 drift 默认被自动 gate 拦截
- 前端双轨边界正式落仓
- 收敛计划从专项转为制度

## 6. 本次已落地 vs 后续计划

### 本次已落地

- desktop `nodeType` authoring surface 已绑定节点目录 truth：已注册类型显示 policy ownership 卡片，未知类型在 authoring 阶段即被禁用，而不是继续依赖 add/import/run 时的兜底报错。
- preflight report 已对 `unknown_node_type` 输出专用 contract guidance，而不是继续把这类问题归并成普通 generic graph 错误。
- backend-owned governance / publishing stores 的 `schema_version` gate 已落地，并进入 CI architecture scorecard 与 release-ready scorecard。
- local `workflow_store/` JSON containers 的 `schema_version` gate 已落地，并开始约束队列、队列控制、节点缓存、模板市场等仍在 desktop 本地持久化的对象。
- template pack import / export 已被收口成显式 artifact contract，并进入 CI architecture scorecard 与 release-ready scorecard。
- local custom templates 的 localStorage contract 已被显式 versioned，并进入 CI architecture scorecard 与 release-ready scorecard。
- offline desktop template catalogs（themes / layouts / cleaning template registry）已被显式 versioned，并进入 CI architecture scorecard 与 release-ready scorecard。

- desktop workflow `version` 主路径收敛
- import migration note / save reject / run explicit version payload
- workflow contract focused validation helper
- workflow contract sync gate
- operator catalog sync gate
- WinUI 主前端决策落仓
- WinUI 默认前端入口脚本与 frontend convergence gate
- WinUI publish / bundle / release wrapper 首版发布链
- CI / release 入口接线
- 对应单测、gate 执行测试、精简 CI 验证

### 明确留到后续阶段

- 完整 JSON Schema validator 进入 desktop 主路径
- desktop 对 Rust operator manifest 的全面消费与生成驱动
- `offline_local` / `base_api` envelope 彻底统一
- 本地持久化对象 `schema_version` 系统化
- fallback inventory 全量盘点
- WinUI 打包 / 安装管线与 Electron 退役时间表
- 高频节点 `node.config` schema 化

## 7. 这次故意不解决的事情

以下事项明确不在这次一次性完成：

- 不重写 Electron 或 WinUI
- 不继续把拆 `app.js` / support 文件当主线
- 不一次性把所有 node type schema 化
- 不一次性把所有 operator metadata 改成全自动生成
- 不在本次全面重构 desktop 本地持久化层

原因：

- Phase 1 的任务是建立硬边界，而不是追求一次性全量改造
- 如果现在同时做 UI 大改、frontend 决策、manifest 生成和持久化升级，会稀释最关键的 contract-first 收敛成果

## 8. 仍需明确的产品 / 架构决策

1. WinUI 打包 / 安装管线何时替代 Electron 兼容打包路径。
2. `offline_local` 与 `base_api` 哪些行为差异必须保留为 adapter 差异，哪些必须统一。
3. Rust operator manifest 的生成归属与消费格式。
4. 哪些本地持久化对象属于“长期恢复状态”，必须在 Phase 2 优先 versioned。
5. 哪些 fallback 属于必须优先退役的主路径风险。

## 9. Phase 1 新增硬边界为什么不是局部修补

补充到 2026-03-24 的判断：

- `nodeType` 输入被节点目录 truth 约束后，desktop authoring surface 不再是字符串逃逸口；操作者在输入阶段就会看到 ownership / identity boundary，而不是等到更晚的路径再碰撞出错。
- `unknown_node_type` 进入预检 UI 的专用错误类型后，contract drift 不再只存在于 support 层或测试层，而是成为用户可见、可定位、可处置的主路径问题。

这些改动的价值不在于“修了几个字段”，而在于改变了系统的受约束方式：

- contract 不再只活在 `contracts/`，而进入 renderer import / save / run 与 main workflow execution
- Rust operator truth 不再只能靠人工记忆同步，CI / release 已能自动拦截 catalog drift
- alias 节点必须有 runtime backing，否则 gate 失败
- 未版本化 workflow 不再静默流过主路径

这就是收敛的第一步：先让错误边界可以被自动发现、自动阻断，再推进单源化与 fallback 删除。

## 10. Phase 2 Foundation Landed On 2026-03-21 And 2026-03-22

本次继续推进了一个 Phase 2 起步项：

- quality rule set 后端化基础已落地到 glue-python，后端开始拥有规则集的存储、查询与版本边界
- desktop 新增 provider 抽象，`base_api` 路径开始优先消费 glue-python governance store
- 指定 `quality_rule_set_id` 但规则集不存在时，主路径不再静默跳过，而是显式失败
- `offline_local` 的 `local_legacy` provider 被保留为临时 adapter provider，并已写入 fallback governance，`remove_by` 为 2026-05-19
- workflow sandbox rule 后端化基础已落地到 glue-python，后端开始拥有规则存储与版本边界
- 自 2026-03-23 起，desktop sandbox rule runtime mirror 已退役；当前 sandbox rule 的读取、编辑、版本、回滚与静默操作统一改由 glue-python contract 提供
- workflow sandbox autofix state / action history foundation 已落地到 glue-python，后端开始拥有强制隔离态、green streak 与 action history 的状态/查询边界
- desktop 在 `base_api` 路径开始优先通过后端 sandbox autofix store 查询状态与历史，并仅把 Electron 本地 autofix state 保留为过渡 mirror
- workflow app registry 后端化基础已落地到 glue-python，后端开始拥有发布注册表的存储与查询边界
- desktop `base_api` 路径开始优先通过后端 registry 发布/读取 workflow app，并把 `template_policy` 纳入 registry 条目
- workflow version store / compare foundation 已落地到 glue-python，后端开始拥有版本快照与 diff 的存储/查询边界
- desktop 在 `base_api` 路径开始优先通过后端 version store 记录、列出、恢复与对比 workflow 版本
- manual review queue/history foundation 已落地到 glue-python，后端开始拥有审核状态与历史查询边界
- desktop 在 run / queue / sandbox 主路径中生成 `pending_reviews` 时，`base_api` 路径开始优先写入后端 manual review store；前端继续保留审核操作面
- workflow run audit query foundation 已落地到 glue-python，后端开始拥有 run history / timeline / failure summary / audit log 的查询边界
- desktop 在 `base_api` 路径开始优先通过后端 run audit store 查询运行历史与审计；本地文件仅保留镜像职责
- 自 2026-03-22 起，`base_api` 默认已不再把 workflow audit event 或 run history 写入本地 run/audit mirror；quality gate report、sandbox alerts 与 perf dashboard 也已改为消费 backend run query model
- 自 2026-03-22 起，推荐前端启动路径开始默认把 workflow version 与 workflow run audit provider 切到 glue-python，这让 `offline_local` 的这两类本地 owner 开始退出默认使用面
- 自 2026-03-22 起，推荐前端启动路径也开始默认把 quality rule set 与 workflow app registry provider 切到 glue-python，这让 `offline_local` 的更多治理/发布 owner 开始退出默认使用面
- 自 2026-03-22 起，推荐前端启动路径也开始默认把 manual review 与 run baseline provider 切到 glue-python，这让 `offline_local` 的审核与回归 owner 也开始退出默认使用面
- 自 2026-03-22 起，这六类 governance / publishing stores 的默认解析也已改为优先 glue-python；`local_legacy` 从“隐式默认”收缩成“只有显式声明时才允许的兼容 fallback”
- 自 2026-03-23 起，`workflow version local_legacy` 已正式退役；workflow version snapshot / compare 不再允许继续回落到本地 JSONL
- 自 2026-03-23 起，`workflow run audit local_legacy` 也已正式退役；workflow run history / timeline / failure summary / audit log 不再允许继续回落到本地 JSONL
- 自 2026-03-23 起，`workflow quality rule set local_legacy` 与 `workflow app registry local_legacy` 也已正式退役；quality rule set 与 workflow app registry 不再允许继续回落到本地 JSON
- 自 2026-03-23 起，`manual review local_legacy` 与 `run baseline local_legacy` 也已正式退役；manual review queue/history 与 run baseline registry 不再允许继续回落到本地 JSON
- 自 2026-03-23 起，`sandbox autofix local_legacy` 也已正式退役；sandbox autofix state / actions / execution overlay 不再允许继续回落到本地 provider
- 自 2026-03-23 起，`sandbox rule local_legacy` 的外部 provider 选择面与本地 runtime mirror 都已退役；当前只保留 glue-owned remote governance 这一条边界
- run baseline registry foundation 已落地到 glue-python，后端开始拥有 baseline 保存与读取边界
- desktop 在 `base_api` 路径开始优先通过后端 baseline registry 保存与读取 baseline，`compareRunWithBaseline` 的本地 registry 依赖已开始退出主路径
- WinUI 主前端当前已覆盖 manual review queue/history、approve/reject、recent runs、run timeline、failure summary、audit events、quality rule set list/save/delete、sandbox rules viewer/editor、version rollback、sandbox mute helper，以及 sandbox autofix state/history viewer/editor
- fallback governance gate 已从手工固定清单推进到自动发现 desktop `local_legacy` provider / local mirror；新增本地兼容层若缺 inventory section，会在 gate 阶段直接失败
- WinUI / Electron 前端启动链自 2026-03-22 起开始显式确保本地 glue-python governance bridge 健康，这为后续把 `offline_local` 的治理 owner 收口到 backend contract 提供了启动前提
- `docs/fallback_retirement_backlog_20260322.md` 已把现存 fallback 按删债批次挂牌；其中 sandbox autofix local mirror 已从 `base_api` 运行主路径退出，剩余阻塞集中在 `offline_local`

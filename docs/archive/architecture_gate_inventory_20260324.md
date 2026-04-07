# AIWF Architecture Gate Inventory

> Historical snapshot. Retained for gate-history context; not current implementation authority.

日期: 2026-03-24

## 目标

这份清单不新增 gate。
它只回答三件事：

1. 哪些 gate 是核心边界
2. 哪些 gate 是支持性边界
3. 哪些 gate 未来可以合并

## 核心边界 Gate

### `check_workflow_contract_sync.ps1`

- owner: desktop workflow contract
- source authority: `contracts/workflow/*.json` + desktop main-path contract enforcement
- blocking intent: 防止 workflow 顶层 contract、unknown node type guard、run payload drift

### `check_operator_catalog_sync.ps1`

- owner: Rust operator metadata authority
- source authority: `apps/accel-rust/src/operator_catalog_data.rs`
- blocking intent: 防止 Rust operator metadata 对 desktop 漂移

### `check_fallback_governance.ps1`

- owner: architecture / fallback governance
- source authority: `docs/archive/fallback_governance_20260320.md`
- blocking intent: 防止无 owner / 无退出条件 fallback 重新进入主链

### `check_governance_store_schema_versions.ps1`

- owner: governance persistence boundary
- source authority: glue-python owned governance store contracts
- blocking intent: 防止 backend-owned governance store runtime output 丢失 `schema_version`

### `check_frontend_convergence.ps1`

- owner: frontend convergence decision
- source authority: `docs/frontend_convergence_decision_20260320.md`
- blocking intent: 防止 WinUI 主前端决策被回退成默认双主线

## 支持性边界 Gate

### `check_local_workflow_store_schema_versions.ps1`

- owner: desktop local durable state boundary
- source authority: `workflow_ipc_state.js`
- blocking intent: 约束 `workflow_store/` 本地 JSON 容器 versioning

### `check_template_pack_contract_sync.ps1`

- owner: desktop template artifact boundary
- source authority: `workflow_template_pack_contract.js` + `contracts/desktop/template_pack_artifact.schema.json`
- blocking intent: 约束 template pack import / export artifact

### `check_local_template_storage_contract_sync.ps1`

- owner: renderer local template storage boundary
- source authority: `renderer/workflow/template-storage-contract.js`
- blocking intent: 约束 localStorage custom template envelope / entry

### `check_offline_template_catalog_sync.ps1`

- owner: offline desktop template asset boundary
- source authority: `rules/templates/*.json` + `offline_template_catalog_contract.js`
- blocking intent: 约束 desktop offline template catalogs 与 pack manager round-trip

### `check_node_config_schema_coverage.ps1`

- owner: desktop node-config validation discipline
- source authority: 当前仍主要是 `renderer/workflow/workflow-contract.js`
- blocking intent: 防止已覆盖 node config schema 回退

### `check_local_node_catalog_policy.ps1`

- owner: desktop local node presentation / palette policy
- source authority: local-node policy / presentation files
- blocking intent: 防止 local workflow node UI metadata 漂移

## 当前碎片化判断

### 真的核心，不应轻易合并

- `workflow_contract_sync`
- `operator_catalog_sync`
- `fallback_governance`
- `frontend_convergence`

这些 gate 对应不同 owner，不应为了“少脚本”而合并掉语义。

### 可以考虑在未来形成套件层聚合

- `governance_store_schema_versions`
- `local_workflow_store_schema_versions`
- `template_pack_contract_sync`
- `local_template_storage_contract_sync`
- `offline_template_catalog_sync`

它们都属于 “versioned durable artifacts / stores” 这一大类。
未来可以用一个上层聚合入口统一执行，但不建议现在直接删除细分 gate，因为当前仓库仍在快速收敛，细分失败信息更有价值。

### 当前最容易被误判的 gate

- `node_config_schema_coverage`

它看起来像 schema authority gate，但实际更多是在保护 JS helper 已经承载的 node-config truth。
它是必要的，但不能被误读为“contracts/` 已经成为 node config authority”。

## 建议

1. 保留现有 gate 颗粒度，但把上面这 5 个 durable-artifact gates 视作一个逻辑簇
2. 在后续 release scorecard 展示层按“核心边界 / durable artifact / frontend / fallback”分组
3. 在 authority 真正进一步收口前，不要为了减少脚本数量而失去失败可解释性

# AIWF Fallback Governance

日期：2026-03-20

## 目标

fallback 允许存在，但只能作为带退出条件的临时机制，不能成为永久边界掩体。

## 强制字段模板

每个 compatibility fallback 必须记录：

- `owner`
- `reason`
- `added_at`
- `remove_by`
- `success_metric`
- `kill_condition`

建议补充：

- `scope`
- `risk_if_removed_too_early`
- `observability`

## Governance Rules

1. 不允许新增没有 `remove_by` 的 fallback。
2. 不允许新增没有 owner 的 fallback。
3. fallback 必须绑定可观测退出条件，而不是“以后再看”。
4. 新 fallback 必须在同次 PR / 变更中写入 inventory 或规则文档。
5. 若 fallback 已过 `remove_by` 仍未删除，必须重新审批并更新原因。

## Temporary Fallback Classes

### A. Migration Fallback

适用：

- 历史对象缺失版本字段
- 旧 payload 需要一次性迁移到新 contract

要求：

- 只能用于显式 migration
- 必须记录迁移行为
- 必须有明确退役时间

### B. Adapter Fallback

适用：

- `offline_local` / `base_api` 在 transport 或 endpoint 层的临时差异

要求：

- 只能存在于 adapter 层
- 不得成为新的语义源

### C. Dependency Fallback

适用：

- 外部依赖缺失时的降级策略

要求：

- 必须说明用户可见影响
- 必须说明成功指标与 kill condition

## Phase 1 Approved Temporary Fallbacks

### 1. workflow.version migration on import / normalization

- owner: desktop workflow architecture
- reason: 历史 workflow graph 存在未版本化样本，Phase 1 先引入显式迁移，避免继续静默漂移
- added_at: 2026-03-20
- remove_by: 2026-05-19
- success_metric: 新保存与新运行的 workflow 全量携带顶层 `version`；历史样本迁移比例持续下降
- kill_condition: Phase 2 manifest / validator 完成后，未版本化 graph 在主路径直接拒绝，不再自动补齐

说明：

- 此 fallback 只允许做显式 migration，不允许无记录静默通过
- 本次已在 renderer import 和 main workflow normalization 中记录 migration

## Phase 2 Required Inventory

下一阶段必须清点并挂牌的重点 fallback：

- Python app constructor compatibility path
- workflow import / payload 历史兼容路径
- desktop `offline_local` / `base_api` 语义差异补丁
- 任何缺少 owner / remove_by 的旧 compatibility logic

## Gate Direction

Phase 3 前应至少具备以下 gate：

- 新 fallback 缺少 owner 或 `remove_by` 时失败
- 高风险 fallback 超过 `remove_by` 且未续批时失败
- adapter fallback 越界进入 contract 层时失败
- 任何新增 `local_legacy` provider / local mirror 若未在文件中声明 `FALLBACK_GOVERNANCE_TITLE` 并落入 inventory，直接失败

## New Transitional Provider Added On 2026-03-21

### desktop quality rule set `local_legacy` provider

- retired_at: 2026-03-23
- owner: workflow/governance convergence
- removal_reason: quality rule set owner 已完全收口到 glue-python；本地 `quality_rule_center.json` 不再允许作为兼容 provider 继续存在
- follow_up: 显式声明 `qualityRuleSetProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python governance store

### desktop workflow sandbox rule local mirror

- retired_at: 2026-03-23
- owner: workflow/governance convergence
- removal_reason: workflow sandbox rule 的读取、编辑、版本、回滚与静默操作主路径已全部收口到 glue-python；desktop 不再保留独立的本地 rule mirror store 或同步接口
- follow_up: `workflowSandboxRuleProvider=local_legacy` 继续保持拒绝；任何新的本地 sandbox rule mirror 或独立版本语义都应视为架构违规。

说明：

- 这条 fallback 现在只保留为历史记录，不再代表现存主路径。

### desktop workflow sandbox autofix local mirror

- retired_at: 2026-03-23
- owner: workflow/governance convergence
- removal_reason: workflow sandbox autofix 的读取、写入与执行主路径已完全收口到 glue-python；本地 autofix state provider 不再允许作为兼容入口继续存在
- follow_up: 显式声明 `workflowSandboxAutoFixProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python sandbox autofix contract

### desktop workflow app registry `local_legacy` provider

- retired_at: 2026-03-23
- owner: workflow/product convergence
- removal_reason: workflow app registry owner 已完全收口到 glue-python；本地 `workflow_apps.json` 不再允许作为兼容 provider 继续存在
- follow_up: 显式声明 `workflowAppRegistryProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python workflow app registry

## Retired Fallbacks

### desktop workflow version `local_legacy` provider

- retired_at: 2026-03-23
- owner: workflow/platform convergence
- removal_reason: workflow version owner 已完全收口到 glue-python；推荐启动路径与 store 默认解析都已优先走 glue-python，本地 version cache 不再允许作为兼容 fallback 继续存在
- follow_up: 显式声明 `workflowVersionProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python workflow version store

### desktop manual review `local_legacy` provider

- retired_at: 2026-03-23
- owner: workflow/governance convergence
- removal_reason: manual review queue/history owner 已完全收口到 glue-python；本地 manual review queue/history 文件不再允许作为兼容 provider 继续存在
- follow_up: 显式声明 `manualReviewProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python manual review contract

### desktop workflow run audit local mirror

- retired_at: 2026-03-23
- owner: workflow/observability convergence
- removal_reason: workflow run history / timeline / failure summary / audit log 的默认写入与查询已全部收口到 glue-python；本地 run/audit JSONL 不再允许作为兼容 provider 继续存在
- follow_up: 显式声明 `workflowRunAuditProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python workflow run audit contract

### desktop run baseline `local_legacy` provider

- retired_at: 2026-03-23
- owner: workflow/observability convergence
- removal_reason: run baseline owner 已完全收口到 glue-python；本地 baseline registry 不再允许作为兼容 provider 继续存在
- follow_up: 显式声明 `runBaselineProvider=local_legacy` 现在会直接失败，提醒调用方改用 glue-python baseline registry

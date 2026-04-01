# AIWF Fallback Retirement Backlog

日期：2026-03-22

## 目的

这份清单把 Phase 3 的 fallback retirement 提前收敛成可执行 backlog。

它回答三个问题：

- 哪些 fallback 仍然存在
- 它们现在还挡在哪个主路径上
- 下一批应该先删哪一个

## 当前判断

优先级规则：

1. 先删已经不再卡住 `base_api` 主路径、只剩 `offline_local` 兼容职责的 fallback。
2. 再删主路径仍依赖、但已具备 backend owner 和 WinUI 主前端消费面的 fallback。
3. 最后处理还同时卡住语义边界和人工治理面的 fallback。

## 批次划分

### Batch A：优先退役候选

#### desktop workflow sandbox autofix local mirror

- 当前状态：已于 2026-03-23 退役。
- removal_result：`workflowSandboxAutoFixProvider=local_legacy` 不再被接受；sandbox autofix state / actions / execution overlay 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### desktop workflow run audit local mirror

- 当前状态：已于 2026-03-23 退役。
- removal_result：`workflowRunAuditProvider=local_legacy` 不再被接受；当前保留的是 desktop local-runtime run/audit truth，远端 run audit 改为显式 `base_http` provider。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

### Batch B：主路径已后端化，但仍有 `offline_local` 本地 owner

#### desktop quality rule set `local_legacy` provider

- 当前状态：已于 2026-03-23 退役。
- removal_result：`qualityRuleSetProvider=local_legacy` 不再被接受；quality rule set 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### desktop workflow app registry `local_legacy` provider

- 当前状态：已于 2026-03-23 退役。
- removal_result：`workflowAppRegistryProvider=local_legacy` 不再被接受；workflow app registry 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### desktop workflow version `local_legacy` provider

- 当前状态：已于 2026-03-23 退役。
- removal_result：`workflowVersionProvider=local_legacy` 不再被接受；workflow version snapshot / compare 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### desktop manual review `local_legacy` provider

- 当前状态：已于 2026-03-23 退役。
- removal_result：`manualReviewProvider=local_legacy` 不再被接受；manual review queue/history 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### desktop run baseline `local_legacy` provider

- 当前状态：已于 2026-03-23 退役。
- removal_result：`runBaselineProvider=local_legacy` 不再被接受；run baseline registry 统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

### Batch C：最后处理

#### desktop workflow sandbox rule local mirror

- 当前状态：已于 2026-03-23 退役。
- removal_result：`workflowSandboxRuleProvider=local_legacy` 不再被接受；workflow sandbox rule 的读取、编辑、版本、回滚与静默操作统一改由 glue-python owner 提供。
- residual_risk：仍需观察极少数绕过推荐启动链的旧脚本是否会命中显式 local_legacy 配置并失败。

#### workflow.version migration on import / normalization

- 当前状态：这是 migration fallback，不是 provider fallback。
- 仍保留原因：历史 graph 仍存在未版本化样本。
- 风险等级：高
- 最早可删日期：2026-05-19
- 删除前提：
  - 主路径对未版本化 workflow 直接拒绝
  - 历史导入样本完成清点并留出必要 migration fixture

## 2026-03-22 到 2026-04-05 的执行顺序

1. 先完成 `sandbox autofix local mirror` 的 `offline_local` 收口设计，因为它已经从 `base_api` 运行主路径退出，是最接近可删的一项。
2. 再处理 `workflow run audit` 的 execution / governance 分界，避免继续把本地 runtime run/audit 真相默认写进 glue-python。
3. 然后推进 quality rule set / app registry / workflow version 的 `offline_local` owner 收口。
4. 前端默认启动链已经补上 `ensure_local_governance_bridge.ps1`；下一步可以开始在不要求用户手工先起 glue-python 的前提下收口 `offline_local` 的 governance owner。

## 不在这一批立即做的事

- `workflow sandbox rule local mirror` 已于 2026-03-23 退役；后续不应重新引入本地 rule mirror。
- 不在 2026-03-22 直接删除 `workflow.version migration`，因为历史未版本化 graph 仍未完成清点。
- 不在这一轮硬删 `manual review local_legacy`，因为 `offline_local` 审核状态机还没切到统一 owner。
## Rollout Governance Addendum

### cleaning default rust_v2 fallback to python_legacy

- current_status: active compatibility fallback during cleaning default rollout governance
- owner: cleaning/runtime convergence
- target_remove_by: 2026-06-30
- removal_result: default mode no longer depends on Python legacy fallback for normal release traffic; fallback becomes exception-only or is removed entirely
- residual_risk: rollout thresholds may stay noisy if shadow compare mismatch rate or rust error rate is not stable enough across release and package audits

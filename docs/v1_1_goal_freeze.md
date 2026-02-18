# AIWF v1.1 Goal Freeze

## Scope
- 工作流编排：节点拖拽、连线/取消连线、保存/加载流程、运行可视化状态。
- 预处理平台：支持单阶段声明式配置与多阶段 `extract/clean/structure/audit` 流水线。
- 发布产物：`installer + portable` 双包，且每包包含 `manifest.json`、`RELEASE_NOTES.md`、`SHA256SUMS.txt`。
- 可观测性：Workflow Studio 可展示 chiplet 级别失败率和平均耗时，支持历史汇总刷新。

## Freeze Baseline
- 冻结日期：`2026-02-18`
- 冻结版本：`1.1.0`
- 机器可读验收配置：`ops/config/v1_1_acceptance.json`

## Acceptance
- `apps/glue-python` 单测通过。
- `apps/dify-desktop` 单测与 workflow UI 测试通过。
- `ops/scripts/run_regression_quality.ps1` 生成报告并通过阈值校验。
- `ops/scripts/release_productize.ps1 -Version <x.y.z>` 可产出 installer/portable 两类交付包，且包含清单与摘要文件。

## Out Of Scope
- 在线云端编排协同。
- 自动模板设计器。
- 大规模分布式调度。

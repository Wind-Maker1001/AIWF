# AIWF v1.1.5 Release Notes / 发布说明

发布日期 / Date: 2026-02-25

## 中文

### 概览
- 本版本聚焦稳定性封板，不引入高风险新功能。
- 已完成 3 轮完整发布封板（发布门禁 + 启动检查 + 两套验收），全部通过。

### 核心更新
- 发布稳定性流程
  - 新增 `ops/scripts/release_stability_v1_1_5.ps1`。
  - 支持连续多轮封板验证并输出汇总报告。
- 路由门禁稳定化
  - 工作流路由基准在严格阈值下稳定通过。
  - fallback 风险已收敛，封板期间实测 `fallback_ratio=0`。
- Rust 发布门禁抗抖动优化
  - 发布脚本默认 Rust 基准行数调整为 `100000`。
  - 移除发布阶段强制 Arrow Always，降低环境波动导致的误拦截。
- 桌面版本升级
  - `AIWF Dify Desktop` 版本升级为 `1.1.5`。

### 质量与验证
- 封板报告：
  - `release/stability_v1.1.5/stability_summary.json`
  - `release/stability_v1.1.5/stability_summary.md`
- 每轮均通过：
  - `release_productize`（全门禁）
  - `check_desktop_packaged_startup`
  - `acceptance_desktop_real_sample`
  - `acceptance_desktop_finance_template`

### 交付产物
- 安装包：
  - `release/offline_bundle_1.1.5_installer/AIWF_Offline_Bundle/AIWF Dify Desktop Setup 1.1.5.exe`
- 便携包：
  - `release/offline_bundle_1.1.5_portable/AIWF_Offline_Bundle/AIWF Dify Desktop 1.1.5.exe`

### 升级说明
- 从 `v1.1.4` 升级到 `v1.1.5` 建议直接覆盖安装。
- 若你已使用自定义模板，建议升级后执行一次模板预检与样例验收。

## English

### Overview
- This release focuses on stability hardening and release sealing.
- No high-risk feature expansion in this version.
- A full 3-round release seal (gates + startup + dual acceptance suites) passed.

### Highlights
- Release stability pipeline
  - Added `ops/scripts/release_stability_v1_1_5.ps1`.
  - Supports multi-round release verification with consolidated reports.
- Routing gate hardening
  - Workflow routing benchmark now passes under strict thresholds.
  - Fallback risk is contained; observed `fallback_ratio=0` during sealing runs.
- Rust release-gate anti-flake tuning
  - Default Rust benchmark rows in release flow adjusted to `100000`.
  - Removed forced Arrow Always in release gating to reduce environment-driven false blocks.
- Desktop version bump
  - `AIWF Dify Desktop` upgraded to `1.1.5`.

### Validation
- Seal reports:
  - `release/stability_v1.1.5/stability_summary.json`
  - `release/stability_v1.1.5/stability_summary.md`
- Every round passed:
  - `release_productize` (full gates)
  - `check_desktop_packaged_startup`
  - `acceptance_desktop_real_sample`
  - `acceptance_desktop_finance_template`

### Deliverables
- Installer:
  - `release/offline_bundle_1.1.5_installer/AIWF_Offline_Bundle/AIWF Dify Desktop Setup 1.1.5.exe`
- Portable:
  - `release/offline_bundle_1.1.5_portable/AIWF_Offline_Bundle/AIWF Dify Desktop 1.1.5.exe`

### Upgrade Notes
- Recommended upgrade path from `v1.1.4`: direct reinstall/overwrite.
- If you use custom templates, run one precheck and one acceptance sample after upgrade.

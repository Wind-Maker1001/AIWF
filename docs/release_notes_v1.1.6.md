# AIWF v1.1.6 Release Notes / 发布说明

发布日期 / Date: 2026-02-25

## 中文

### 概览
- 本版本聚焦“后端不可用时仍可交付”的稳定性能力。
- 新增 GUI 级自动回退与运行模式审计，降低联机依赖带来的中断风险。
- 完成 v1.1.6 发布门禁实跑，关键链路全部通过。

### 核心更新
- GUI 自动回退能力
  - 当使用 `base_api` 模式且后端失败时，自动回退本地离线清洗。
  - 远端异常与远端返回 `ok=false` 两类场景都支持回退。
  - 可在 GUI 中通过 `后端失败时自动切换离线模式` 开关控制（默认开启）。
- 结果可视化增强
  - 结果摘要新增 `运行模式` 指标（`base_api/offline_local/offline_fallback`）。
  - 回退触发时状态栏显示明确中文提示。
- 运行审计日志
  - 新增运行模式审计日志：`run_mode_audit.jsonl`。
  - 记录字段：`ts/mode/fallback_applied/reason/remote_error/job_id/duration_ms`。
- 质量与门禁
  - 新增发布门禁脚本：`ops/scripts/release_gate_v1_1_6.ps1`。
  - 覆盖：`test:unit`、`smoke`、`packaged startup`、`fallback scenario`。
  - 新增回退逻辑单测：`main_ipc_run_cleaning_fallback.test.js`。

### 验证结果
- `npm run test:unit`：通过
- `npm run smoke`：通过
- `check_desktop_packaged_startup.ps1`：通过
- `dify_run_with_offline_fallback.ps1`（故障注入）：通过
- 门禁汇总：
  - `release/gate_v1.1.6/20260225_182131/release_gate_summary.json`
  - `release/gate_v1.1.6/20260225_182131/release_gate_summary.md`

### 交付产物
- 安装包：`AIWF Dify Desktop Setup 1.1.6.exe`
- 便携版：`AIWF Dify Desktop 1.1.6.exe`

## English

### Overview
- This release focuses on resilience when backend services are unavailable.
- Added GUI-level automatic fallback and run-mode auditing to reduce online dependency risk.
- v1.1.6 release gate was executed and all key checks passed.

### Highlights
- GUI auto-fallback
  - In `base_api` mode, failures automatically fall back to local offline cleaning.
  - Supports both remote exceptions and remote `ok=false` responses.
  - Controlled by GUI switch `Auto fallback to offline when backend fails` (enabled by default).
- Result visibility
  - Added `run mode` metric in summary (`base_api/offline_local/offline_fallback`).
  - Explicit status message is shown when fallback is applied.
- Run audit log
  - Added run-mode audit JSONL log: `run_mode_audit.jsonl`.
  - Logged fields: `ts/mode/fallback_applied/reason/remote_error/job_id/duration_ms`.
- Quality and gating
  - Added release gate script: `ops/scripts/release_gate_v1_1_6.ps1`.
  - Coverage: `test:unit`, `smoke`, `packaged startup`, `fallback scenario`.
  - Added fallback-focused unit test: `main_ipc_run_cleaning_fallback.test.js`.

### Validation
- `npm run test:unit`: pass
- `npm run smoke`: pass
- `check_desktop_packaged_startup.ps1`: pass
- `dify_run_with_offline_fallback.ps1` (fault injection): pass
- Gate reports:
  - `release/gate_v1.1.6/20260225_182131/release_gate_summary.json`
  - `release/gate_v1.1.6/20260225_182131/release_gate_summary.md`

### Deliverables
- Installer: `AIWF Dify Desktop Setup 1.1.6.exe`
- Portable: `AIWF Dify Desktop 1.1.6.exe`

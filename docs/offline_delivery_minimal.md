# AIWF Electron Compatibility Offline Delivery (Minimal Bundle)

This document exists for the secondary Electron compatibility frontend.

WinUI is the primary frontend. Use this path only when you explicitly need the Electron compatibility shell and its legacy installer/portable artifacts.

## Goal

Build a minimal Electron compatibility package that can be copied to another Windows machine and installed directly.

## 1. Build Desktop Exe

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_electron_compatibility.ps1 -Version "<version>"
```

Compatibility release audit:

- `release\release_gate_audit_<version>.json`
- includes `frontend_verification.primary` and `frontend_verification.compatibility`
- includes `architecture_scorecard`
- includes `sidecar_regression` and `sidecar_python_rust_consistency`
- includes `cleaning_rust_v2_rollout`
- references the latest `ops\logs\frontend_verification\frontend_primary_verification_latest.json` and `frontend_compatibility_verification_latest.json`
- references `ops\logs\architecture\architecture_scorecard_release_ready_latest.json` and `architecture_scorecard_release_ready_latest.md`
- references `ops\logs\regression\sidecar_regression_quality_report.json` and `sidecar_python_rust_consistency_report.json`
- release is now blocked unless `architecture_scorecard_release_ready_latest.json` reports `overall_status = passed`
- release is now blocked unless `sidecar_regression_quality_report.json` reports `ok = true`
- release is now blocked unless `sidecar_python_rust_consistency_report.json` reports `ok = true` and contains no `skipped` entries
- release is now blocked unless the latest default+verify acceptance evidence for `desktop_real_sample` and `desktop_finance_template` is present and passes rollout gate validation

Output example:

- `apps\dify-desktop\dist\AIWF Dify Desktop <version>.exe`
- `apps\dify-desktop\dist\AIWF Dify Desktop Setup <version>.exe`

## 2. Generate Offline Bundle

```powershell
# installer bundle (recommended)
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>" -PackageType installer

# portable bundle
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>" -PackageType portable
```

Bundle output:

- `release\offline_bundle_<version>_installer\AIWF_Offline_Bundle`
- `release\offline_bundle_<version>_portable\AIWF_Offline_Bundle`

Bundle content:

- desktop `.exe` by `PackageType`
- optional `.blockmap`
- `README.txt`
- `SHA256SUMS.txt`
- `manifest.json`
- `RELEASE_NOTES.md`
- docs (`quickstart_desktop_offline.md`, `dify_desktop_app.md`, this file)
- `contracts/desktop/`
- `contracts/workflow/`
- `contracts/rust/`
- `contracts/glue/`
- `contracts/governance/`
- `contracts/desktop/template_pack_artifact.schema.json`
- `contracts/desktop/local_template_storage.schema.json`
- `contracts/desktop/office_theme_catalog.schema.json`
- `contracts/desktop/office_layout_catalog.schema.json`
- `contracts/desktop/cleaning_template_registry.schema.json`
- `contracts/desktop/offline_template_catalog_pack_manifest.schema.json`
- `contracts/workflow/workflow.schema.json`
- `contracts/workflow/render_contract.schema.json`
- `contracts/workflow/minimal_workflow.v1.json`
- `contracts/rust/operators_manifest.v1.json`
- `contracts/rust/operators_manifest.schema.json`
- `contracts/glue/ingest_extract.schema.json`
- `contracts/governance/governance_capabilities.v1.json`

Bundle/package gate notes:

- `package_offline_bundle.ps1` now blocks packaging unless the latest sidecar regression report is `ok=true`
- the same package step also blocks if the latest Python/Rust consistency report contains any `skipped` entries
- package now also blocks unless the latest default+verify acceptance evidence exists for both `desktop_real_sample` and `desktop_finance_template`
- required evidence files live at:
  - `ops\logs\acceptance\desktop_real_sample\cleaning_shadow_rollout.json`
  - `ops\logs\acceptance\desktop_finance_template\cleaning_shadow_rollout.json`
- package/release consume those latest evidence files and do not auto-run acceptance on your behalf
- both evidence files must represent real glue `run_cleaning` compare results with `requested_rust_v2_mode = default`, `verify_on_default = true`, and `shadow_compare.status = matched`
- if you claim image/XLSX enhanced ingest support for the bundle, start local `glue-python` with `-RequireEnhancedIngest`

Generate a zip for copying:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\zip_offline_bundle.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\zip_offline_bundle.ps1 -Version "<version>" -PackageType installer
```

## 3. Validate Docs Links

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_docs_links.ps1 -IncludeReadme
```

Recommended before a compatibility release:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Compatibility
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_regression_quality.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1 -RequireAccel
```

The shared async benchmark gate used by release verification now submits against tenant `bench_async` and keeps `AIWF_ASYNC_BENCH_MAX_IN_FLIGHT` aligned to the default accel-rust tenant concurrency limit.

## 4. Copy To Target Machine

Copy:

- `release\offline_bundle_<version>_installer\AIWF_Offline_Bundle`

On the target machine:

1. Run the desktop exe.
2. Launch the app.
3. Keep `离线本地模式`.
4. Drag raw files into the queue and click `开始生成`.

## 5. Notes

- This path is compatibility-only, not the main desktop release path.
- Offline mode does not require local SQL/Java/Rust/Python services.
- OCR on images is enabled by default in the Electron GUI.
- If bundle includes `tools/`, the app will prefer bundled `tesseract/pdftoppm`.
- If runtime dependency is missing, the app will auto-fallback and show a warning.

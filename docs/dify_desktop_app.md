# AIWF Electron Compatibility Frontend

This document describes the secondary Electron compatibility frontend under `apps/dify-desktop`.

WinUI is the primary frontend. Electron remains for Workflow Studio compatibility, advanced diagnostics, and current Electron packaging paths.

## Current Surfaces

- home screen for offline jobs, queue management, precheck, and local release helpers
- explicit `Legacy Workflow Studio` compatibility window for advanced workflow editing
- governance / diagnostics / publish panels are compatibility-only and require explicit admin mode: `--workflow-admin` or `?legacyAdmin=1`
- optional `base_api` mode for calling local AIWF backend endpoints

## Frontend Role

- secondary Electron compatibility frontend
- not the long-term primary desktop shell
- use WinUI first for the main desktop path

## Runtime Modes

- `offline_local`
  - default mode
  - runs the local offline engine
- `base_api`
  - calls your local backend
  - can automatically fall back to offline mode

Current fallback policies in the UI:

- `smart`
- `smart_strict`
- `always`
- `never`

## Current Desktop Defaults

From the current Electron config support:

- `mode = offline_local`
- `baseUrl = http://127.0.0.1:18080`
- `enableOfflineFallback = true`
- `fallbackPolicy = smart`
- `outputRoot`:
  - `E:\Desktop_Real\AIWF` if `E:\Desktop_Real` exists
  - otherwise `Desktop\AIWF_Builds`

## Inputs, Templates, and Outputs

Supported offline inputs:

- `csv`
- `xlsx`
- `txt`
- `docx`
- `pdf`
- images: `png/jpg/jpeg/bmp/webp/tif/tiff`

Current built-in template choices in the UI:

- `default`
- `debate_evidence_v1`
- `finance_report_v1`

Current Fluent variants in the UI:

- `fluent_ms_light`
- `fluent_ms_strong`
- `fluent_ms_vibrant`

Possible outputs:

- Office artifacts: `fin.xlsx`, `audit.docx`, `deck.pptx`
- markdown artifacts: `evidence.md`, `paper_markdown_index.md`, `ai_corpus.md`, `quality_report.md`

The current UI enables `md_only` by default. If you want the Office trio, uncheck `仅输出 Markdown`.

## OCR and Quality Gates

- image OCR uses local Tesseract when available
- scanned PDF OCR uses `pdftoppm + tesseract` when available
- content and Office quality gates can downgrade a run to markdown-only output
- when Office dependencies are missing, the app falls back to markdown outputs instead of failing hard

## Run, Test, and Build

Helper script:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron
```

Compatibility workflow entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron -Workflow
```

Compatibility admin entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron -WorkflowAdmin
```

Core package scripts:

```powershell
cd .\apps\dify-desktop
npm run smoke
npm run test:unit
npm run test:workflow-ui
npm run release:gate
npm run build:win
npm run build:win:installer
npm run release:oneclick
```

Helper packaging path:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_electron_compatibility.ps1 -Version "<version>"
```

Build artifacts are written under `apps/dify-desktop/dist`.

## Logs and Local State

Desktop config and logs live under Electron `userData`.

Useful generated files:

- `config.json`
- `logs/run_mode_audit.jsonl`
- `logs/route_metrics.jsonl`
- `logs/route_metrics_summary.json`
- `workflow_store/`

## Related Docs

- [quickstart_desktop_offline.md](quickstart_desktop_offline.md)
- [quickstart_native_winui.md](quickstart_native_winui.md)
- [frontend_convergence_decision_20260320.md](frontend_convergence_decision_20260320.md)
- [electron_compatibility_retirement_plan_20260321.md](electron_compatibility_retirement_plan_20260321.md)
- [dify_local_integration.md](dify_local_integration.md)
- [offline_delivery_minimal.md](offline_delivery_minimal.md)
- [finance_template_v1.md](finance_template_v1.md)

# AIWF Desktop Offline Quickstart

Use this path only when you explicitly need the Electron compatibility shell and offline engine without starting the backend service chain locally.

WinUI is the primary frontend. Electron is the secondary compatibility frontend.

## What This Mode Does

- default mode is `offline_local`
- no local SQL Server / Java / Python / Rust service is required
- the desktop app still supports explicit `base_api` mode when you need the backend later
- for image and XLSX enhanced ingest, the desktop app will prefer the local `glue-python` sidecar (`glueUrl`, default `http://127.0.0.1:18081`); plain CSV/TXT/PDF fallback paths remain available when the sidecar is unavailable

## Run or Package

Development run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron
```

That script installs dependencies, runs `npm run smoke`, and then launches the app.

Windows packaging:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_electron_compatibility.ps1 -Version "<version>"
```

Artifacts are written under `apps/dify-desktop/dist`.

## Default Desktop Behavior

- mode: `offline_local`
- output root:
  - `E:\Desktop_Real\AIWF` if `E:\Desktop_Real` exists
  - otherwise `Desktop\AIWF_Builds`
- built-in home screen and embedded `Workflow Studio`

## Inputs and Outputs

Supported offline inputs:

- `csv`
- `xlsx`
- `txt`
- `docx`
- `pdf`
- images: `png/jpg/jpeg/bmp/webp/tif/tiff`

Possible artifacts:

- `fin.xlsx`
- `audit.docx`
- `deck.pptx`
- markdown artifacts such as `evidence.md`, `ai_corpus.md`, and quality reports

The current UI enables `仅输出 Markdown` by default. Uncheck it if you want the Office trio.

## Common Usage

1. Launch the desktop app.
2. Keep `离线本地模式（推荐）`.
3. Drag files into the queue or fill the manual path fields.
4. Optional: run `模板预检`.
5. Choose a template if needed:
   - `default`
   - `debate_evidence_v1`
   - `finance_report_v1`
6. Click `开始生成`.

Current Fluent variants in the UI:

- `Fluent Light`
- `Fluent Strong`
- `Fluent Vibrant`

## OCR and Fallback Notes

- image OCR uses local Tesseract when available
- scanned PDF OCR uses `pdftoppm + tesseract` when available
- if Office dependencies or quality gates block the Office path, the app falls back to markdown outputs instead of failing hard

## Optional Backend Mode

The desktop app can also call your local AIWF backend in `base_api` mode. `offline_local` and `base_api` are explicit modes, not automatic failover paths.

See:

- [quickstart_native_winui.md](quickstart_native_winui.md)
- [frontend_convergence_decision_20260320.md](frontend_convergence_decision_20260320.md)
- [dify_desktop_app.md](dify_desktop_app.md)
- [dify_local_integration.md](dify_local_integration.md)

## Offline Bundle

To package a minimal offline delivery bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>" -PackageType installer
```

See:

- [offline_delivery_minimal.md](offline_delivery_minimal.md)
- [electron_compatibility_retirement_plan_20260321.md](electron_compatibility_retirement_plan_20260321.md)
- [finance_template_v1.md](finance_template_v1.md)

The bundle now also carries `contracts/desktop/`, `contracts/workflow/`, `contracts/rust/`, and `contracts/governance/` so template-related desktop contracts, workflow contracts, Rust authority manifests, and governance capability authority ship with the offline artifact.

For image/XLSX enhanced ingest validation, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_regression_quality.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1 -RequireAccel
```

If you want to validate the real desktop XLSX regression fixtures locally, install `apps/dify-desktop` dependencies first. The fixture test may skip on a machine without `exceljs`; that is expected for local environments and is not treated as a repository bug.

Optional local precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_desktop_fixture_deps.ps1
```

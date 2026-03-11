# AIWF Desktop Offline Quickstart

Use this path when you want the Electron desktop app and offline engine without starting the backend service chain locally.

## What This Mode Does

- default mode is `offline_local`
- no local SQL Server / Java / Python / Rust service is required
- the desktop app still supports `base_api` mode and offline fallback when you need it later

## Run or Package

Development run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1
```

That script installs dependencies, runs `npm run smoke`, and then launches the app.

Windows packaging:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1 -BuildWin -BuildInstaller
```

Artifacts are written under `apps/dify-desktop/dist`.

## Default Desktop Behavior

- mode: `offline_local`
- fallback policy: `smart`
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

The desktop app can also call your local AIWF backend in `base_api` mode and fall back to offline mode when configured.

See:

- [dify_desktop_app.md](dify_desktop_app.md)
- [dify_local_integration.md](dify_local_integration.md)

## Offline Bundle

To package a minimal offline delivery bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>" -PackageType installer
```

See:

- [offline_delivery_minimal.md](offline_delivery_minimal.md)
- [finance_template_v1.md](finance_template_v1.md)

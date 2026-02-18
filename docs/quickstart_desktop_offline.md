# AIWF Quickstart (Desktop Offline)

## Goal

Use desktop app only. No local SQL/Java/Rust/Python service required.

## 1. Install / Run

Build desktop exe (on build machine):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1 -BuildWin -BuildInstaller
```

Output:
- `apps/dify-desktop/dist/AIWF Dify Desktop <version>.exe` (portable single exe)
- `apps/dify-desktop/dist/AIWF Dify Desktop Setup <version>.exe` (installer, recommended)

## 2. Desktop Usage

1. Start app (default mode is `离线本地模式`).
2. Drag raw files into task queue (or fill input paths manually).
3. Click `开始生成`.

Recommended settings:
- `office_theme=assignment` for coursework deliverables.
- `office_theme=debate_plus` for debate materials.
- `office_quality_mode=high` for best layout quality.

Supported raw inputs in offline mode:
- `csv`
- `xlsx`
- `txt`
- `docx`
- `pdf`
- `png/jpg/jpeg/bmp/webp`

Output artifacts:
- `fin.xlsx`
- `audit.docx`
- `deck.pptx`

Default output root:
- `文档\AIWF-Offline\<job_id>\artifacts`

## 3. Optional Backend Mode

If you want to call your AIWF/Dify backend instead of local offline engine:

1. Switch mode to `连接你的 AIWF 后端`
2. Fill `baseUrl` and optional `API Key`
3. Click `检查状态` then `开始生成`

Related docs:
- `docs/dify_local_integration.md`

## 4. Cleanup Local Build Artifacts

```powershell
# preview only
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -DryRun -RemoveLogs

# execute cleanup
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -RemoveLogs
```

Optional cleanup for Rust build cache / nested repo metadata:

```powershell
# preview
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -DryRun -RemoveAccelTarget

# execute
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -RemoveAccelTarget

# dangerous: remove nested git metadata under apps/accel-rust/.git
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -RemoveAccelNestedGit -ForceDangerous
```

## 5. Build Offline Delivery Bundle

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>" -PackageType installer
```

Bundle output:
- `release\offline_bundle_<version>_installer\AIWF_Offline_Bundle`

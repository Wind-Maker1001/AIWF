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
3. Click `模板预检` (recommended), then click `开始生成`.

Recommended settings:
- `office_theme=assignment` for coursework deliverables.
- `office_theme=debate_plus` for debate materials.
- `office_quality_mode=high` for best layout quality.
- Finance statements: set `cleaning_template=finance_report_v1`.

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

### 2.1 Finance Template Quickstart (`finance_report_v1`)

Use this when your raw files are balance sheet / income statement / cashflow data.

1. In GUI, set `数据模板` to `财报模板 v1（资产/利润/现金流）`.
2. Keep `office_quality_mode=high`.
3. Drag `xlsx/csv/txt` with table headers into queue.
4. (Recommended) click `模板预检` and check:
   - missing required fields
   - amount convert rate
   - quality gate prediction
5. Click `开始生成`.

Template behavior summary:
- Rename: `Amt -> amount`, `ID -> id`
- Cast: `id:int`, `amount:float`, `currency:string`
- Required: `id`, `amount`
- Filter: `0 <= amount <= 100000000`
- Dedup: keep last by `id`

If you see failures:
- `required field missing`: check `id/amount` or source headers.
- `cast failed`: remove non-numeric symbols from amount.
- `empty output`: verify amount range and source quality.

Details:
- `docs/finance_template_v1.md`

Template management:
- In GUI `模板管理`, you can:
  - view template rules
  - disable/enable a template for current user
  - import/export template JSON (user-level)

## 3. Optional Backend Mode

If you want to call your AIWF/Dify backend instead of local offline engine:

1. Switch mode to `连接你的 AIWF 后端`
2. Fill `baseUrl` and optional `API Key`
3. Click `检查连通性` then `开始生成`

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

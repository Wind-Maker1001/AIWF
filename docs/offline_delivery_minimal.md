# AIWF Offline Delivery (Minimal Bundle)

## Goal

Build a minimal package that can be copied to another Windows machine and installed directly.

## 1. Build Desktop Exe

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1 -BuildWin -BuildInstaller
```

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
- desktop `.exe`（按 `PackageType`）
- optional `.blockmap`
- `README.txt`
- `SHA256SUMS.txt`
- docs (`quickstart_desktop_offline.md`, `dify_desktop_app.md`, this file)

Generate a single zip for copy:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\zip_offline_bundle.ps1
# or pin version explicitly
powershell -ExecutionPolicy Bypass -File .\ops\scripts\zip_offline_bundle.ps1 -Version "<version>" -PackageType installer
```

## 3. Validate Docs Links (Optional)

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_docs_links.ps1 -IncludeReadme
```

## 4. Copy to Target Machine

Copy the whole folder:
- `release\offline_bundle_<version>_installer\AIWF_Offline_Bundle`

On target machine:
1. Run desktop exe.
2. Launch app.
3. Keep `离线本地模式`.
4. Drag raw files into queue and click `开始生成`.

## 5. Notes

- Offline mode does not require local SQL/Java/Rust/Python services.
- OCR on images is enabled by default in Desktop GUI.
- If bundle includes `tools/` (from `-IncludeBundledTools`), app will优先使用内置 `tesseract/pdftoppm`。
- If runtime dependency is missing, app will auto-fallback and show warning.
- Default output path: `文档\AIWF-Offline\<job_id>\artifacts`.

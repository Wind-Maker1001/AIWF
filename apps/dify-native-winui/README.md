# AIWF Native Desktop (WinUI 3) - Bootstrap

This folder is the starting point for migrating the desktop GUI from Electron web UI to a native WinUI 3 shell.

## Current scope

- Native migration scaffold and architecture notes
- IPC bridge contract draft for integrating existing AIWF runtime services
- WinUI 3 MVP shell with:
  - run configuration inputs
  - run trigger buttons
  - artifacts/result panel
- Minimal bridge integration:
  - `GET /health`
  - `POST /jobs/{job_id}/run/{flow}`

## Build and smoke

Build:

```powershell
dotnet build .\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj -c Release
```

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_native_winui.ps1 -Configuration Debug
```

Smoke check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1 -Configuration Release
```

The smoke script now verifies startup marks and can enforce optional startup budgets
for first activation, `MainWindow` ctor, canvas init, and canvas prewarm.

UI Automation smoke:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_uia_smoke.ps1 -Configuration Release
```

The UIA smoke covers native window attach, workspace input edit, canvas command execution
(`新建画布` + snapshot file creation), and round-trip navigation across workspace/canvas/results.

## Planned migration order

1. Build native shell window + navigation
2. Port daily workflow pages first (run settings, queue, run status, artifacts)
3. Keep advanced/dev panels behind developer mode
4. Reuse existing backend/runtime APIs via local bridge
5. Replace Electron packaging with MSIX/installer pipeline

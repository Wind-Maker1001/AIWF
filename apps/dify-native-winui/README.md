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
  - `POST /run-cleaning`

## Build and smoke

Build (VS MSBuild):

```powershell
"D:\Environments\Microsoft Visual Studio\insiders\MSBuild\Current\Bin\amd64\MSBuild.exe" `
  .\AIWF.Native.WinUI.sln `
  /t:Restore,Build `
  /p:Configuration=Release `
  /p:Platform=x64
```

Smoke check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1
```

## Planned migration order

1. Build native shell window + navigation
2. Port daily workflow pages first (run settings, queue, run status, artifacts)
3. Keep advanced/dev panels behind developer mode
4. Reuse existing backend/runtime APIs via local bridge
5. Replace Electron packaging with MSIX/installer pipeline

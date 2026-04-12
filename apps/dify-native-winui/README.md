# AIWF Native Desktop (WinUI 3)

This folder contains the primary desktop frontend for AIWF.

Electron remains available as a secondary compatibility shell for Workflow Studio and transition-only use cases.

For this repository, the default release audience is personal/friend sideload distribution rather than enterprise trusted distribution.

## Current role

- Primary desktop frontend
- Native shell and architecture notes
- IPC bridge contract draft for integrating existing AIWF runtime services
- WinUI 3 MVP shell with:
  - run configuration inputs
  - cleaning precheck trigger
  - run trigger buttons
  - artifacts/result panel
- Minimal bridge integration:
  - `GET /health`
  - `POST /cleaning/precheck`
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

Publish:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\publish_native_winui.ps1 -Version "<version>" -Configuration Release
```

Bundle / release:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildWin -Configuration Release -Version "<version>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildInstaller -Configuration Release -Version "<version>" -CreateZip
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_bundle.ps1 -Version "<version>" -Configuration Release -CreateZip
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip
```

Optional MSIX preview:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release
```

Trusted signed MSIX / appinstaller:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode ProvidedPfx -PfxPath "<path-to-signing.pfx>" -PfxPassword "<password>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

Stable WinUI releases should use trusted signing plus `appinstaller`, not preview self-signed MSIX.

Trusted signing from a release host certificate store:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode StoreThumbprint -SigningThumbprint "<thumbprint>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

Bundle install entrypoint:

```powershell
.\Install_AIWF_Native_WinUI.cmd
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

# AIWF Native WinUI Quickstart

This is the current primary frontend path.

When you launch the AIWF desktop frontend, use WinUI by default rather than Electron.

The default WinUI launch path now ensures the local glue-python governance bridge is healthy first.
It also defaults manual review, quality rule sets, workflow app registry, workflow version storage, workflow run audit, and run baseline to backend-owned glue-python providers.

## Current Role

- WinUI is the primary desktop frontend.
- Electron remains only as a secondary compatibility shell and Workflow Studio compatibility entrypoint.

## Run

Recommended default entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1
```

Direct WinUI launch:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_native_winui.ps1 -Configuration Debug
```

If you need to bypass the bridge health/start check temporarily:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_native_winui.ps1 -Configuration Debug -SkipEnsureGlueBridge
```

## Build

```powershell
dotnet build .\apps\dify-native-winui\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj -c Release -p:Platform=x64
```

## Publish And Package

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildWin -Configuration Release -Version "<version>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildInstaller -Configuration Release -Version "<version>" -CreateZip
powershell -ExecutionPolicy Bypass -File .\ops\scripts\publish_native_winui.ps1 -Version "<version>" -Configuration Release
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip
```

Default release audience for this repo: `PersonalSideload`.
Default personal signing mode: `PersonalSideloadCert`.

Optional MSIX preview:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ensure_personal_sideload_certificate.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -ReleaseAudience PersonalSideload -SigningMode PersonalSideloadCert
```

Trusted signed MSIX:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode ProvidedPfx -PfxPath "<path-to-signing.pfx>" -PfxPassword "<password>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

For stable WinUI releases, trusted MSIX signing must include `-GenerateAppInstaller`.

Trusted signing from a release host certificate store:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode StoreThumbprint -SigningThumbprint "<thumbprint>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

Installed bundle entrypoint:

```powershell
.\Install_AIWF_Native_WinUI.cmd
```

## Smoke

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1 -Configuration Release
```

Optional UIA smoke:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_uia_smoke.ps1 -Configuration Release
```

## When To Still Use Electron

Use Electron only when you explicitly need:

- Workflow Studio compatibility entrypoint
- Existing Electron advanced diagnostics / compatibility tools
- Existing Electron compatibility packaging path

Current WinUI governance coverage:

- manual review queue
- manual review history
- approve / reject decision entry over the backend governance contract
- recent runs viewer
- run timeline viewer
- failure summary viewer
- audit event viewer
- quality rule set list / save / delete
- sandbox rule current state viewer/editor
- sandbox rule version list / rollback
- sandbox mute helper
- sandbox autofix state viewer/editor
- sandbox autofix action history viewer

Compatibility entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron
```

Compatibility workflow-only entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron -Workflow
```

Compatibility admin entrypoint:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -Frontend Electron -WorkflowAdmin
```

## Related Docs

- [frontend_convergence_decision_20260320.md](frontend_convergence_decision_20260320.md)
- [offline_delivery_native_winui.md](offline_delivery_native_winui.md)
- [personal_sideload_certificate_20260321.md](personal_sideload_certificate_20260321.md)
- [../apps/dify-native-winui/README.md](../apps/dify-native-winui/README.md)
- [quickstart_desktop_offline.md](quickstart_desktop_offline.md)

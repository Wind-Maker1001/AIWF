# AIWF Native WinUI Delivery

This is the delivery path for the primary frontend.

Recommended default release audience for this repository:

- `PersonalSideload`

Use `ManagedTrusted` only when you actually have a trusted signing environment and a real update host.

Recommended default signing mode for `PersonalSideload`:

- `PersonalSideloadCert`

## Publish WinUI

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\publish_native_winui.ps1 -Version "<version>" -Configuration Release
```

## Build A WinUI Bundle

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_bundle.ps1 -Version "<version>" -Configuration Release -CreateZip
```

Bundle output:

- `release\native_winui_bundle_<version>\AIWF_Native_WinUI_Bundle`
- `release\native_winui_bundle_<version>\AIWF_Native_WinUI_Bundle.zip`

Bundle contents:

- `app\`
- `docs\`
- `contracts\desktop\`
- `contracts\workflow\`
- `contracts\rust\`
- `contracts\governance\`
- `Install_AIWF_Native_WinUI.ps1`
- `Install_AIWF_Native_WinUI.cmd`
- `Uninstall_AIWF_Native_WinUI.ps1`
- `install_manifest.json`
- `README.txt`
- `manifest.json`
- `RELEASE_NOTES.md`
- `SHA256SUMS.txt`
- `contracts/desktop/template_pack_artifact.schema.json`
- `contracts/desktop/local_template_storage.schema.json`
- `contracts/desktop/office_theme_catalog.schema.json`
- `contracts/desktop/office_layout_catalog.schema.json`
- `contracts/desktop/cleaning_template_registry.schema.json`
- `contracts/desktop/offline_template_catalog_pack_manifest.schema.json`
- `contracts/workflow/workflow.schema.json`
- `contracts/workflow/render_contract.schema.json`
- `contracts/workflow/minimal_workflow.v1.json`
- `contracts/rust/operators_manifest.v1.json`
- `contracts/rust/operators_manifest.schema.json`
- `contracts/governance/governance_capabilities.v1.json`

## Primary Frontend Release Entry

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip
```

Primary release audit:

- `release\release_frontend_audit_<version>.json`
- includes `frontend_verification.primary` and `frontend_verification.compatibility`
- includes `architecture_scorecard`
- references the latest `ops\logs\frontend_verification\frontend_primary_verification_latest.json` and `frontend_compatibility_verification_latest.json`
- references `ops\logs\architecture\architecture_scorecard_release_ready_latest.json` and `architecture_scorecard_release_ready_latest.md`
- release is now blocked unless `architecture_scorecard_release_ready_latest.json` reports `overall_status = passed`

Optional MSIX preview:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ensure_personal_sideload_certificate.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -ReleaseAudience PersonalSideload -SigningMode PersonalSideloadCert
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip -IncludeMsix -ReleaseAudience PersonalSideload
```

Trusted signed MSIX / appinstaller:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode ProvidedPfx -PfxPath "<path-to-signing.pfx>" -PfxPassword "<password>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

Trusted signing on a release host with the certificate already installed:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_msix.ps1 -Version "<version>" -Configuration Release -SigningMode StoreThumbprint -SigningThumbprint "<thumbprint>" -CertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -AppInstallerUriBase "https://example.com/aiwf/winui"
```

For the main release wrapper:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_frontend_productize.ps1 -Version "<version>" -Frontend WinUI -Configuration Release -CreateZip -IncludeMsix -ReleaseAudience ManagedTrusted -MsixSigningMode ProvidedPfx -MsixPfxPath "<path-to-signing.pfx>" -MsixPfxPassword "<password>" -MsixCertificatePath "<path-to-signing.cer>" -GenerateAppInstaller -MsixAppInstallerUriBase "https://example.com/aiwf/winui"
```

Stable-channel rule:

- `stable` + `ReleaseAudience PersonalSideload` may use self-signed MSIX for friend/community sideload distribution.
- `stable` + `ReleaseAudience PersonalSideload` should prefer `PersonalSideloadCert`, so updates keep the same sideload identity.
- default personal release entrypoints warn when the certificate is within 30 days of expiry and block when within 14 days, unless explicitly overridden.
- `stable` + `ReleaseAudience ManagedTrusted` may not use preview self-signed signing unless `-AllowPreviewMsixOnStable` is explicitly passed for a local dry run.
- `stable` + `ReleaseAudience ManagedTrusted` + trusted MSIX signing must also generate an `appinstaller`.

Electron compatibility release path:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_electron_compatibility.ps1 -Version "<version>"
```

## Validation

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_frontend_convergence.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1 -Configuration Release
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_personal_sideload_certificate.ps1
```

If you want the release audit to carry fresh frontend verification state, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Compatibility
```

The shared async benchmark gate used by release verification now runs under tenant `bench_async`; if you change accel-rust tenant concurrency locally, keep `AIWF_ASYNC_BENCH_MAX_IN_FLIGHT` aligned with it.

## Install From Bundle

On the target machine:

1. Open the bundle folder.
2. Run `Install_AIWF_Native_WinUI.cmd` or `Install_AIWF_Native_WinUI.ps1`.
3. Launch `AIWF Native WinUI` from the Start Menu.

Default install root:

- `%LOCALAPPDATA%\Programs\AIWF\NativeWinUI`

## Electron Status

Electron remains available only for compatibility:

- Workflow Studio compatibility entrypoint
- transition-period diagnostics and governance panels
- legacy Electron packaging path

It is not the default primary delivery path anymore.

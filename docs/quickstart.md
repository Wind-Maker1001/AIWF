# AIWF Quickstart

Use this page for the fastest current local startup path.
For the full categorized documentation map, use [README.md](README.md).

## Current Default Path

- WinUI is the primary desktop frontend.
- Electron is the compatibility shell, not the default main path.
- `base-java` owns job lifecycle.
- `glue-python` owns governance state.

If you are trying to understand why those boundaries exist, read:

- [authority_execution_convergence_20260406.md](authority_execution_convergence_20260406.md)
- [governance_control_plane_boundary_20260324.md](governance_control_plane_boundary_20260324.md)
- [frontend_convergence_decision_20260320.md](frontend_convergence_decision_20260320.md)

## 1. Bring Up Backend Services

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Use [quickstart_backend.md](quickstart_backend.md) if you need backend-only setup details.

## 2. Launch the Primary Frontend

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1
```

For the native frontend directly:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_native_winui.ps1 -Configuration Debug
```

Use [quickstart_native_winui.md](quickstart_native_winui.md) if you need WinUI-specific prerequisites.

## 3. Run Local Verification

Fast local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
```

Default local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

Use [verification.md](verification.md) for the exact gate and profile coverage.

## Compatibility

Only use these if you explicitly need the compatibility flow:

- [quickstart_desktop_offline.md](quickstart_desktop_offline.md)
- [dify_desktop_app.md](dify_desktop_app.md)
- [offline_delivery_minimal.md](offline_delivery_minimal.md)
- [electron_compatibility_retirement_plan_20260321.md](electron_compatibility_retirement_plan_20260321.md)

## 5. Next Docs To Read

- Full documentation map: [README.md](README.md)
- Native WinUI delivery: [offline_delivery_native_winui.md](offline_delivery_native_winui.md)
- Cleaning rules: [cleaning_rules.md](cleaning_rules.md)
- Dify/local backend integration: [dify_local_integration.md](dify_local_integration.md)
- Personal sideload certificate: [personal_sideload_certificate_20260321.md](personal_sideload_certificate_20260321.md)

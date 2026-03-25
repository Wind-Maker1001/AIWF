# Architecture Gate Inventory Addendum (2026-03-25)

This addendum records one gate-inventory change after `docs/architecture_gate_inventory_20260324.md`.

## New core boundary gate

`ops/scripts/check_governance_control_plane_boundary.ps1`

- owner: governance control-plane boundary
- source authority: `apps/glue-python/aiwf/governance_surface.py`
- blocking intent: stop drift between real glue-python `/governance/*` runtime routes and declared governance ownership metadata

## Why it is a core boundary gate

This gate is not another persistence check.
It protects the split that the repository has now explicitly accepted:

- `base-java` owns job lifecycle
- `glue-python` owns governance state

Without this gate, the repo could silently drift in either direction:

- new governance routes could appear without ownership metadata
- governance metadata could start claiming lifecycle semantics
- `/capabilities` metadata could drift away from runtime route reality

## What is wired now

The gate is now wired into:

- `ops/scripts/ci_check.ps1`
- `ops/scripts/package_offline_bundle.ps1`
- `ops/scripts/package_native_winui_bundle.ps1`
- `ops/scripts/package_native_winui_msix.ps1`
- `ops/scripts/release_productize.ps1`
- `ops/scripts/release_frontend_productize.ps1`
- `ops/scripts/release_electron_compatibility.ps1`

This means the boundary is enforced in both CI and release/package paths, not only in local analysis.

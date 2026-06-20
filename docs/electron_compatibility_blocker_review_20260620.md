# Electron Compatibility Blocker Review

Date: 2026-06-20

## Summary

Electron remains a compatibility-only shell.

WinUI now covers the primary run path, the primary release path, and every capability still listed as `compat-hidden` in the machine-readable Electron capability inventory.

The remaining reason Electron still exists is not missing WinUI parity. It is bounded compatibility retention for:

- explicit script/argv-only `Workflow Studio` entrypoints
- the explicit offline-home helper shell
- the dedicated Electron installer/portable release wrapper
- admin-gated and release-disabled runtime/dev debug helpers

## Why Electron Still Exists On 2026-06-20

- existing users still have a compatibility path for installer/portable Electron artifacts
- the explicit offline-home helper shell is still useful for bounded offline workflows
- the explicit `Workflow Studio` compatibility entrypoint has not been intentionally removed yet
- dev/debug helpers are already gated, but not yet deleted

No new primary-path work should land in Electron under this blocker review.

## Current Boundaries

- WinUI remains the primary frontend and primary release path
- Electron remains compatibility-only and must stay off default onboarding
- governance, diagnostics, publish, compare, review, and pack-management surfaces remain `compat-hidden`
- Electron packaging must continue to live behind `ops/scripts/release_electron_compatibility.ps1`

## Next Checkpoint

- reviewed_at: `2026-06-20`
- next_review_by: `2026-07-31`
- machine-readable authority: `contracts/desktop/electron_compatibility_inventory.v1.json`

By `2026-07-31`, either:

- the retained compatibility shell is reduced again, or
- another dated blocker review must explain the remaining Electron-only retention in concrete terms

## Related Docs

- [electron_compatibility_retirement_plan_20260321.md](electron_compatibility_retirement_plan_20260321.md)
- [electron_capability_inventory_20260321.md](electron_capability_inventory_20260321.md)
- [../contracts/desktop/electron_compatibility_inventory.v1.json](../contracts/desktop/electron_compatibility_inventory.v1.json)

# Repository Optimization Snapshot (2026-02-16)

## Completed in this round

- Added deterministic docs-link validation script:
  - `ops/scripts/check_docs_links.ps1`
- Added offline delivery bundle packer:
  - `ops/scripts/package_offline_bundle.ps1`
- Extended workspace cleanup script with optional heavy cleanup switches:
  - `-RemoveAccelTarget`
  - `-RemoveAccelNestedGit`
  - `-ForceDangerous` (required when removing nested git metadata)
- Added docs checks into CI entry script:
  - `ops/scripts/ci_check.ps1` (default enabled, can be skipped by `-SkipDocsChecks`)
- Updated quickstart and handoff docs to include new operational paths.

## Verification result

- Docs local links check: pass
- Cleanup script dry-run: pass
- Offline bundle generation: pass
  - output: `release/offline_bundle_<version>/AIWF_Offline_Bundle`

## Current high-value cleanup candidates (manual decision)

1. `apps/accel-rust/target/`
- Large local build cache.
- Safe to remove; can be rebuilt.

2. `apps/accel-rust/.git/`
- Nested repo metadata under monorepo path.
- Remove only if you no longer need independent history.

3. `docs/archive/dify_standalone_frontend_legacy_20260216.md`
- Legacy compatibility doc only (already archived).

## Recommended commands

```powershell
# 1) Check docs local links
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_docs_links.ps1 -IncludeReadme

# 2) Dry-run heavy cleanup
powershell -ExecutionPolicy Bypass -File .\ops\scripts\clean_workspace_artifacts.ps1 -DryRun -RemoveLogs -RemoveAccelTarget

# 3) Build offline bundle
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_offline_bundle.ps1 -Version "<version>"
```

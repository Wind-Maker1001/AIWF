# AIWF Verification Guide

This document maps local verification entrypoints to the actual CI scripts in the repository.

## Local CI Entry Points

Fast local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
```

The quick profile still runs:

- docs link checks
- release evidence checks
- OpenAPI / SDK sync checks
- secret scan
- encoding checks
- Rust / Java / Python tests
- rust transform benchmark self-test
- desktop unit and UI checks
- native WinUI primary frontend smoke outside CI unless you explicitly skip it

The quick profile now auto-skips:

- Electron compatibility packaged-startup checks
- regression quality and heavier benchmark / chaos suites

Default / full local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

The default / full path now keeps WinUI as the primary frontend verification surface.
Electron compatibility packaged-startup checks have moved to an explicit compatibility stage.

Full local profile:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Full
```

The full profile adds:

- smoke
- invalid parquet fallback integration
- contract tests
- chaos checks
- routing and async benchmark gates
- rust transform and new-ops benchmark gates

Explicit Electron compatibility stage:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Compatibility
```

The compatibility profile focuses on the secondary Electron package path:

- Electron compatibility packaged-startup checks
- frontend convergence checks
- docs / release-evidence / lightweight repository guards unless you explicitly skip them

It auto-skips backend suites, native WinUI smoke, desktop UI tests, and heavier benchmark / chaos paths.

Frontend verification evidence written by `ci_check.ps1`:

- `ops/logs/frontend_verification/frontend_primary_verification_latest.json`
- `ops/logs/frontend_verification/frontend_compatibility_verification_latest.json`

OpenAPI / SDK sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_openapi_sdk_sync.ps1
```

This gate now enforces:

- checked-in Rust OpenAPI `contracts/rust/openapi.v2.yaml` keeps the current handwritten Python client surface covered
- current handwritten client `apps/glue-python/aiwf/rust_client.py` exposes the active transform path functions such as `transform_rows_v3`, `postprocess_rows_v1`, and `quality_check_v2`
- legacy helper `ops/scripts/generate_rust_client.ps1` remains auxiliary only; it is not the current authority path for the Python client surface

Architecture scorecard written by `ci_check.ps1`:

- `ops/logs/architecture/architecture_scorecard_latest.json`
- `ops/logs/architecture/architecture_scorecard_latest.md`
- `ops/logs/architecture/architecture_scorecard_release_ready_latest.json`
- `ops/logs/architecture/architecture_scorecard_release_ready_latest.md`

Workflow contract sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_workflow_contract_sync.ps1
```

This gate now enforces:

- `contracts/workflow/workflow.schema.json` keeps `workflow_id`, `version`, `nodes`, and `edges` as required top-level fields
- desktop default workflow graph keeps explicit top-level `version`
- desktop import normalization still migrates missing `version`
- desktop import path still rejects unregistered node types
- desktop run-payload path still rejects unregistered node types
- desktop authoring `nodeType` surface still disables adding unknown node types
- desktop preflight UI still emits explicit `unknown_node_type` contract guidance
- failure output stays machine-readable so the architecture scorecard can call out import / run / authoring / preflight workflow-contract regressions explicitly

The architecture scorecard and release-ready scorecard now surface `authoring_rejected_unknown_type` and `preflight_unknown_type_guided` under `workflow_contract_sync.details`.

Governance control plane boundary gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_governance_control_plane_boundary.ps1
```

This gate now enforces:

- every glue-python `/governance/*` runtime route stays covered by an explicit governance surface entry
- governance surface metadata stays explicit about `control_plane_role`, `state_owner`, `job_lifecycle_control_plane_owner`, and `lifecycle_mutation_allowed`
- `lifecycle_mutation_allowed` stays `false`, so governance surfaces do not silently absorb job-lifecycle semantics
- the explicit boundary route `/governance/meta/control-plane` stays present
- the governance capability map in `/capabilities` stays aligned with the governance surface authority instead of drifting into a second handwritten metadata layer
- checked-in governance capability manifest `contracts/governance/governance_capabilities.v1.json` stays aligned with glue-python governance surface authority
- governance capability export script `ops/scripts/export_governance_capabilities.ps1` now regenerates the checked-in manifest and generated frontend constants directly from `apps/glue-python/aiwf/governance_surface.py`
- checked-in desktop capability asset `apps/dify-desktop/workflow_governance_capabilities.generated.js` stays aligned with the governance capability manifest
- checked-in WinUI capability asset `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/GovernanceCapabilities.Generated.cs` stays aligned with the governance capability manifest
- governance capability export script `ops/scripts/export_governance_capabilities.ps1` remains the checked-in regeneration entrypoint for those desktop and WinUI assets

The architecture scorecard and release-ready scorecard now surface this boundary as `governance_control_plane_boundary`.

Governance store schema version gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_governance_store_schema_versions.ps1
```

This gate now enforces:

- backend-owned governance / publishing stores keep `schema_version` on their normalized output objects
- `workflow_app_registry_store.js`, `workflow_quality_rule_store.js`, `workflow_manual_review_store.js`, `workflow_version_store.js`, `workflow_run_baseline_store.js`, `workflow_sandbox_rule_store.js`, and `workflow_sandbox_autofix_store.js` keep explicit version markers in source
- workflow run audit no longer belongs to this default governance-owned store set; its default owner is now local runtime, with remote lifecycle providers used only explicitly through `base_http`
- sandbox governance paths keep `schema_version` on sandbox rules, rule versions, compare payloads, rollback payloads, autofix state, and autofix action history
- failure output stays machine-readable so the architecture scorecard can call out missing source markers or missing runtime schema-version outputs explicitly

The architecture scorecard and release-ready scorecard now surface this boundary as `governance_store_schema_versions`.

Local workflow store schema version gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_local_workflow_store_schema_versions.ps1
```

This gate now enforces:

- desktop local workflow shell containers stay minimally shaped and do not regain long-lived `schema_version`
- `workflow_task_queue.json`, `workflow_queue_control.json`, `workflow_node_cache.json`, `workflow_node_cache_metrics.json`, and `template_marketplace.json` are written as minimal local shell containers
- only persisted template pack entries continue to keep explicit `template_pack_entry.v1`
- legacy unversioned queue / queue-control / template-marketplace / node-cache payloads still migrate on read instead of hard-failing silently
- failure output stays machine-readable so the architecture scorecard can call out unexpected shell-object versioning or missing legacy migration paths explicitly

The architecture scorecard and release-ready scorecard now surface this boundary as `local_workflow_store_schema_versions`.

Template pack contract sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_template_pack_contract_sync.ps1
```

This gate now enforces:

- legacy template pack artifacts without explicit `schema_version` still migrate into `template_pack_artifact.v1`
- marketplace entries continue to use `template_pack_entry.v1`
- exported template pack files are always written as `template_pack_artifact.v1`
- template pack artifacts keep non-empty template arrays and template graphs that still satisfy workflow contract requirements
- failure output stays machine-readable so the architecture scorecard can call out import migration loss, marketplace/artifact schema drift, or empty exported packs explicitly

Schema file:

- `contracts/desktop/template_pack_artifact.schema.json`

The architecture scorecard and release-ready scorecard now surface this boundary as `template_pack_contract_sync`.

Local template storage contract sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_local_template_storage_contract_sync.ps1
```

This gate now enforces:

- renderer local custom templates stored under `aiwf.workflow.templates.v1` keep a versioned envelope `local_template_storage.v1`
- each persisted local custom template keeps `local_template_entry.v1`
- legacy bare-array localStorage payloads still migrate on read
- `saveCurrentAsTemplate` continues to write versioned local template storage instead of reverting to a bare array
- failure output stays machine-readable so the architecture scorecard can call out missing local template migration or versioned-save regressions explicitly

Schema file:

- `contracts/desktop/local_template_storage.schema.json`

The architecture scorecard and release-ready scorecard now surface this boundary as `local_template_storage_contract_sync`.

Offline template catalog sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_offline_template_catalog_sync.ps1
```

This gate now enforces:

- `rules/templates/office_themes_desktop.json` keeps a versioned `office_theme_catalog.v1`
- `rules/templates/office_layouts_desktop.json` keeps a versioned `office_layout_catalog.v1`
- `rules/templates/cleaning_templates_desktop.json` keeps a versioned `cleaning_template_registry.v1`
- `offline_engine_config.js` still accepts legacy unversioned catalogs on read, so existing local files do not break
- failure output stays machine-readable so the architecture scorecard can call out theme/layout/registry schema drift or missing legacy migration paths explicitly

Schema files:

- `contracts/desktop/office_theme_catalog.schema.json`
- `contracts/desktop/office_layout_catalog.schema.json`
- `contracts/desktop/cleaning_template_registry.schema.json`

The architecture scorecard and release-ready scorecard now surface this boundary as `offline_template_catalog_sync`.

Offline template catalog pack manager:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\template_pack_manager.ps1 -Action export -Version "<version>"
```

This tool now emits a versioned pack manifest for offline template catalogs instead of a bare manifest blob.

Manifest schema file:

- `contracts/desktop/offline_template_catalog_pack_manifest.schema.json`

Node config schema coverage gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_node_config_schema_coverage.ps1
```

This gate now enforces:

- canonical node config contract asset `contracts/desktop/node_config_contracts.v1.json` stays aligned with Rust authoritative workflow validation consumption
- glue authoritative validation client continues to route through `/operators/workflow_contract_v1/validate`
- desktop workflow helper code and packaging manifests do not retain generated node-config helper dependencies
- a minimum `nested_shape_constrained` count
- a required nested-shape set for critical nodes such as `load_rows_v3`, `quality_check_v3`, `quality_check_v4`, `office_slot_fill_v1`, `optimizer_v1`, `parquet_io_v2`, `plugin_registry_v1`, `transform_rows_v3`, `lineage_v3`, `rule_simulator_v1`, `constraint_solver_v1`, and `udf_wasm_v2`
- failure output stays machine-readable so the release-ready scorecard can call out required nested coverage gaps, Rust authority drift, and generated-helper retirement drift explicitly

Contract and authority assets:

- `contracts/desktop/node_config_contracts.v1.json`
- `apps/accel-rust/src/governance_ops/contracts/workflow_contract.rs`
- `apps/accel-rust/src/http/handlers_extended/platform.rs`
- `apps/glue-python/aiwf/workflow_validation_client.py`

Current note:

- node-config authority lives in `contracts/desktop/node_config_contracts.v1.json`; the main executable consumer is now Rust, while desktop workflow helpers keep UI-only formatting and migration behavior

Each run also writes timestamped snapshots beside the `*_latest.json` files.

Workflow preflight report export contract:

- `contracts/desktop/preflight_report_contract.v1.json`
- `contracts/desktop/preflight_report_contract.schema.json`

The desktop report support now exports JSON preflight reports as `workflow_preflight_report_export.v1` envelopes instead of raw ad-hoc objects.

Local node catalog policy gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_local_node_catalog_policy.ps1
```

This gate now enforces:

- checked-in local node presentation map `apps/dify-desktop/renderer/workflow/local-node-presentations.js` stays aligned with local authoring node types
- checked-in local node palette policy `apps/dify-desktop/renderer/workflow/local-node-palette-policy.js` stays aligned with local authoring node ordering and section policy
- runtime `defaults-catalog.js` keeps local node `name` / `desc` / `group` / `policy_section` aligned with local policy truth
- failure output stays machine-readable so the release-ready scorecard can call out local node presentation drift, palette policy drift, and local catalog metadata drift explicitly

Each run also writes timestamped snapshots beside the `*_latest.json` files.

Operator catalog sync gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_operator_catalog_sync.ps1
```

This gate now enforces:

- checked-in Rust operator manifest `contracts/rust/operators_manifest.v1.json` stays in sync with Rust source truth
- checked-in Rust operator manifest schema `contracts/rust/operators_manifest.schema.json` stays present as the versioned contract for that manifest
- checked-in desktop Rust operator manifest module `apps/dify-desktop/workflow_chiplets/domains/rust_operator_manifest.generated.js` stays in sync with the checked-in manifest
- checked-in renderer Rust operator manifest module `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js` stays in sync with the checked-in manifest
- checked-in Rust operator presentation map `apps/dify-desktop/renderer/workflow/rust-operator-presentations.js` stays aligned with the renderer desktop-exposable Rust operator surface
- checked-in Rust operator palette policy `apps/dify-desktop/renderer/workflow/rust-operator-palette-policy.js` stays aligned with renderer desktop-exposable Rust operator domains and ordering policy
- every published Rust operator exposed by `operator_catalog_data.rs` must stay routed by desktop `rust_ops_domain.js`
- every published Rust operator must stay routed by desktop `rust_ops_domain.js`
- every non-`palette_hidden` Rust operator marked desktop-exposable by manifest must stay represented in desktop `defaults-catalog.js`
- every Rust operator marked desktop-exposable by manifest must stay routed by desktop `rust_ops_domain.js`
- `palette_hidden` Rust operators must remain present in manifest/generated modules but must not be required in the default hand-authored palette surface
- desktop Rust routing and desktop Rust catalog must not expose operators outside manifest desktop exposure
- renderer Rust operator presentations must cover every desktop-exposable Rust operator with non-empty `name` / `desc`, and must not keep stale entries
- renderer Rust operator palette policy must cover every desktop-exposable Rust operator domain, and pinned order entries must not drift or duplicate
- failure output stays machine-readable so the release-ready scorecard can call out manifest drift, published Rust operator drift, and desktop exposure drift explicitly

Each run also writes timestamped snapshots beside the `*_latest.json` files.

Rust operator manifest export:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\export_operator_manifest.ps1
```

This script regenerates `contracts/rust/operators_manifest.v1.json` from Rust catalog and workflow source files.
The manifest is expected to conform to `contracts/rust/operators_manifest.schema.json`.
It also regenerates `apps/dify-desktop/workflow_chiplets/domains/rust_operator_manifest.generated.js` for desktop runtime consumption.
It also regenerates `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js` for renderer-side Rust palette filtering.

Rust client generation note:

- `ops/scripts/generate_rust_client.ps1` remains an auxiliary helper for OpenAPI exploration only
- the current repository keeps `apps/glue-python/aiwf/rust_client.py` as the maintained hand-written client surface
- OpenAPI/client sync is currently enforced by external contract tests rather than by a checked-in generated SDK artifact

If your local machine does not have SQL ready yet, add `-SkipSqlConnectivityGate`.

## Component-Level Checks

Rust:

```powershell
cd .\apps\accel-rust
cargo test -q
```

Python:

```powershell
cd .\apps\glue-python
python -m unittest discover -s tests -v
```

See also:

- [glue_python_regression_checklist.md](glue_python_regression_checklist.md)

Java:

```powershell
cd .\apps\base-java
mvn -q test
```

Desktop:

```powershell
cd .\apps\dify-desktop
npm run smoke
npm run test:unit
npm run test:workflow-ui
```

Native WinUI primary frontend:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1 -Configuration Release
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildWin -Configuration Release -Version "<version>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_aiwf_frontend.ps1 -BuildInstaller -Configuration Release -Version "<version>" -CreateZip
powershell -ExecutionPolicy Bypass -File .\ops\scripts\publish_native_winui.ps1 -Version "<version>" -Configuration Release
powershell -ExecutionPolicy Bypass -File .\ops\scripts\package_native_winui_bundle.ps1 -Version "<version>" -Configuration Release
```

Electron compatibility packaged startup checks:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_desktop_packaged_startup.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_desktop_lite_packaged_startup.ps1
```

These Electron package checks are compatibility-only. Keep them in full validation or run them explicitly when you are changing Electron packaging paths.

Sidecar release verification:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_regression_quality.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_sidecar_python_rust_consistency.ps1 -RequireAccel
```

Release wrappers and offline bundle packaging now consume those reports directly:

- `ops/scripts/release_frontend_productize.ps1`
- `ops/scripts/release_electron_compatibility.ps1`
- `ops/scripts/package_offline_bundle.ps1`

Cleaning Rust v2 rollout gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_cleaning_rust_v2_rollout.ps1
```

This gate consumes:

- the latest `run_mode_audit.jsonl` evidence produced for cleaning rollout validation
- `ops/logs/regression/sidecar_python_rust_consistency_report.json`
- a stable sample result with `execution.shadow_compare`
- in release/package mode, the latest acceptance evidence such as `ops/logs/acceptance/desktop_real_sample/cleaning_shadow_rollout.json`

It blocks when:

- `execution.shadow_compare` is missing required structure
- `run_mode_audit.jsonl` is missing rollout fields such as `execution_mode`, `requested_rust_v2_mode`, `effective_rust_v2_mode`, `verify_on_default`, or `shadow_compare_status`
- the sidecar Python/Rust consistency report contains any `reason_counts` mismatch

`run_sidecar_python_rust_consistency.ps1` remains the source that produces the consistency report; the rollout gate only consumes that evidence for release and readiness decisions.

Current default release expectation:

- release/package do not auto-run acceptance
- they require the latest default+verify acceptance evidence for `desktop_real_sample` and `desktop_finance_template`
- those evidence files must point to real `run_mode_audit.jsonl` and `cleaning_result.json`, not synthetic gate samples
- those evidence files must come from glue `run_cleaning`
- release-readiness rule: `requested_rust_v2_mode == "default"` and `verify_on_default == true`
- release-readiness rule: `execution_mode == "rust_v2"` and `shadow_compare.status == "matched"`

For local desktop fixture verification, `offline_ingest_fixture_assets.test.js` may skip the real XLSX fixture path if `exceljs` is not installed. That is expected locally; use `ops/scripts/check_desktop_fixture_deps.ps1` before treating it as a regression.

## Backend Smoke and Fallback

Restart the full local backend:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Smoke:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

Fallback validation:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\test_invalid_parquet_fallback.ps1
```

## Dependency and Security Checks

Runtime dependency precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_runtime_deps.ps1
```

Developer tool precheck:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_dev_tools.ps1
```

RustSec audit:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_cargo_audit.ps1
```

## Performance Gates

Rust transform gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_transform_bench_gate.ps1
```

Rust new-ops gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_new_ops_bench_gate.ps1
```

Routing benchmark gate:

```powershell
cd .\apps\dify-desktop
npm run bench:routing
```

Async benchmark trend gate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_async_bench_trend.ps1
```

This gate now runs against tenant `bench_async` by default and uses quota-respecting submission with `AIWF_ASYNC_BENCH_MAX_IN_FLIGHT=4`.
If you raise `AIWF_TENANT_MAX_CONCURRENCY` locally for accel-rust benchmark runs, raise `AIWF_ASYNC_BENCH_MAX_IN_FLIGHT` to the same value so trend samples stay comparable.

## GitHub Workflows

GitHub-hosted quick workflow:

- file: `.github/workflows/ci.yml`
- actual command:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick -SkipToolChecks -SkipSqlConnectivityGate -SkipDesktopUiTests
```

Self-hosted full workflow:

- file: `.github/workflows/full-integration-self-hosted.yml`
- default behavior: runs `ci_check.ps1` on a Windows self-hosted runner
- `run_full_integration=false` skips smoke in that workflow
- scheduled runs execute from the default branch only
- Electron compatibility packaged-startup checks are now a separate explicit stage; run `ci_check.ps1 -CiProfile Compatibility` when changing Electron packaging paths

## CI Helper Scripts

- `ops/scripts/dispatch_full_integration_self_hosted.ps1`
- `ops/scripts/get_ci_status.ps1`
- `ops/scripts/verify_branch_ci.ps1`

Use those scripts when you want to validate branch CI from a local PowerShell session.
`get_ci_status.ps1` and `verify_branch_ci.ps1` now also surface the local `architecture_scorecard_release_ready_latest.json` summary when it exists.

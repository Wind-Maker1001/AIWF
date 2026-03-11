# AIWF Project Audit (2026-02-18)

## Scope
- Rust accelerator: `apps/accel-rust`
- Python glue: `apps/glue-python`
- Desktop app: `apps/dify-desktop`
- Base API: `apps/base-java`
- Contracts and ops scripts

## Landed Capabilities
1. `transform_rows_v2` supports rich filters (`in/not_in/regex/not_regex`), quality gates, and aggregates.
2. `text_preprocess_v2` supports markdown-oriented paper pre-clean.
3. Async task lifecycle is complete: submit, poll, cancel.
4. Task store supports `base_api`, `sqlcmd`, and `odbc`.
5. Tenant field (`tenant_id`) is wired across Rust -> Base API -> SQL.
6. Runtime metrics and task-store health metrics are exposed at `/metrics`.
7. CI gates include routing benchmark, async benchmark trend, contract checks, and chaos checks.
8. OpenAPI contract upgraded to component-complete request/response schemas (`contracts/rust/openapi.v2.yaml`).
9. Desktop runtime dependency checks support bundled tools/fonts in addition to system dependencies.
10. CI now includes explicit OpenAPI <-> SDK sync gate (`ops/scripts/check_openapi_sdk_sync.ps1`).
11. Desktop UI supports one-click bundled font installation for current user on Windows.

## Risk Closure (3 items)
1. ODBC SQL safety:
   - Closed by switching ODBC task-store write/read/cancel paths to parameterized execution.
   - Also added backend auto-fallback: `odbc -> sqlcmd -> base_api -> disable`.
2. OpenAPI completeness:
   - Closed by expanding endpoint-level contract to schema-complete components for current API surface.
3. Offline runtime dependency rigidity:
   - Closed by adding bundled dependency detection and fallback for OCR/fonts in runtime checks.

## Current Low Risks
- Output quality may still vary on target machines if document viewers do not have suitable CJK fonts.
- Contract is complete for current endpoints; any future fields require version bump and SDK regeneration.

## Latest Validation (2026-02-18)
- `cargo test -q` (accel-rust): pass (12 tests)
- `mvn -q test` (base-java): pass
- `python -m unittest tests.test_cleaning_flow -v` (glue-python): pass
- `npm run test:unit` (dify-desktop): pass
- `powershell -File ops/scripts/check_runtime_deps.ps1`: pass
- `powershell -File ops/scripts/ci_check.ps1`: pass (full pipeline, packaging checks included)

## Key Changed Files in This Audit Cycle
- `apps/accel-rust/src/main.rs`
- `contracts/rust/openapi.v2.yaml`
- `apps/dify-desktop/main_runtime_fonts.js`
- `apps/dify-desktop/renderer/index.html`
- `ops/scripts/check_runtime_deps.ps1`
- `docs/codex_handoff.md`

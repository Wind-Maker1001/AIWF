# Codex Handoff (Updated: 2026-02-19)

## 1) Project State
- Primary delivery path: `apps/dify-desktop` (Windows-first, offline-capable, one-click GUI).
- Optional service chain: `apps/base-java` + `apps/glue-python` + `apps/accel-rust`.
- Current pipeline is stable in CI for:
  - ingest/preprocess/cleaning
  - Office outputs (`xlsx/docx/pptx`) + Markdown outputs
  - workflow canvas + routing benchmark + package startup
  - SQL persistence + fallback integration + async/contract/chaos checks

## 2) Landed Capabilities
- Rust operators:
  - `transform_rows_v2` (sync + async submit/poll/cancel + stream/resume/checkpoint)
  - `aggregate_rows_v1`, `quality_check_v1`, `aggregate_pushdown_v1`
  - `text_preprocess_v2`
  - `plugin_exec_v1` + `plugin_health_v1`
- Plugin hardening:
  - default-off enable gate
  - tenant/plugin/command allowlists
  - signature verification
  - manifest validation (`name/version/api_version`) and compatibility gate
  - output-size cap and stream-safe stdout/stderr reads
- Data safety:
  - tenant row/payload/workflow-step quotas
  - `input_uri` jsonl/csv streaming + byte/row limits
  - SQL where-clause validation and parameterized SQL paths
- Desktop UX/runtime:
  - drag-drop queue ingestion (`PDF/docx/txt/image/xlsx/csv`)
  - startup self-check panel (fonts/OCR/pdftoppm/task-store diagnostics + suggestions)
  - runtime health checks, font install action, route diagnostics panel
- Privacy hardening:
  - `ops/config/dev.env` removed from git tracking; use local file + committed `ops/config/dev.env.example`
  - default egress-off gate for external AI/OTLP (`AIWF_ALLOW_EGRESS=false`)
  - CI secret scan gate (`ops/scripts/secret_scan.ps1`)
  - glue traceback debug disabled in production/release mode
- Office output quality:
  - desktop and glue both support replaceable layout/theme templates
  - paper-clean profile configurable for citation/reference stripping
  - desktop office outputs now enforce anti-overflow paging in PPTX evidence/warnings
  - desktop docx/xlsx/pptx support mixed insertion of table + image artifacts
  - desktop office text path includes mojibake fallback cleanup for corrupted lines

## 3) Decoupled Templates / Profiles
- Desktop office themes: `rules/templates/office_themes_desktop.json`
- Desktop office layouts: `rules/templates/office_layouts_desktop.json`
- Assignment template pack v1 defaults landed in desktop theme/layout files
- Glue office layouts: `rules/templates/office_layouts.json`
- Paper markdown clean profile: `rules/templates/paper_clean_profile.json`
- Observability assets:
  - `ops/observability/dashboard_accel_rust.json`
  - `ops/observability/alerts_accel_rust.yml`
  - `ops/observability/README.md`

## 4) Runtime Env Vars
- Task store:
  - `AIWF_RUST_TASK_STORE_REMOTE=true|false`
  - `AIWF_RUST_TASK_STORE_BACKEND=base_api|sqlcmd|odbc`
  - `AIWF_RUST_TASK_STORE_PATH`
  - `AIWF_RUST_TASK_TTL_SEC`
  - `AIWF_RUST_TASK_MAX`
- SQL:
  - `AIWF_SQL_HOST`
  - `AIWF_SQL_PORT`
  - `AIWF_SQL_DB`
  - `AIWF_SQL_USER`
  - `AIWF_SQL_PASSWORD`
  - `AIWF_SQL_USE_WINDOWS_AUTH=true|false`
- Plugin security:
  - `AIWF_PLUGIN_ENABLE=true`
  - `AIWF_PLUGIN_TENANT_ALLOWLIST`
  - `AIWF_PLUGIN_ALLOWLIST`
  - `AIWF_PLUGIN_COMMAND_ALLOWLIST`
  - `AIWF_PLUGIN_SIGNING_SECRET`
  - `AIWF_PLUGIN_MAX_OUTPUT_BYTES`
- Observability:
  - `AIWF_OTEL_EXPORTER_OTLP_ENDPOINT`
  - `AIWF_ALLOW_EGRESS=true|false`
  - `AIWF_DEBUG_ERRORS=true|false`
- Optional template overrides:
  - `AIWF_OFFICE_THEME_FILE_DESKTOP`
  - `AIWF_OFFICE_LAYOUT_FILE_DESKTOP`
  - `AIWF_OFFICE_THEME_FILE` (glue)
  - `AIWF_OFFICE_LAYOUT_FILE` (glue)

## 5) Acceptance Checklist
1. `powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1`
2. `cargo test -q` (in `apps/accel-rust`)
3. `mvn -q test` (in `apps/base-java`)
4. `python -m unittest tests.test_cleaning_flow -v` (in `apps/glue-python`)
5. `npm run test:unit` and `npm run test:workflow-ui` (in `apps/dify-desktop`)
6. `powershell -ExecutionPolicy Bypass -File .\ops\scripts\acceptance_desktop_real_sample.ps1`

Latest desktop real-sample acceptance run: **2026-02-22**, passed.

## 6) Known Risks (Current)
- Medium: workflow routing fallback ratio in dense random graphs remains around `~0.48`; passes gate, but visual quality headroom exists.
- Low: ODBC mode still depends on external SQL availability despite fallback chain.
- Low: observability templates are delivered, but still require environment-side datasource/alert-route binding.
- Security action required: if historical commits previously contained local passwords, rotate credentials once and avoid reusing old secrets.

## 7) Recommended Handoff Sequence
1. Run checklist in section 5.
2. If using SQL mode, run migrations first:
   - `ops/scripts/db_migrate.ps1`
3. If releasing installer/bundle:
   - `ops/scripts/release_productize.ps1 -Version <x.y.z>`
4. Validate desktop startup self-check in UI before first production run.

## 8) References
- `docs/dify_desktop_app.md`
- `docs/offline_delivery_minimal.md`
- `docs/quickstart_backend.md`
- `docs/project_audit_20260218.md`
- `docs/archive/`

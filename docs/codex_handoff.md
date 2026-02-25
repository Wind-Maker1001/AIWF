# Codex Handoff (Updated: 2026-02-23)

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
    - supports rule-level engine flag: `rules.execution_engine=columnar_v1`
  - `transform_rows_v2` cache controls:
    - `GET /operators/transform_rows_v2/cache_stats`
    - `POST /operators/transform_rows_v2/cache_clear`
  - `aggregate_rows_v1`, `quality_check_v1`, `aggregate_pushdown_v1`
  - `text_preprocess_v2`
  - `plugin_exec_v1` + `plugin_health_v1`
  - extension ops v1:
    - `plugin_operator_v1`
    - `columnar_eval_v1`
    - `stream_window_v1`
    - `sketch_v1`
    - `runtime_stats_v1`
  - explain plan feedback loop:
    - `explain_plan_v1` now supports `actual_stats` + persisted feedback scaling
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
  - default graph is now standard pipeline v1 (directly runnable from GUI reset state)
  - startup self-check panel (fonts/OCR/pdftoppm/task-store diagnostics + suggestions)
  - runtime health checks, font install action, route diagnostics panel
- Privacy hardening:
  - `ops/config/dev.env` removed from git tracking; use local file + committed `ops/config/dev.env.example`
  - default egress-off gate for external AI/OTLP (`AIWF_ALLOW_EGRESS=false`)
  - CI secret scan gate (`ops/scripts/secret_scan.ps1`)
  - glue traceback debug disabled in production/release mode
  - plugin operator audit log: `tmp/plugin_audit.log`
- Office output quality:
  - desktop and glue both support replaceable layout/theme templates
  - paper-clean profile configurable for citation/reference stripping
  - desktop office outputs now enforce anti-overflow paging in PPTX evidence/warnings
  - desktop docx/xlsx/pptx support mixed insertion of table + image artifacts
  - desktop office text path includes mojibake fallback cleanup for corrupted lines

## 2.1) New Contracts and Gates
- contract file:
  - `contracts/rust/operators_extension_v1.schema.json`
- performance gate:
  - `ops/scripts/check_rust_new_ops_bench_gate.ps1`
  - wired into `ops/scripts/ci_check.ps1` (skip switch: `-SkipRustNewOpsBenchGate`)
- Dify adapter:
  - `apps/dify-desktop/dify_adapter.js`
  - normalized request/response/error mapping for bridge runtime

## 3) Decoupled Templates / Profiles
- Desktop office themes: `rules/templates/office_themes_desktop.json`
- Desktop office layouts: `rules/templates/office_layouts_desktop.json`
- Desktop cleaning template registry: `rules/templates/cleaning_templates_desktop.json`
- Desktop GUI template manager supports user-level enable/disable + import/export JSON
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
- Transform cache:
  - `AIWF_RUST_TRANSFORM_CACHE_ENABLED=true|false`
  - `AIWF_RUST_TRANSFORM_CACHE_TTL_SEC`
  - `AIWF_RUST_TRANSFORM_CACHE_MAX_ENTRIES`
- Transform engine:
  - `AIWF_RUST_TRANSFORM_ENGINE=row_v1|columnar_v1|columnar_arrow_v1|auto_v1`
  - `AIWF_RUST_ENGINE_PROFILE_PATH` (optional, default: `apps/accel-rust/conf/transform_engine_profile.json`)
- SQL:
  - `AIWF_SQL_HOST`
  - `AIWF_SQL_PORT`
  - `AIWF_SQL_DB`
  - `AIWF_SQL_USER`
  - `AIWF_SQL_PASSWORD`
  - `AIWF_SQL_TRUSTED=1|true|yes` (smoke/CI SQL verify uses Windows integrated auth `-E`)
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
7. `powershell -ExecutionPolicy Bypass -File .\ops\scripts\acceptance_desktop_finance_template.ps1`

Latest desktop real-sample acceptance run: **2026-02-22**, passed.
Latest desktop finance-template acceptance run: **2026-02-22**, passed.
Latest full CI run: **2026-02-23**, passed (`ops/scripts/ci_check.ps1`, full path including package/contract/chaos/smoke).

Recent fixes (2026-02-23):
- Fixed brittle workflow UI tests that assumed 6 default nodes; tests now assert against runtime default graph node count.
- Replaced FastAPI deprecated startup hook in `apps/dify-console` with lifespan startup initialization.
- Hardened `ops/scripts/smoke_test.ps1` SQL verification:
  - supports trusted auth fallback (`sqlcmd -E`) when SQL password is unset/placeholder
  - emits detailed `sqlcmd` error message instead of generic failure text
  - validates placeholder passwords early with actionable guidance
- Hardened CI/release quality gates (default-on):
  - `ops/scripts/ci_check.ps1` now runs SQL connectivity gate by default (`ops/scripts/check_sql_connectivity.ps1`)
  - Rust transform benchmark gate is now default-on in CI (no env-toggle needed), using strong profile:
    - `Rows=120000`, `Runs=4`, `Warmup=1`, `MinSpeedup=1.03`, `MinArrowSpeedup=0.90`, `EnforceArrowAlways`
  - Routing benchmark thresholds are tightened in CI:
    - `MAX_MS_PER_EDGE=130`
    - `MAX_WORST_SCENARIO_MS_PER_EDGE=170`
    - `MAX_RANDOM_FALLBACK_RATIO=0.50`
    - `MAX_FALLBACK_RATIO=0.50`
    - trend median thresholds: `120/160`
  - `ops/scripts/release_productize.ps1` now enforces SQL connectivity + routing gate + rust transform gate by default:
    - release routing threshold: `125/165`, fallback `0.48`, trend `115/155`
    - release rust transform defaults: `Rows=120000`, `Runs=4`, `MinSpeedup=1.03`, `MinArrowSpeedup=0.95`

Recent desktop UX upgrades (2026-02-22):
- Precheck supports clickable issue localization and sample file quick-open.
- Precheck thresholds configurable in GUI (`amount_convert_rate_min`, `max_invalid_ratio`, `min_output_rows`).
- Workflow Studio node palette/chiplet registry now includes Rust v2 node types:
  - `join_rows_v2`
  - `aggregate_rows_v2`
  - `quality_check_v2`
  - `load_rows_v2`
  - `schema_registry_v1_infer`
  - `schema_registry_v1_get`
  - `schema_registry_v1_register`

Recent rust upgrades (2026-02-22):
- `transform_rows_v2` supports in-memory result cache (ttl + max entries + hit/miss/evict metrics).
- Quality gates now support `max_filtered_rows`, `max_duplicate_rows_removed`, `allow_empty_output`.
- `transform_rows_v2` adds `columnar_v1` prototype path for cast/filter/required/dedup/sort.
- `transform_rows_v2` adds `columnar_arrow_v1` path (Arrow column vectors + `take/lexsort` kernels for cast/filter/required + shared dedup/sort columnar path).
- `transform_rows_v2` now supports expression-driven computed fields and vectorized text/date post-ops:
  - `computed_fields` (e.g. `mul($price,$qty)`, `concat(...)`, `coalesce(...)`)
  - `string_ops` (`trim/lower/upper/replace`)
  - `date_ops` (`parse_ymd/year/month/day`)
- `columnar_v1` hot path optimized:
  - cast stage now updates values in-place (reduced map insert/clone overhead)
  - dedup stage removes extra `HashSet+retain` pass
  - single-key sort uses comparator fast path
- new v2 operators landed:
  - `join_rows_v2` (multi-key + `inner/left/right/full/semi/anti`)
  - `aggregate_rows_v2` (`count_distinct/stddev/percentile_p50`)
  - `quality_check_v2` (`range_checks/dependency_checks/drift_check` + quality score)
  - `load_rows_v2` (adds `txt`; `pdf/docx/xlsx/image` metadata-mode probe rows)
  - schema registry endpoints:
    - `schema_registry_v1/register`
    - `schema_registry_v1/get`
    - `schema_registry_v1/infer`
  - workflow router now supports `schema_registry_v1_register|get|infer` nodes directly
- `transform_rows_v2_stream` now supports incremental watermark filtering:
  - `watermark_field`
  - `watermark_value`
- New metrics: `aiwf_transform_rows_v2_columnar_calls_total`, `aiwf_transform_rows_v2_columnar_success_total`.
- Observability v2 counters added:
  - `aiwf_join_rows_v2_calls_total`
  - `aiwf_aggregate_rows_v2_calls_total`
  - `aiwf_quality_check_v2_calls_total`
  - `aiwf_schema_registry_{register|get|infer}_total`
- Benchmark helper: `ops/scripts/bench_rust_transform.ps1` (row vs columnar compare).
- Benchmark gate helper: `ops/scripts/check_rust_transform_bench_gate.ps1`.
- Bench learning writes history (`ops/logs/bench/rust_transform/history.jsonl`) and can update engine profile.
- Learned profile adds auto-routing decision quality:
  - `auto_decision_hit_rate`
  - `auto_decision_hit_samples`
- Rust benchmark gate is now tiered by profile thresholds:
  - `large` payload: enforce strict speed gates (`columnar_v1` and `columnar_arrow_v1`)
  - `non-large` payload: enforce correctness/runability, skip strict speed assertions to avoid medium-size noise flakiness
- Release packaging now enforces rust transform benchmark gate by default (`ops/scripts/release_productize.ps1`).

Recent rust/de-coupling upgrades (2026-02-24):
- New operator capability negotiation endpoint:
  - `POST /operators/capabilities_v1`
  - returns operator/version/capability matrix for workflow node rendering and runtime gating.
- New I/O contract validation endpoint:
  - `POST /operators/io_contract_v1/validate`
  - validates target operator input payload before execution (strict mode supported).
- New failure classification/recovery policy endpoint:
  - `POST /operators/failure_policy_v1`
  - classifies errors (`transient_timeout/upstream_5xx/input_invalid/...`) and outputs retry/recovery action.
- New incremental planning endpoint:
  - `POST /operators/incremental_plan_v1`
  - emits deterministic fingerprint + cache-hit/checkpoint-based resume strategy.
- `transform_rows_v2_stream` now supports run-level chunk throttling:
  - request: `max_chunks_per_run`
  - response: `has_more`, `next_checkpoint`
- New explain endpoint:
  - `POST /operators/explain_plan_v2`
  - extends v1 with actual stage stats and optional runtime-stats embedding.
- Desktop Workflow chiplet registry + GUI palette/forms wired for:
  - `capabilities_v1`, `io_contract_v1`, `failure_policy_v1`, `incremental_plan_v1`, `explain_plan_v2`.
- Additional rust capability expansion (2026-02-25):
  - multi-tenant isolation policy endpoint:
    - `POST /operators/tenant_isolation_v1`
  - tenant operator allow/deny policy endpoint:
    - `POST /operators/operator_policy_v1`
    - `run_workflow` now enforces tenant operator policy before executing each step.
  - unified error code normalization in workflow runtime:
    - runtime stats error code now uses normalized classes (`QUOTA_REJECT/TIMEOUT/POLICY_BLOCKED/INPUT_INVALID/...`).
  - adaptive optimizer endpoint:
    - `POST /operators/optimizer_adaptive_v2`
  - vector index v2 endpoints:
    - `POST /operators/vector_index_v2/build`
    - `POST /operators/vector_index_v2/search`
    - `POST /operators/vector_index_v2/eval`
    - search supports shard/filter/incremental append or replace, plus optional metadata rerank (`rerank_meta_field`, `rerank_meta_weight`).
    - eval outputs `recall_at_k` and `mrr` based on labeled cases.
  - stream reliability endpoint:
    - `POST /operators/stream_reliability_v1`
    - supports dedup/checkpoint/dlq flush/replay/consistency_check/stats.
  - lineage + provenance fusion endpoint:
    - `POST /operators/lineage_provenance_v1`
  - contract regression case generation endpoint:
    - `POST /operators/contract_regression_v1`
  - performance baseline endpoint:
    - `POST /operators/perf_baseline_v1` (`get/set/check`)
  - desktop node palette/chiplets/forms wired for:
    - `tenant_isolation_v1`, `operator_policy_v1`, `optimizer_adaptive_v2`,
      `vector_index_v2_build`, `vector_index_v2_search`,
      `vector_index_v2_eval`,
      `stream_reliability_v1`, `lineage_provenance_v1`,
      `contract_regression_v1`, `perf_baseline_v1`.
  - release/runtime scripts:
    - `ops/scripts/release_rust_binary.ps1`
    - `ops/scripts/package_rust_offline_bundle.ps1`
    - `ops/scripts/check_rust_operator_perf_gate.ps1`
  - desktop release baseline script:
    - `ops/scripts/release_baseline_v1_1_4.ps1`
    - outputs clean-windows checklist + baseline summary under `release/v1.1.4/`.
  - markdown quality gate productization:
    - quality report now includes extraction/gibberish/reference-prune/section-integrity metrics.
    - automatic fallback to `text_fidelity` mode when quality gate blocks / output empty / gibberish ratio too high.
  - workflow observability upgrades:
    - new IPC: `aiwf:getWorkflowPerfDashboard` (error rate / p95 / retry rate / fallback rate).
    - workflow studio diagnostics panel upgraded to display these metrics.
    - workflow studio now shows offline capability boundary hints (local-only vs online-required nodes).

## 6) Known Risks (Current)
- Medium: workflow routing fallback ratio in dense random graphs remains around `~0.48`; now under stricter gate, but visual quality headroom still exists.
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




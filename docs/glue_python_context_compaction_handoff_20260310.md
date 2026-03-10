# Glue Python Detailed Handoff / Context Compaction Pickup (2026-03-10)

## 1. Purpose
- This document is the **primary pickup file** for any continuation of the `glue-python` workstream after context compaction.
- It records:
  - the original problem,
  - what was changed,
  - what was validated,
  - what is already pushed,
  - what is still only in the local working tree,
  - and the safest next actions.

---

## 2. Original Problem
- Main issue originally under review:
  - `apps/glue-python/aiwf/preprocess_pipeline.py`
- Defect:
  - `run_preprocess_pipeline_impl()` always wrote the final output through `preprocess_file(..., {"output_format": "csv"})`
  - so `final_output_path="final.jsonl"` still produced CSV content.
- Companion risk:
  - relative `final_output_path` such as `..\outside.csv` could escape `job_root`.

This initial bug led to a broader stabilization / modularization / contract-tightening effort around `glue-python`.

---

## 3. Scope Rules Used During This Workstream
- User explicitly asked that I focus on the **Python module** first.
- In practice, intentional code changes in this workstream were limited to:
  - `apps/glue-python`
  - root `pytest.ini` earlier when repo-level Python test isolation was needed
  - later, coordinated Java contract work was validated once the Java-side agent finished
  - `docs/glue_python_context_compaction_handoff_20260310.md`
- Important rule followed throughout:
  - never intentionally mix Python commits with Rust / Java / Desktop files
  - always use explicit `git add -- <paths>`
- Important note:
  - this repo has often been **globally dirty** outside Python
  - broad staging commands are unsafe

---

## 4. Current Branch / Repo State

### Active branch
- `codex/rust-module-review-20260309`

### Current tracking state
- `git status -sb` currently shows branch tracking:
  - `origin/codex/rust-module-review-20260309`
- After the 2026-03-10 cleanup and contract-freeze pass, this branch also contains these additional local commits:
  - `27b683b` `refactor(desktop): modularize shell and workflow canvas`
  - `bbe1b56` `refactor(base-java): strip legacy glue path params`
  - `a970919` `refactor(glue-python): sanitize attached flow context params`
  - `29e7508` `feat(glue-python): add strict job context mode`
  - `14a0c7d` `chore(ops): enable strict job context rollout`

### Current local working tree status relevant to Python
At the time of this handoff refresh, the previously local-only Python follow-up has already been landed locally across:
- `apps/glue-python/aiwf/accel_client.py`
- `apps/glue-python/aiwf/flow_context.py`
- `apps/glue-python/app.py`
- `apps/glue-python/tests/test_app.py`
- `apps/glue-python/tests/test_paths.py`

The working tree is currently clean.

### Non-Python dirty files
- The worktree is also dirty in many non-Python areas (Desktop mainly).
- Do **not** sweep them into a Python commit.

---

## 5. High-Level Outcome So Far

### 5.1 Python-side result
`glue-python` is now in a substantially healthier state:
- original pipeline output-format bug fixed
- path escape protections in place
- `preprocess` modularized
- `cleaning` modularized
- `glue-python -> accel-rust` boundary tightened
- route-level contract / E2E-style tests added
- explicit `job_context` accepted from Java
- real multi-service live integration run completed

### 5.2 Java-side result
The Java contract work was later landed in the same branch and validated:
- Java now explicitly sends:
  - `job_id`
  - `flow`
  - `actor`
  - `ruleset_version`
  - `trace_id`
  - `job_context`
  - `params`
- `job_context` includes:
  - `job_root`
  - `stage_dir`
  - `artifacts_dir`
  - `evidence_dir`
- Java still keeps `params.job_root` as compatibility fallback during migration.

### 5.3 Cross-service state
The contract migration is now in **phase 2 rollout-active** state:
- Java can emit explicit `job_context`
- Python can consume it and prefers it over `params.job_root`
- Java no longer re-injects legacy path fields into Glue `params`
- Python sanitizes attached flow params before dispatch
- Python still retains legacy fallback internally when strict mode is disabled
- strict mode is now available and enabled by default in the Glue startup script

---

## 6. Key Python Commits Already Landed

Below are the most relevant Python-side commits already committed and pushed during this workstream:

### Initial hardening / expansion
- `5b1d786` `fix: harden python service modules`
- `28fb4d0` `feat: expand glue-python extension and preprocess support`

### Preprocess / cleaning modularization
- `bfbf105` `refactor: split glue-python preprocess modules`
- `fadc0b1` `refactor: modularize glue-python flow helpers`
- `5f2c8f7` `refactor: split glue-python cleaning core helpers`

### Contract / E2E coverage
- `444b123` `test: add glue-python route contract coverage`

### Accel boundary tightening
- `31e44f4` `refactor: add glue-python accel client`
- `490d0ac` `refactor: share glue-python accel transport`
- `5e8ea0b` `refactor: model glue-python accel responses`

### Java-Python contract migration support
- `726b863` `feat: accept explicit job_context in glue-python`
- `e2d2b6b` `test: cover glue-python job_context route contract`

### Legacy fallback observability
- `1c68c84` `chore: warn on legacy glue-python path fallbacks`

### Contract-freeze follow-up
- `bbe1b56` `refactor(base-java): strip legacy glue path params`
- `a970919` `refactor(glue-python): sanitize attached flow context params`
- `29e7508` `feat(glue-python): add strict job context mode`
- `14a0c7d` `chore(ops): enable strict job context rollout`

### Handoff doc
- `449c430` `docs: add glue-python compaction handoff`

### Java contract commit (already pushed in this branch)
- `113af9f` `feat(base-java): tighten glue flow contracts`

---

## 7. Python Module Structure After Refactor

### Core
- `apps/glue-python/app.py`
- `apps/glue-python/aiwf/config.py`
- `apps/glue-python/aiwf/paths.py`
- `apps/glue-python/aiwf/flow_context.py`

### Preprocess stack
- `apps/glue-python/aiwf/preprocess.py`
- `apps/glue-python/aiwf/preprocess_cli.py`
- `apps/glue-python/aiwf/preprocess_service.py`
- `apps/glue-python/aiwf/preprocess_stages.py`
- `apps/glue-python/aiwf/preprocess_runtime.py`
- `apps/glue-python/aiwf/preprocess_ops.py`
- `apps/glue-python/aiwf/preprocess_registry.py`
- `apps/glue-python/aiwf/preprocess_evidence.py`
- `apps/glue-python/aiwf/preprocess_reporting.py`
- `apps/glue-python/aiwf/preprocess_validation.py`
- `apps/glue-python/aiwf/preprocess_conflicts.py`
- `apps/glue-python/aiwf/preprocess_io.py`
- `apps/glue-python/aiwf/preprocess_pipeline.py`

### Cleaning stack
- `apps/glue-python/aiwf/flows/cleaning.py`
- `apps/glue-python/aiwf/flows/cleaning_inputs.py`
- `apps/glue-python/aiwf/flows/cleaning_outputs.py`
- `apps/glue-python/aiwf/flows/cleaning_profile.py`
- `apps/glue-python/aiwf/flows/cleaning_quality.py`
- `apps/glue-python/aiwf/flows/cleaning_config.py`
- `apps/glue-python/aiwf/flows/cleaning_transport.py`
- `apps/glue-python/aiwf/flows/cleaning_flow_helpers.py`
- `apps/glue-python/aiwf/flows/cleaning_orchestrator.py`
- `apps/glue-python/aiwf/flows/cleaning_simple_rules.py`
- `apps/glue-python/aiwf/flows/cleaning_generic_rules.py`
- `apps/glue-python/aiwf/flows/cleaning_artifacts.py`
- `apps/glue-python/aiwf/flows/office_artifacts.py`
- `apps/glue-python/aiwf/flows/artifact_selection.py`
- `apps/glue-python/aiwf/flows/registry.py`

### Accel boundary
- `apps/glue-python/aiwf/accel_client.py`
- `apps/glue-python/aiwf/accel_transport.py`
- `apps/glue-python/aiwf/rust_client.py`

### Extension / registry support
- `apps/glue-python/aiwf/extensions.py`
- `apps/glue-python/aiwf/capabilities.py`
- `apps/glue-python/aiwf/registry_events.py`
- `apps/glue-python/aiwf/registry_policy.py`
- `apps/glue-python/aiwf/registry_utils.py`
- `apps/glue-python/aiwf/sample_extension.py`

---

## 8. What Was Fixed

### 8.1 Original pipeline output-format defect
- Final pipeline output now respects target output format rather than being forced to CSV.

### 8.2 Path safety
- Hardened:
  - `job_root`
  - `final_output_path`
  - canonical bundle output path
  - cleaning input/output path resolution

### 8.3 Compatibility retry behavior
- Replaced blind `TypeError`-driven compatibility fallback with signature-aware invocation logic in Python runtime call wrappers.

### 8.4 Boundary tightening toward accel-rust
- Introduced a dedicated accel client
- Introduced shared accel transport
- Added explicit request / response models for the primary operator calls

### 8.5 Java contract intake
- Python now accepts and prefers explicit `job_context`
- `trace_id` is also carried through request normalization

### 8.6 Contract-freeze hardening
- Java no longer forwards transport-owned path/context fields inside Glue `params`
- Python strips duplicate `job_context/trace_id/stage_dir/artifacts_dir/evidence_dir` entries before runner dispatch
- Python can now reject legacy `params.job_root` / `params.stage_dir` / `params.artifacts_dir` / `params.evidence_dir` inputs when strict mode is enabled
- Ops rollout now defaults Glue startup to strict job-context mode unless explicitly overridden

---

## 9. Validation Performed

### 9.1 Repeated local Python validation
Repeatedly executed during this workstream:
- `python -m pytest -q` in `apps/glue-python`
- `python -m unittest discover -s tests -v` in `apps/glue-python`
- `python -m compileall -q apps/glue-python`

### 9.2 Latest known Python result
- `apps/glue-python` pytest:
  - `112 passed`
- App-specific contract tests:
  - `tests/test_app.py` passing
- HTTP client / accel client tests:
  - passing
- compileall:
  - passing

### 9.3 Real multi-service integration validation completed
Performed from the current working tree:
- `ops/scripts/restart_services.ps1`
- `ops/scripts/smoke_test.ps1`
- `ops/scripts/run_regression_quality.ps1`
- `ops/scripts/test_invalid_parquet_fallback.ps1`

Result:
- PASS
- Verified:
  - base / glue / accel health
  - job creation
  - cleaning flow run
  - SQL persistence
  - office artifact quality
  - strict job-context startup rollout
  - regression quality script using explicit `job_context`
  - invalid parquet fallback path

Note:
- one earlier chained run of `smoke_test.ps1 -WithInvalidParquetFallbackTest` saw a transient base-health connectivity failure during the fallback sub-step
- re-running `test_invalid_parquet_fallback.ps1` directly from the same service state passed cleanly
- current evidence points to a timing / transient connectivity issue, not a strict job-context regression

### 9.4 Explicit `job_context` live priority validation
A live request was sent directly to `glue-python` with conflicting values:
- `job_context.job_root = <ctx_root>`
- `params.job_root = <legacy_root>`

Observed result:
- output artifacts were written under `<ctx_root>`
- not under `<legacy_root>`

Therefore, live behavior is confirmed as:
1. `job_context.*` preferred
2. `params.job_root` fallback only

---

## 10. Java-Python Contract State

### Current agreed request structure
The Java side now sends a stable flow request containing:
- `job_id`
- `flow`
- `actor`
- `ruleset_version`
- `trace_id`
- `job_context`
- `params`

### Current `job_context`
- `job_root`
- `stage_dir`
- `artifacts_dir`
- `evidence_dir`

### Python-side current precedence
- `job_context.*`
- then `params.job_root`
- then local fallback / default path inference

### Migration phase
- Phase 1 complete:
  - Java emits explicit `job_context`
  - Python consumes and prefers it
- Phase 2 rollout active:
  - Java transport no longer emits legacy path fields in `params`
  - Glue startup defaults to strict mode via `ops/scripts/run_glue_python.ps1`
  - `ops/config/dev.env.example` now documents `AIWF_STRICT_JOB_CONTEXT=true`
- Fallback retirement is not yet globally complete:
  - `params.job_root` is still retained in Python compatibility logic when strict mode is disabled

---

## 11. Latest Local Python Follow-Up

The latest locally landed Python follow-up spans:
- `apps/glue-python/aiwf/accel_client.py`
- `apps/glue-python/aiwf/flow_context.py`
- `apps/glue-python/app.py`
- `apps/glue-python/tests/test_app.py`
- `apps/glue-python/tests/test_paths.py`

### What that landed follow-up does
- Prefer `params.job_context.job_root` when building accel cleaning operator requests
- Stop re-flattening `stage_dir`, `artifacts_dir`, and `evidence_dir` back into top-level `params`
- Extend route-level contract coverage so those legacy top-level path fields stay absent
- Add a strict-mode gate that returns `400` for legacy path params when `AIWF_STRICT_JOB_CONTEXT` is enabled

### Validation status
- `tests/test_app.py`: passing
- `apps/glue-python` full pytest: passing
- `python -m compileall -q apps/glue-python`: passing

### Push status
- committed locally
- not pushed yet

## 12. Safe Next Actions

### If continuing Python work
1. Keep scope limited to `apps/glue-python`
2. Commit only explicit Python paths
3. Avoid broad staging due to dirty non-Python tree
4. Continue the migration in this order:
   - keep `job_context.*` primary
   - run strict mode in real environments
   - observe whether any external caller still depends on legacy path params
   - only remove compatibility fallback after repeated green live runs and caller cleanup

### If freezing Python for now
- Also valid.
- The Python module is already in a stable, maintainable state.

### If continuing Java-Python contract tightening
- The safest next step is **not** more broad Python restructuring.
- It is:
  - continue Java/Python smoke using explicit `job_context`
  - keep strict mode enabled in dev / ops rollout
  - then eventually remove `params.job_root` fallback after confidence is high enough

---

## 13. Do / Don't for Next Agent

### Do
- Read:
  - `apps/glue-python/app.py`
  - `apps/glue-python/aiwf/flow_context.py`
  - `apps/glue-python/aiwf/paths.py`
  - `apps/glue-python/aiwf/accel_client.py`
  - `apps/glue-python/aiwf/accel_transport.py`
  - `apps/glue-python/aiwf/flows/cleaning_flow_helpers.py`
- Use explicit `git add -- apps/glue-python/...`
- Re-run:
  - `python -m pytest -q` in `apps/glue-python`
  - `python -m compileall -q apps/glue-python`

### Don't
- Do not commit dirty Desktop / Rust / Java files unintentionally
- Do not remove `params.job_root` fallback yet unless explicitly asked and after more than one clean live validation cycle
- Do not treat the locally committed follow-up as already published

---

## 14. One-Sentence Pickup Summary
- `glue-python` has already fixed the original pipeline output/path bug, been modularized heavily, now accepts explicit Java `job_context`, runs with a stricter transport contract, has strict-mode enforcement available in the runtime path, and has been live-validated across services; the main remaining work is eventual full retirement of the compatibility fallback when confidence is high enough.

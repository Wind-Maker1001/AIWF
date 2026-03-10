# Glue Python Context Compaction Handoff (2026-03-10)

## Purpose
- This file is the pickup point for any post-compaction continuation of the `glue-python` workstream.
- It records the workflow completed so far, the current branch/repo state, what was fixed, what was refactored, what was validated, and the safest next actions.

## Scope
- User explicitly requested that only the Python module be touched.
- In practice, all intentional code changes in this workstream were limited to:
  - `apps/glue-python`
  - root-level `pytest.ini` when needed earlier for repo-level Python test isolation
- Avoid touching Rust / Java / desktop files when continuing this thread unless the user explicitly changes scope.

## Original Problem Statement
- Main bug originally under review:
  - `apps/glue-python/aiwf/preprocess_pipeline.py`
  - Final pipeline export always forced `output_format='csv'`, even when `final_output_path` was something like `final.jsonl`.
  - Relative `final_output_path` could also escape `job_root`.

## Current High-Level Status
- `glue-python` is now in a **stable, modularized, and regression-covered** state.
- The original pipeline output-format bug is fixed.
- Path-boundary handling for `job_root`, `final_output_path`, and canonical bundle output has been tightened.
- `preprocess` and `cleaning` were split into smaller helper modules with preserved top-level entrypoints.
- `glue-python -> accel-rust` has been tightened via:
  - a dedicated accel client
  - shared accel transport
  - explicit request payload models for the main operator calls
- Route-level contract / E2E-style tests were added to cover `app -> cleaning flow`.

## Important Branch / Repo State

### Current branch
- Active branch:
  - `codex/rust-module-review-20260309`

### Remote tracking status at time of writing
- `git status -sb` showed:
  - current branch is **ahead of** `origin/codex/rust-module-review-20260309` by `2` commits
- These two local-only Python commits are:
  - `31e44f4` `refactor: add glue-python accel client`
  - `490d0ac` `refactor: share glue-python accel transport`

### Important caution
- The overall repo working tree is dirty in **non-Python** areas (Rust / Java / desktop).
- Do **not** run broad `git add .` or `git commit -a`.
- If continuing Python-only work, always stage explicit paths under `apps/glue-python`.

## Python-Only Commit Timeline

### 1. Baseline hardening
- `5b1d786` `fix: harden python service modules`
  - Fixed runtime error handling and compatibility issues.

### 2. Feature / extension / preprocess expansion
- `28fb4d0` `feat: expand glue-python extension and preprocess support`
  - Added extension, registry, artifact-selection, and preprocess support infrastructure.

### 3. Preprocess modularization
- `bfbf105` `refactor: split glue-python preprocess modules`
  - Split preprocess responsibilities into dedicated modules.

### 4. Glue flow helper modularization
- `fadc0b1` `refactor: modularize glue-python flow helpers`
  - Further split helper logic around flow orchestration.

### 5. Cleaning core modularization
- `5f2c8f7` `refactor: split glue-python cleaning core helpers`
  - Split cleaning core helpers into smaller modules.

### 6. Route contract coverage
- `444b123` `test: add glue-python route contract coverage`
  - Added route-level contract / E2E-style tests for `app -> cleaning flow`.

### 7. Accel client layer
- `31e44f4` `refactor: add glue-python accel client`
  - Added a dedicated accel client and switched main cleaning operator calls to use it.
  - **Local commit; not yet pushed when this handoff was written.**

### 8. Shared accel transport
- `490d0ac` `refactor: share glue-python accel transport`
  - Extracted shared transport logic used by `accel_client.py` and `rust_client.py`.
  - **Local commit; not yet pushed when this handoff was written.**

## Key Fixes Completed

### A. Original pipeline export bug fixed
- Final output no longer forces CSV when target path implies JSON / JSONL.
- Path escaping for pipeline final output was also blocked.

### B. Path boundary tightening
- Centralized path resolution under:
  - `apps/glue-python/aiwf/paths.py`
- Applied to:
  - `job_root`
  - preprocess final output
  - canonical bundle directory
  - cleaning input/output path resolution

### C. Runtime compatibility tightening
- `app.py` compatibility invocation switched from blind `TypeError` retries to signature-based matching.
- This prevented internal runtime `TypeError` from being incorrectly swallowed as a compatibility fallback.

### D. Registry / extension / artifact improvements
- Extension loading / reload behavior improved.
- Registry conflict behavior became clearer.
- Artifact handling and selection logic were expanded and stabilized.

### E. Contract / E2E coverage
- Added route-level tests that exercise actual flow execution through the HTTP app boundary.

## Current Module Map

### Core app / config / paths
- `apps/glue-python/app.py`
- `apps/glue-python/aiwf/config.py`
- `apps/glue-python/aiwf/paths.py`

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

### Boundary tightening toward accel-rust
- `apps/glue-python/aiwf/accel_client.py`
- `apps/glue-python/aiwf/accel_transport.py`
- `apps/glue-python/aiwf/rust_client.py`

## Validation Performed

### Repeatedly executed during this workstream
- `python -m pytest -q` in `apps/glue-python`
- `python -m unittest discover -s tests -v` in `apps/glue-python`
- `python -m compileall -q apps/glue-python`

### Latest known results before handoff
- `apps/glue-python` pytest:
  - `103 passed`
- `tests/test_http_clients.py`:
  - `9 passed`
- `tests/test_cleaning_flow.py + tests/test_app.py + tests/test_preprocess.py` combinations:
  - passing in latest runs
- compileall:
  - passing

### Post-contract live integration validation (2026-03-10)
- Real backend services were restarted from the current working tree using:
  - `ops/scripts/restart_services.ps1`
- Real end-to-end smoke was executed using:
  - `ops/scripts/smoke_test.ps1`
- Result:
  - PASS
  - base / glue / accel health checks passed
  - job creation passed
  - cleaning flow run passed
  - SQL persistence verification passed
  - office artifact quality gate passed

### Explicit `job_context` priority live check
- After Java-side contract updates, a live request was sent directly to `glue-python` with:
  - `job_context.job_root = <ctx_root>`
  - `params.job_root = <legacy_root>`
- The resulting artifact paths were written under:
  - `<ctx_root>\stage-jc\...`
- They were **not** written under:
  - `<legacy_root>\...`
- Therefore the current Python behavior is confirmed as:
  1. `job_context.*` preferred
  2. `params.job_root` fallback only

### Example live result summary
- `run_ok = true`
- `uses_job_context = true`
- `uses_legacy = false`
- representative artifact path:
  - `D:\AIWF\bus\jobs\<job>-ctx\stage-jc\cleaned.parquet`

## Current Python Working Tree Status
- At handoff time, `apps/glue-python` working tree is clean.
- The only local-only Python work not yet pushed is already committed:
  - `31e44f4`
  - `490d0ac`

## Recommended Next Actions

### If the goal is to continue tightening boundaries
1. **Push local Python accel commits**
   - Push:
     - `31e44f4`
     - `490d0ac`
2. **Continue explicit accel contract work**
   - Add explicit response models for:
     - cleaning operator response
     - `transform_rows_v2` response
   - Goal: reduce remaining dict-shape guessing.
3. **Then tighten `base-java -> glue-python`**
   - Move toward a stable flow request contract so `glue-python` does less control-plane inference.

### If Java-side `job_context` integration is under review
- The first compatibility phase is already live-validated.
- The next safe step is:
  - keep `params.job_root` fallback in place temporarily
  - continue integration runs with Java emitting `job_context`
  - only remove the fallback after repeated green smoke runs

### If the goal is to freeze the Python module
- This is a valid stopping point.
- The Python module is already in a much healthier state than when this thread began.

## Safe Rules for Next Agent
- Stay inside `apps/glue-python` unless the user explicitly broadens scope.
- Use explicit `git add -- <python-paths>` only.
- Do not accidentally commit dirty Rust / Java / desktop files from this worktree.
- Read the following first before continuing:
  - `apps/glue-python/app.py`
  - `apps/glue-python/aiwf/paths.py`
  - `apps/glue-python/aiwf/preprocess.py`
  - `apps/glue-python/aiwf/flows/cleaning.py`
  - `apps/glue-python/aiwf/accel_client.py`
  - `apps/glue-python/aiwf/accel_transport.py`

## One-Sentence Pickup Summary
- The `glue-python` workstream has already fixed the original pipeline output/path bug, modularized preprocess/cleaning substantially, added route-level contract coverage, and started a proper `glue-python -> accel-rust` client/transport boundary; the cleanest next step is to finish explicit accel response contracts and then tighten `base-java -> glue-python`.

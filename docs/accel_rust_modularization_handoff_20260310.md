# Accel Rust Modularization Handoff (2026-03-10)

## Scope
This handoff captures the ongoing `apps/accel-rust` modularization work so another agent can resume after context compaction without re-discovering the current state.

## Current Git State
- Workspace: `D:\AIWF`
- Active branch: `codex/rust-module-review-20260309`
- Current local `HEAD`: `490d0ac0bf957c0ceaf6a008e3ef54da4a14c693`
- Latest pushed accel-rust modularization commit on remote branch: `f2b5398` (`refactor: split advanced rust handler modules`)

## Current Working Tree (uncommitted)
These files are currently modified or newly added in the working tree:
- Modified:
  - `apps/accel-rust/src/http/routes.rs`
  - `apps/accel-rust/src/operators/workflow.rs`
  - `apps/accel-rust/src/operators/workflow/engine.rs`
  - `apps/accel-rust/src/operators/workflow/runner.rs`
  - `apps/accel-rust/src/platform_ops/intelligence.rs`
  - `apps/accel-rust/src/transform_support/control.rs`
- Untracked:
  - `apps/accel-rust/src/operators/workflow/custom.rs`
  - `apps/accel-rust/src/operators/workflow/support.rs`
  - `apps/accel-rust/src/platform_ops/intelligence/`
  - `apps/accel-rust/src/transform_support/control/`

## Validation Status
The current working tree (including the uncommitted changes above) has been validated successfully with:
- `cargo fmt`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`

Latest observed result:
- tests: `53 passed, 1 ignored`
- clippy: clean

## High-Level Goal
The user asked to keep Rust modules maintainable and ensure no single `.rs` file exceeds 2000 lines. We have been proactively decomposing large multi-responsibility files into submodules, while preserving behavior and repeatedly validating after each step.

## Major Completed Refactors Already Landed / Pushed
These are already part of the branch history before the current uncommitted work:
- restore and wire modular `accel-rust` layout
- explicit import cleanup (removing remaining `use crate::*` patterns)
- split `operators/workflow.rs`
- split `execution_ops/storage.rs`
- split `task_store/mod.rs`
- split `plugin_runtime.rs`
- split `row_io.rs`
- split `analysis_ops.rs`
- split `cleaning_runtime.rs`
- split `operators/analytics/aggregate.rs`
- split `operators/analytics/quality.rs`
- split `transform_support/columnar.rs`
- split `platform_ops/streaming.rs`
- split `misc_ops.rs`
- split `api_types/runtime.rs`
- split `http/handlers_core/transform.rs`
- split `http/handlers_extended/classic.rs`
- split `http/handlers_extended/advanced.rs`
- split `governance_ops/reliability.rs`

## Current In-Progress Refactors (uncommitted)
### 1) `operators/workflow/engine.rs`
Intent:
- split generic helper/support logic from the macro-driven registry and custom handlers

Current state:
- added `apps/accel-rust/src/operators/workflow/support.rs`
- added `apps/accel-rust/src/operators/workflow/custom.rs`
- updated `apps/accel-rust/src/operators/workflow/engine.rs`
- updated `apps/accel-rust/src/operators/workflow/runner.rs`
- updated `apps/accel-rust/src/operators/workflow.rs` to include `mod support; mod custom; mod engine; mod runner;`

Important note:
- this split is validated and working
- the macro-driven registry still lives in `engine.rs`
- support functions now include workflow error building, runtime stat recording, trace replay helpers, and generic deserialize/serialize wrappers
- custom functions now include the bespoke non-macro handlers:
  - `workflow_transform_rows_v2_handler`
  - `workflow_plugin_health_v1_handler`
  - `workflow_schema_registry_v2_register_handler`
  - `workflow_schema_registry_v2_get_handler`
  - `workflow_schema_registry_v2_infer_handler`

### 2) `platform_ops/intelligence.rs`
Intent:
- separate vector search, evidence/fact reasoning, finance/anomaly logic, and template/provenance helpers

Current state:
- added directory `apps/accel-rust/src/platform_ops/intelligence/`
- split into:
  - `vector.rs`
  - `reasoning.rs`
  - `finance.rs`
  - `template.rs`
- parent file `apps/accel-rust/src/platform_ops/intelligence.rs` now acts as a thin re-export/wiring module

Important note:
- `run_anomaly_explain_v1` belongs to `finance.rs`, not `reasoning.rs`
- `reasoning.rs` depends on `vector::tokenize_text`
- current split is validated and working

### 3) `transform_support/control.rs`
Intent:
- separate common text/status helpers, tenant policy/quota logic, trace/cancel helpers, and SQL validation

Current state:
- added directory `apps/accel-rust/src/transform_support/control/`
- split into:
  - `common.rs`
  - `tenant.rs`
  - `trace.rs`
  - `sql.rs`
- parent file `apps/accel-rust/src/transform_support/control.rs` now re-exports the public subset actually used by the rest of the crate

Important note:
- during this split I intentionally kept only the externally used tenant helpers re-exported from the parent module
- `common.rs` and `sql.rs` no longer need `use super::*;`-heavy behavior beyond shared imports; current version is validated and working

## Attempted Refactor That Was Reverted
### `http/routes.rs`
I attempted to split route registration into helper functions like `mount_core_routes`, `mount_classic_routes`, etc.

Result:
- reverted / backed away from the helper-function approach because `axum::Router<S>` state generics made the refactor brittle and easy to get subtly wrong
- final state left in working tree is validated and stable, but conceptually this file is best left as a single route chain unless someone wants to spend more effort designing a robust typed-router abstraction

Practical guidance:
- do **not** resume the route-helper split casually
- if revisiting it later, treat it as a separate higher-risk refactor

## Current Largest Production Files
At the moment, the largest non-test Rust files are roughly:
- `apps/accel-rust/src/operators/transform/v2/runner.rs` ！ 388 lines
- `apps/accel-rust/src/http/routes.rs` ！ 381 lines
- `apps/accel-rust/src/operators/workflow/engine.rs` ！ 373 lines
- `apps/accel-rust/src/transform_support/control.rs` ！ 369 lines
- `apps/accel-rust/src/execution_ops/storage/parquet.rs` ！ 360 lines
- `apps/accel-rust/src/operators/join.rs` ！ 355 lines
- `apps/accel-rust/src/row_io/parquet.rs` ！ 353 lines
- `apps/accel-rust/src/main.rs` ！ 347 lines

All are still far below the 2000-line user constraint.

## Suggested Next Steps After Compaction
Recommended next targets, in order:
1. Commit and push the current uncommitted batch (workflow support/custom, intelligence split, control split)
2. If more decomposition is desired, prefer one of:
   - `apps/accel-rust/src/operators/transform/v2/runner.rs`
   - `apps/accel-rust/src/operators/join.rs`
   - `apps/accel-rust/src/execution_ops/storage/parquet.rs`
3. Avoid re-opening the `http/routes.rs` split unless explicitly requested

## Good Resume Commands
Useful commands to resume quickly:
- `git status --short -- apps/accel-rust`
- `cargo test --all-targets`
- `cargo clippy --all-targets -- -D warnings`
- file size scan:
  - `Get-ChildItem apps/accel-rust/src -Recurse -Filter *.rs | % { $lines=(Get-Content $_.FullName | Measure-Object -Line).Lines; [PSCustomObject]@{Lines=$lines; Path=$_.FullName} } | Sort-Object Lines -Descending | Select-Object -First 15`

## Notes for the Next Agent
- Prefer mechanical, low-risk splits that preserve public function names and existing call sites.
- After each split, run the full validation loop immediately.
- When a parent module is loaded via `#[path = ...]`, remember child module resolution can be tricky; use `#[path = ...]` explicitly when needed.
- When a split introduces type inference noise in tests, first check whether module re-exports changed; several earlier `E0282` bursts were just cascading fallout from broken imports/exports.

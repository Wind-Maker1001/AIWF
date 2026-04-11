# Node Config Contract Authority Update (2026-03-24)

This note records the schema-authority change landed during the March 2026 architecture follow-up cycle.

## Current state

- `contracts/desktop/node_config_contracts.v1.json` is now the canonical authority for the current desktop schema-covered node-config surface.
- The contract currently covers 30 schema-covered node types consumed by desktop workflow authoring and runtime validation.
- `apps/accel-rust/src/governance_ops/contracts/workflow_contract.rs` is now the only executable consumer that turns this contract into workflow/node-config validation semantics on the main path.

## What no longer lives in JS

- Desktop JS no longer owns node-config validator execution on the main workflow save/publish/run paths.
- Desktop workflow helpers only keep UI formatting, migration hints, and editor-facing convenience behavior.

## Gate impact

- `ops/scripts/check_node_config_schema_coverage.ps1` now treats `contracts/desktop/node_config_contracts.v1.json` as the authority and checks Rust authoritative consumption plus glue authoritative-validation routing against it.
- `ops/scripts/ci_check.ps1` surfaces Rust authority drift, generated-helper retirement drift, and nested coverage drift through the architecture scorecard.

## Remaining boundary

This does not mean all workflow schema authority is solved:

- top-level workflow shape is still governed by `contracts/workflow/*.json`
- node-config rule authority for the current schema-covered surface is now governed by `contracts/desktop/node_config_contracts.v1.json`
- Rust operator metadata authority remains in Rust + exported manifest, not in this node-config contract file

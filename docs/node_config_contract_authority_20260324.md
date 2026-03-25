# Node Config Contract Authority Update (2026-03-24)

This note records the schema-authority change landed after `docs/architecture_followup_review_20260324.md`.

## Current state

- `contracts/desktop/node_config_contracts.v1.json` is now the canonical authority for the current desktop schema-covered node-config surface.
- The contract currently covers 30 schema-covered node types consumed by desktop workflow authoring and runtime validation.
- `apps/dify-desktop/workflow_contract.js` and `apps/dify-desktop/renderer/workflow/workflow-contract.js` now derive `NODE_CONFIG_SCHEMA_IDS` and `NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE` from generated contract modules instead of maintaining a split contract/non-contract type map.

## What still lives in JS

- validator-kind execution still lives in JS helpers
- helper functions such as enum, array, nested object, aggregate definition, window function, and conditional rule evaluation still execute in JS

That is an implementation concern, not rule ownership.
Per-node rule authorship for the current schema-covered surface has moved to `contracts/desktop/node_config_contracts.v1.json`.

## Gate impact

- `ops/scripts/export_node_config_contracts.ps1` regenerates the CJS and ESM contract modules consumed by desktop.
- `ops/scripts/check_node_config_schema_coverage.ps1` now treats `contracts/desktop/node_config_contracts.v1.json` as the coverage authority and checks generated modules plus desktop catalog alignment against it.
- `ops/scripts/ci_check.ps1` surfaces contract module drift and nested coverage drift through the architecture scorecard.

## Remaining boundary

This does not mean all workflow schema authority is solved:

- top-level workflow shape is still governed by `contracts/workflow/*.json`
- node-config rule authority for the current desktop schema-covered surface is now governed by `contracts/desktop/node_config_contracts.v1.json`
- Rust operator metadata authority remains in Rust + exported manifest, not in this node-config contract file

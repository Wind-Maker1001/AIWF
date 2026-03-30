# AIWF Project Review (2026-03-25)

## Scope

This review reassesses the repository under `D:\AIWF` after the convergence work around governance control-plane boundaries, node-config contract assets, runtime parity gates, and frontend primary-path decisions.

This is not a generic status note.
It is a candid project-level architecture review focused on:

- real authority boundaries
- cross-runtime truth ownership
- generated artifact discipline
- gate growth
- frontend convergence realism
- the remaining mismatch between governance infrastructure and semantic ownership

说明：

- This review was reconciled on 2026-03-28 against the current checked-out repository state
- It intentionally prefers current implementation truth over intermediate aspirational wording

## Validation Performed

Targeted validation used in this reassessment included:

```powershell
cd D:\AIWF\apps\dify-desktop
node --test `
  tests-node/governance_control_plane_boundary_gate.test.js `
  tests-node/node_config_schema_gate.test.js `
  tests-node/node_config_runtime_parity_gate.test.js `
  tests-node/workflow_contract_gate.test.js `
  tests-node/operator_catalog_gate.test.js `
  tests-node/fallback_governance_gate.test.js `
  tests-node/frontend_convergence_gate.test.js `
  tests-node/governance_store_schema_gate.test.js `
  tests-node/local_workflow_store_schema_gate.test.js
```

Observed result during reassessment:

- targeted gate and Rust-authoritative validation paths pass
- no evidence was found that the repo has reverted to handwritten node-config type maps or local-legacy governance defaults on the main path

## Executive Judgment

AIWF has now moved from “early convergence” into “governed convergence”.

That is real progress.

But the main risk has also changed:

- the biggest danger is no longer only application sprawl
- it is now governance-layer sprawl plus duplicated interpreters

The repo is getting better at controlling drift.
It is not yet equally good at deleting the structures that create drift.

## What Is Clearly Better Than Before

### 1. Control-plane boundaries are no longer implicit

The repo now clearly supports this split:

- `base-java` owns job lifecycle
- `glue-python` owns governance state
- Rust owns operator semantics truth

This is visible in:

- `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
- `apps/glue-python/aiwf/governance_surface.py`
- `docs/governance_control_plane_boundary_20260324.md`
- `ops/scripts/check_governance_control_plane_boundary.ps1`

This is a real architecture gain.

### 2. Node-config rule authorship is no longer trapped in handwritten JS maps

The repository now has:

- `contracts/desktop/node_config_contracts.v1.json`
- generated contract consumers for desktop
- shared fixtures
- a validation-error item contract
- parity and coverage gates

Representative files:

- `contracts/desktop/node_config_contracts.v1.json`
- `contracts/desktop/node_config_contract_fixtures.v1.json`
- `contracts/desktop/node_config_validation_errors.v1.json`
- `apps/glue-python/aiwf/node_config_contract_runtime.py`
- `ops/scripts/check_node_config_schema_coverage.ps1`
- `ops/scripts/check_node_config_runtime_parity.ps1`

This is one of the strongest structural improvements in the repo.

### 3. Governance store access is more disciplined and more structured

Desktop governance stores now normalize provider resolution and preserve structured remote failure payloads instead of flattening everything to opaque error strings.

Representative files:

- `apps/dify-desktop/workflow_governance.js`
- `apps/dify-desktop/workflow_store_remote_error.js`
- `apps/dify-desktop/workflow_app_registry_store.js`
- `apps/dify-desktop/workflow_version_store.js`
- `apps/dify-desktop/workflow_quality_rule_store.js`
- `apps/dify-desktop/workflow_manual_review_store.js`
- `apps/dify-desktop/workflow_run_audit_store.js`

This is the right direction.

### 4. The WinUI primary frontend decision is now engineering reality, not just wording

The repo consistently treats:

- WinUI as primary frontend
- Electron as compatibility shell

That is reflected in:

- `README.md`
- `docs/frontend_convergence_decision_20260320.md`
- `ops/scripts/check_frontend_convergence.ps1`
- WinUI-oriented packaging and release wrappers

The strategy is now materially wired into the project.

## What Is Still Not Actually Finished

### 1. Governance still does not fully own workflow authoring semantics

This is the most important unfinished truth.

Today:

- `glue-python` owns workflow app/version storage routes
- but those save paths still only reject:
  - top-level graph contract violations
  - unregistered node types

They do **not** yet fully reject contract-covered invalid node-config semantics on save.

Current code and tests align on this:

- `apps/glue-python/aiwf/governance_workflow_apps.py`
- `apps/glue-python/aiwf/governance_workflow_versions.py`
- `apps/glue-python/tests/test_app.py`

That means the project currently sits in this state:

- contract-backed node-config parity infrastructure exists
- governance authoring routes still only partially adopt it

This is a meaningful intermediate state, not final convergence.

### 2. Duplicated interpreters are still real

Even after the node-config parity work, validator execution semantics still exist in more than one runtime:

- desktop JS interpreter
- glue-python Python interpreter

So the current truth is not “single executable authority”.
It is:

- single rule authorship
- multiple interpreters
- parity gates to stop drift

That is acceptable as a transition.
It is not a desirable long-term steady state.

### 3. Electron remains the dominant maintenance surface for authoring semantics

The decision has converged.
The maintenance load has not converged as far.

Most active workflow authoring complexity still lands in:

- `apps/dify-desktop/renderer/workflow/*`
- `apps/dify-desktop/workflow_*`
- `apps/dify-desktop/tests-node/*`

So the more accurate statement is:

- frontend strategy has converged
- authoring maintenance weight still leans heavily toward Electron

## The Real New Risk

The main architecture risk is now second-order complexity.

The repo has accumulated a substantial control layer around the product:

- contract helpers
- generated manifests
- generated capability modules
- store schema gates
- parity gates
- fallback governance rules
- frontend convergence gates
- architecture scorecards

This is useful.
But it is now large enough to become its own maintenance surface.

Representative examples:

- `apps/dify-desktop/renderer/workflow/workflow-contract.js`
- `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js`
- `ops/scripts/check_operator_catalog_sync.ps1`
- `ops/scripts/check_node_config_schema_coverage.ps1`
- `ops/scripts/check_node_config_runtime_parity.ps1`
- `ops/scripts/check_governance_store_schema_versions.ps1`
- `ops/scripts/check_frontend_convergence.ps1`

The danger is no longer just “missing boundaries”.
It is:

- governance complexity rising faster than product complexity is being deleted

## Current Best Reading Of The System

The most accurate high-level description of AIWF today is:

- a multi-authority platform under active convergence
- with explicit control-plane boundaries
- contract-backed runtime parity work in progress
- a primary WinUI frontend strategy
- and an Electron compatibility surface that still carries most authoring complexity

That is healthier than the repo’s earlier state.
It is not yet a simplified final state.

## What Must Not Be Misstated

These statements would currently be inaccurate:

- “contracts now fully own workflow semantics”
- “governance save paths now fully reject contract-invalid node-config”
- “desktop and glue now only consume contract outputs and no longer interpret rules”
- “frontend maintenance weight has already moved off Electron”

These statements are currently accurate:

- control-plane ownership is much clearer
- contract-backed rule authorship is materially better
- parity and coverage discipline are much better
- generated artifact chains are now critical infrastructure
- the remaining hard problem is duplicated interpreters plus unfinished semantic ownership transfer

## Final Conclusion

AIWF is materially better structured than it was on 2026-03-24.

But the most important truth on 2026-03-25 is not “convergence is done”.
It is:

- convergence is real
- authority boundaries are real
- parity infrastructure is real
- but governance-layer complexity and duplicated interpreters are now the main architectural liabilities

If the next phase goes well, the project should move from:

- controlling drift

to:

- deleting the structures that make drift possible

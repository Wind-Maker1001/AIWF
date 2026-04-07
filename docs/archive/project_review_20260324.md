# AIWF Project Review (2026-03-24)

> Historical snapshot. Retained for review lineage; not current implementation authority.

## Scope

This report reassesses the repository under `D:\AIWF` after the architecture convergence work landed between March 20, 2026 and March 24, 2026.

The review focuses on:

- cross-runtime architecture boundaries
- workflow contract enforcement
- Rust operator metadata ownership
- desktop and native frontend convergence
- governance store ownership and schema versioning
- the new gate layer added under `ops/scripts`

This is not a generic status report. It is a candid architecture review centered on what has materially improved, what has merely moved, and what new risks are now forming.

## Validation Performed

Targeted architecture gate tests executed successfully on 2026-03-24:

```powershell
cd D:\AIWF\apps\dify-desktop
node --test `
  tests-node/workflow_contract_gate.test.js `
  tests-node/operator_catalog_gate.test.js `
  tests-node/fallback_governance_gate.test.js `
  tests-node/frontend_convergence_gate.test.js `
  tests-node/governance_store_schema_gate.test.js `
  tests-node/local_workflow_store_schema_gate.test.js
```

Observed result:

- 10 tests passed
- 0 failed

## Executive Findings

### 1. Architecture convergence is now real work, not just intent

This repository is no longer merely talking about contract-first convergence.

Actual enforcement now exists in the desktop main path:

- workflow top-level `version` is explicitly normalized and enforced
- unregistered node types are blocked earlier
- Rust operator metadata is being exported into a machine-readable manifest
- new sync gates are wired into `ci_check.ps1`

This is a genuine improvement over the earlier state, where many boundaries only existed in docs or informal discipline.

### 2. The main risk has shifted from application sprawl to governance-layer sprawl

The biggest architecture risk is no longer only "large files" or "mixed concerns in app code".

It is now the growth of a second-order architecture layer:

- contract helpers
- generated manifests
- palette policies
- local storage contracts
- governance store schema gates
- fallback governance gates
- frontend convergence gates

This layer is valuable, but it is now substantial enough to become its own maintenance surface.

Representative examples:

- `apps/dify-desktop/renderer/workflow/workflow-contract.js`
- `apps/dify-desktop/renderer/workflow/rust_operator_manifest.generated.js`
- `ops/scripts/check_operator_catalog_sync.ps1`
- `ops/scripts/check_governance_store_schema_versions.ps1`
- `ops/scripts/check_local_workflow_store_schema_versions.ps1`

Truth:

- you are no longer only engineering product features
- you are engineering a policy-and-contract platform around the product

If this layer is not aggressively simplified and kept authority-driven, it can become a new form of architecture debt.

### 3. `glue-python` is drifting toward a second control plane

This is the most important hidden architectural truth in the current codebase.

`glue-python` used to be primarily:

- flow runtime
- preprocess runtime
- orchestration runtime

It is now also taking ownership of:

- quality rule sets
- workflow sandbox rules
- workflow sandbox autofix state
- workflow app registry
- workflow versions
- manual review queue/history
- workflow run audit
- run baselines

The route surface in `apps/glue-python/app.py` now includes a broad governance API family under `/governance/...`.

This may be the correct short-term move for local-first convergence, but it changes the architecture:

- `base-java` remains the formal control plane for jobs and `job_context`
- `glue-python` is becoming the practical control plane for governance state and workflow authoring artifacts

If you do not explicitly codify this split, you will recreate control-plane logic in two runtimes.

### 4. Rust operator metadata ownership is finally becoming explicit

This is one of the strongest improvements in the repository.

The introduction of:

- `contracts/rust/operators_manifest.v1.json`
- `contracts/rust/operators_manifest.schema.json`
- generated desktop modules based on manifest export
- `check_operator_catalog_sync.ps1`

means the repo is moving from:

- "desktop hand-maintains a parallel understanding of Rust operators"

to:

- "Rust operator truth is exportable and testable"

This is high-value architecture work.

However, the convergence is not complete yet.

Current truth:

- Rust is closer to being the metadata authority
- desktop still contains significant manifest-derived or manifest-adjacent logic
- generation discipline itself is now a critical dependency

The next mistake would be to stop at "generated files exist" without fully institutionalizing:

- who owns generation
- when it runs
- how drift is blocked
- which files are authoritative vs generated

### 5. Workflow contract enforcement is materially better, but still implementation-heavy

Desktop workflow contract enforcement is no longer decorative.

The main path now contains:

- top-level `version` handling
- focused contract normalization
- import-time migration behavior
- early rejection for unregistered node types
- run-payload version propagation

This is a real gain.

But the implementation is still heavily embedded in JS logic rather than externalized contract assets.

The strongest example is `apps/dify-desktop/renderer/workflow/workflow-contract.js`, which now contains:

- top-level workflow validation
- node config validation
- type-specific config rules

This is functional, but it creates a new risk:

- the workflow schema in `contracts/workflow/` may stay thin
- the real schema may silently migrate into JavaScript

That would mean convergence succeeded at the runtime level but failed at the contract-authority level.

### 6. Frontend convergence has a decision, but not yet a fully reduced implementation surface

The repo now contains an explicit convergence decision:

- WinUI is the primary frontend
- Electron is a bounded compatibility shell

That is a meaningful improvement over ambiguous dual-front-end drift.

But the active change surface still shows that Electron remains very large and heavily touched.

Current reality:

- the decision exists
- the gates exist
- the repo activity is still dominated by desktop/Electron workflow work

So the convergence is directionally correct, but not yet economically complete.

In practice, this means:

- strategy has changed faster than maintenance weight has changed

That gap is normal for a transition, but it must keep shrinking.

### 7. Schema versioning discipline has improved sharply

This is the second strongest improvement after operator metadata convergence.

There is now a visible, repeated pattern across:

- workflow graph
- governance stores
- local workflow store containers
- template pack artifacts
- local template storage
- offline template catalogs

This is a real architecture maturity signal.

The repository is finally moving from:

- "JSON exists"

to:

- "JSON objects have named schema versions and migration expectations"

That is exactly the right direction.

### 8. The repo now risks overfitting to local governance mechanics

A new blind spot is emerging:

- a large amount of convergence work is centered on local stores, desktop governance overlays, version snapshots, audit mirrors, and compatibility retirement

This is useful, but it can pull the architecture into a local administration bias.

The platform still needs strong attention on the core business path:

- job creation
- run flow
- operator execution
- artifacts
- auditability
- domain evolution

If too much architecture energy goes into sidecar governance machinery, the core domain model can stagnate while the tooling around it becomes increasingly elaborate.

## What Has Genuinely Improved

### Workflow contract is now present in the execution path

This is a substantive improvement.

Before:

- workflow schema existed
- runtime behavior could still bypass or partially ignore it

Now:

- desktop import/save/run paths are aware of versioned workflow shape
- unknown node types are treated as contract violations earlier

That is real architecture progress.

### Rust authority is becoming machine-readable

The introduction of an exported operator manifest is not a cosmetic refactor.

It is a move toward actual platform authority.

This is one of the few changes in the repo that directly reduces future cross-runtime drift.

### CI and release gates now carry architectural intent

The new script family under `ops/scripts` is not just extra checking.

It means architectural rules are starting to become operational rules.

That is exactly how architecture becomes durable.

### Governance and persistence are now version-aware

The repo is showing more discipline around long-lived state.

This is especially important in a local-first system, where file-backed state can easily become implicit and fragile.

## Current Architectural Risks

### Risk 1. `glue-python` becomes a broad governance server without an explicit re-splitting plan

If `glue-python` continues to absorb governance stores and governance APIs, then you need to decide whether it is now:

- still only a runtime host
- or a formal governance/control-plane component

If you avoid making that decision, the architecture will remain semantically split:

- Java owns official control-plane language
- Python owns practical governance state

That is survivable for a while, but dangerous long-term.

### Risk 2. The true schema may migrate out of `contracts/` and into code helpers

If the most complete workflow and node-config validation lives in:

- desktop JS helpers
- Python normalization code
- per-surface logic

then `contracts/` stops being the authority and becomes documentation.

That would be a regression disguised as convergence.

### Risk 3. Generated artifacts can become pseudo-authority

Generated manifest files are helpful, but only if the source of truth remains obvious.

If the team starts editing around generated outputs, or if generation is not mandatory and reproducible, then you will end up with:

- source truth
- generated truth
- effective runtime truth

all slightly different.

### Risk 4. Gate count can outgrow the team's ability to reason about them

The repository now has a rapidly growing governance/gate layer.

That is a strength only if:

- each gate has a crisp authority boundary
- failures are interpretable
- there are not too many near-duplicate checks

Otherwise you risk a future where:

- architecture is guarded by many scripts
- but nobody can confidently explain which script is the real boundary owner

### Risk 5. Frontend convergence may remain rhetorical if Electron keeps absorbing mainline architectural logic

If WinUI is truly the primary frontend, then over time you should expect:

- fewer new ownership-heavy features landing first in Electron
- Electron increasingly consuming already-defined contracts rather than inventing new ones

If the opposite continues, the decision will remain nominal.

## The Most Important Truths You Are Likely To Underestimate

### Truth 1. You have already crossed from "application architecture" into "platform governance architecture"

That changes what good design looks like.

The hard part is no longer only feature decomposition.
The hard part is:

- authority
- versioning
- drift control
- generated metadata discipline
- lifecycle and retirement of compatibility layers

If you keep evaluating your architecture with only "module split" instincts, you will miss the real risk.

### Truth 2. A versioned local-first platform can die from meta-complexity even while feature architecture improves

Your core application code may get cleaner while the surrounding:

- contract helpers
- migration code
- asset catalogs
- governance stores
- sync gates
- packaging flows

gets harder to understand as a system.

That is now a first-class risk in AIWF.

### Truth 3. Convergence is not complete when gates exist; it is complete when duplicate semantic ownership disappears

This is the most important standard to hold.

You should not evaluate success by:

- how many schema versions exist
- how many gates were added
- how many docs were written

You should evaluate success by:

- how many things now have one unquestioned owner

That is the real architecture metric.

## Recommended Next Actions

### Immediate

1. Decide explicitly whether `glue-python` is now a governance/control-plane component or only a temporary host for local-first governance stores.
2. Move toward a stronger contract authority model for workflow and node config, so `contracts/` does not lag behind JS validation logic.
3. Make operator manifest generation non-optional and clearly document the authority chain:
   - Rust source
   - manifest export
   - generated desktop modules
   - desktop consumption
4. Reduce duplicate or overlapping gates where possible by grouping them under fewer clearly-owned architecture checks.

### Near-Term

5. Create an architecture inventory that classifies every new gate by owner, source authority, and blocking intent.
6. Start measuring Electron vs WinUI architectural load, not only user-facing feature parity.
7. Continue moving long-lived local stores to explicit versioned containers, but do not allow store-versioning work to dominate all architecture bandwidth.

### Medium-Term

8. Promote high-value node config schemas from implementation-only JS into reusable contract assets.
9. Define a retirement path for governance logic that should not permanently live in `glue-python`, if any.
10. Add a single architecture scorecard document or generated artifact that reports:
    - single-source coverage
    - fallback inventory
    - generated artifact drift
    - front-end convergence status

## Updated Assessment

Compared with the March 13, 2026 review:

- architecture discipline has improved materially
- contract and schema awareness have improved sharply
- metadata ownership is moving in the right direction
- CI carries more architectural meaning

Updated judgment:

- platform architecture maturity: **higher than before**
- architecture convergence maturity: **real, but still mid-flight**
- maintainability: **improved in the core direction, but now exposed to governance-layer bloat**
- most important active risk: **control-plane ambiguity plus meta-layer growth**

If forced to summarize the current state in one sentence:

AIWF is no longer mainly suffering from a lack of architecture; it is now at risk of suffering from too many partially-centralized architecture mechanisms unless authority is simplified aggressively.

## Final Verdict

This project is stronger than it was ten days ago.

The best news is:

- the repo has started turning architectural intent into enforced reality

The main warning is:

- the convergence work itself is now large enough to need its own convergence discipline

The next phase should therefore focus less on adding more governance machinery and more on proving that:

- one concept has one owner
- one contract has one authority
- one generated artifact has one source
- one frontend is actually primary in implementation effort, not only in documents

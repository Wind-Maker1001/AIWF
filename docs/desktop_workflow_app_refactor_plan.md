# Desktop Workflow App Refactor Plan

This note captures the current refactor plan for:

- `apps/dify-desktop/renderer/workflow/app.js`

It reflects the current mainline structure after the recent small-step controller extractions.

## Why This Exists

`workflow/app.js` used to be the largest orchestration file in the desktop renderer.

Current file size:

- `apps/dify-desktop/renderer/workflow/app.js`: about 1000+ lines

That is down substantially from the earlier 2200+ line state. The file is now mostly a shell that wires together extracted controllers plus a small amount of top-level state.

## Current Extracted Modules

The following concerns are already out of `app.js`:

- `diagnostics-ui.js`
- `diagnostics-panel-ui.js`
- `preflight-ui.js`
- `preflight-actions-ui.js`
- `preflight-controller-ui.js`
- `preflight-rust-helpers.js`
- `panels-ui.js`
- `template-ui.js`
- `config-ui.js`
- `app-form-ui.js`
- `app-publish-ui.js`
- `flow-io-ui.js`
- `palette-ui.js`
- `connectivity-ui.js`
- `canvas-view-ui.js`
- `graph-shell-ui.js`
- `run-payload-ui.js`
- `run-controller-ui.js`
- `status-ui.js`
- `support-ui.js`
- `quality-gate-ui.js`
- `sandbox-ui.js`
- `audit-ui.js`
- `version-cache-ui.js`
- `run-queue-ui.js`
- `review-queue-ui.js`
- `quality-rule-set-ui.js`
- `debug-api-ui.js`
- `canvas.js`
- `store.js`
- `graph.js`
- `elements.js`

## Refactor Rule

Do not try to fully decompose `app.js` into zero local logic.

The remaining shell is acceptable if it keeps:

- initialization order explicit
- state ownership obvious
- top-level orchestration easy to audit

Extract only code that is:

- stateless
- presenter-like
- pure transformation
- IPC wrapper logic with narrow inputs/outputs
- shell-adjacent helpers that reduce top-level noise without obscuring control flow

## Current Hotspots

The current `app.js` mainly still owns these groups:

### 1. Shell Status and Final Wiring

Key functions:

- `setStatus`
- `renderAll`

Why hot:

- these are the final top-level shell entry points used by many extracted modules
- careless cleanup here can create startup-order or TDZ regressions

Recommendation:

- keep them in `app.js`
- only extract if there is a very clear shell abstraction with no initialization risk

### 2. Remaining Top-Level State

Key state:

- `cfgViewMode`
- `selectedEdge`
- `lastCompareResult`
- `lastPreflightReport`
- `lastAutoFixSummary`
- `lastTemplateAcceptanceReport`
- the `renderMigrationReport` bridge

Why hot:

- this is the last shared state tying together multiple extracted modules
- moving it blindly would make ownership less clear, not more clear

Recommendation:

- prefer small cleanup passes
- do not force these into a store or coordinator unless a real need appears

### 3. Initialization Order

Key risk:

- `app.js` now creates many modules that depend on each other through injected callbacks
- the main remaining complexity is startup order, not business logic size

Recommendation:

- when cleaning further, prioritize deterministic initialization order
- validate every cleanup with `test:unit` and `test:workflow-ui`

## What Was Extracted in This Phase

This phase completed these controller/helper moves out of `app.js`:

- published app / schema orchestration
- audit / timeline / failure summary controller
- version / cache controller
- run history / queue controller
- diagnostics panel controller
- review queue controller
- quality rule set controller
- compare baseline / lineage helpers
- template pack management
- flow import / export / save-load controller
- palette / node creation controller
- connectivity / offline-boundary controller
- canvas view controller
- graph shell controller
- payload builders
- run controller
- debug API shell
- quality-gate prefs handling
- preflight Rust helpers
- preflight controller
- preflight export / acceptance actions

## Best Next Steps

Recommended order from here:

1. `app.js shell cleanup`
   - initialization order only
   - lowest risk
2. `stop extracting`
   - preferred default
   - the remaining shell is already small and understandable
3. `only extract again if a new concrete hotspot appears`
   - for example a new panel family or repeated orchestration logic

## What Not To Extract Yet

Avoid extracting these unless a stronger boundary becomes necessary:

- the top-level shell status orchestration
- the last shell state variables that coordinate extracted modules
- initialization-order glue whose main job is to make startup explicit

## Validation After Each Extraction

Run at minimum:

```powershell
cd .\apps\dify-desktop
npm run test:unit
npm run test:workflow-ui
```

If a change touches runtime orchestration more deeply, also consider:

```powershell
npm run smoke
```

## Success Criteria

A refactor step is good if:

- user-visible behavior does not change
- moved code has a narrow ownership boundary
- `app.js` gets simpler without hiding orchestration behind unclear indirection
- tests remain green without brittle new coupling

At the current stage, success no longer means “make `app.js` smaller at any cost”.
Success now means:

- keep `app.js` readable
- keep startup order stable
- stop before decomposition starts hurting clarity

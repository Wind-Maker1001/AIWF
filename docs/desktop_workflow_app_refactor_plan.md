# Desktop Workflow App Refactor Plan

This note captures the current refactor plan for:

- `apps/dify-desktop/renderer/workflow/app.js`

It is based on archaeology of earlier desktop handoff notes plus the current mainline file layout.

## Why This Exists

`workflow/app.js` is still the largest orchestration file in the desktop renderer.

Current file size:

- `apps/dify-desktop/renderer/workflow/app.js`: about 2200+ lines

The file already delegates a lot of leaf concerns to extracted modules:

- `diagnostics-ui.js`
- `preflight-ui.js`
- `panels-ui.js`
- `template-ui.js`
- `config-ui.js`
- `app-form-ui.js`
- `support-ui.js`
- `canvas.js`
- `store.js`
- `graph.js`
- `elements.js`

That means the remaining content in `app.js` is mostly orchestration glue, cross-panel coordination, and top-level workflow actions.

## Refactor Rule

Do not try to “fully decompose” `app.js` in one pass.

The historical desktop handoff conclusion still applies:

- keep state-coupled orchestration in `workflow/app.js` until dependency seams are explicit
- extract only code that is:
  - stateless
  - presenter-like
  - pure transformation
  - IPC wrapper logic with narrow inputs/outputs

## Current Hotspots

The current `app.js` still owns these large concern groups:

### 1. Workflow run orchestration

Key functions:

- `runPayload`
- `runWorkflow`
- `enqueueWorkflowRun`
- `refreshRunHistory`
- `refreshQueue`
- `pauseQueue`
- `resumeQueue`

Why hot:

- these functions coordinate store state, preflight results, UI status, queue actions, and runtime calls
- changes here are easy to make but hard to reason about globally

Recommendation:

- keep the top-level run orchestration in `app.js`
- only extract helper builders and response normalizers

### 2. Preflight and quality-gate flow

Key functions:

- `runWorkflowPreflight`
- `exportPreflightReport`
- `runTemplateAcceptance`
- `exportTemplateAcceptanceReport`
- `refreshQualityGateReports`
- `exportQualityGateReports`

Why hot:

- this logic mixes graph validation, Rust operator probing, acceptance reporting, and UI status

Recommendation:

- extract pure report-shaping helpers first
- keep “when to run what” decisions in `app.js`

### 3. Published app / schema form orchestration

Key functions:

- `publishApp`
- `refreshApps`
- app-schema sync helpers
- run-params sync helpers

Why hot:

- this area mixes form serialization, JSON normalization, schema rendering, and publish IPC

Recommendation:

- next extraction target is a narrow “workflow app publish controller” module
- do not move shared store or status responsibilities yet

### 4. Audit / sandbox / cache / timeline admin surfaces

Key functions:

- `refreshTimeline`
- `refreshFailureSummary`
- `refreshSandboxAlerts`
- `exportSandboxAudit`
- `loadSandboxRules`
- `saveSandboxRules`
- `refreshSandboxRuleVersions`
- `refreshSandboxAutoFixLog`
- `refreshAudit`

Why hot:

- these are operational dashboards layered on top of the same app shell
- they are good candidates for extraction because they are panel-oriented

Recommendation:

- extract one panel family at a time:
  - sandbox
  - audit
  - quality gate
- each extracted module should own:
  - data fetch
  - row formatting
  - export formatting
- keep global status updates in `app.js`

## Best Next Extractions

Recommended order:

1. `workflow quality/report helpers`
   - pure helpers only
   - lowest risk
2. `published app / schema publish controller`
   - medium risk
   - clear UI boundary
3. `sandbox panel controller`
   - medium risk
   - panel-local behavior
4. `audit + timeline panel controller`
   - medium risk
5. `queue/run orchestration`
   - highest risk
   - delay until seams are much clearer

## What Not To Extract Yet

Avoid extracting these until there is a stronger integration boundary:

- the top-level `setStatus` / user feedback orchestration
- the main workflow run path
- queue and run history coordination
- logic that simultaneously touches:
  - store
  - canvas
  - preflight
  - panel rendering
  - IPC state

## Extra Risk Notes

- `app.js` still contains some historical mojibake in built-in template labels and descriptions
- if you touch template metadata during refactor, treat text cleanup as a separate change from structural extraction
- keep each extraction small enough that:
  - `npm run test:unit`
  - `npm run test:workflow-ui`
  still give fast feedback

## Validation After Each Extraction

Run at minimum:

```powershell
cd .\apps\dify-desktop
npm run test:unit
npm run test:workflow-ui
```

If the extraction touches run orchestration, also run:

```powershell
npm run smoke
```

## Success Criteria

A refactor step is good if:

- user-visible behavior does not change
- the moved code has a narrow ownership boundary
- `app.js` gets smaller without becoming a thin file that still secretly coordinates everything through globals
- tests remain green without adding brittle integration coupling

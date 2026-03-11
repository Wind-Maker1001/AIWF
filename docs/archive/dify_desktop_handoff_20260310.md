# Dify Desktop Electron Frontend Handoff (2026-03-10)

## Scope

This handoff covers the Electron frontend work under `apps/dify-desktop`, including:

- Fluent / touch / high-DPI refactor
- single-window dual-tab shell (`Home` + `Workflow Studio`)
- home page script modularization
- workflow page modularization attempts

This file is intended for post-compaction pickup.

---

## Safe Remote Baseline

The latest **known-good pushed branch state** is:

- Branch: `codex/electron-fluent-touch-dpi-20260309`
- Remote commit: `1861dea`
- Commit message: `desktop: stabilize workflow studio editor modules`

That branch already contains:

- Fluent/touch/high-DPI improvements
- single-window dual-tab shell
- home/workflow layout fixes
- home page modularization
- workflow modularization up to:
  - `elements.js`
  - `diagnostics-ui.js`
  - `preflight-ui.js`
  - `template-utils.js`
  - `panels-ui.js`
  - `template-ui.js`
  - `config-ui.js`
  - `app-form-ui.js`
  - `support-ui.js`

If pickup needs a safe checkpoint, start from that remote branch/commit.

---

## Current Branch State

Current local branch:

- `codex/rust-module-review-20260309`

The previously local-only desktop follow-up batch has now been consolidated into branch history:

- Local commit: `27b683b`
- Commit message: `refactor(desktop): modularize shell and workflow canvas`

This batch contains the Fluent/touch/high-DPI work, the single-window shell work, the home-page modularization, and the workflow canvas / viewport stabilization that had previously only existed in the working tree.

Updated existing files in that batch:

- `apps/dify-desktop/main_window_support.js`
- `apps/dify-desktop/renderer/fluent-init.js`
- `apps/dify-desktop/renderer/fluent-shell.css`
- `apps/dify-desktop/renderer/index.html`
- `apps/dify-desktop/renderer/workflow.html`
- `apps/dify-desktop/renderer/workflow/app.js`
- `apps/dify-desktop/renderer/workflow/canvas.js`
- `apps/dify-desktop/renderer/workflow/canvas_consts.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_interactions.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_nodes.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_viewport.mjs`
- `apps/dify-desktop/tests-node/main_window_support.test.js`
- `apps/dify-desktop/tests/main-ui.spec.js`
- `apps/dify-desktop/tests/workflow-ui.spec.js`

Added modularization / coverage files in that batch:

- `apps/dify-desktop/renderer/home-app.js`
- `apps/dify-desktop/renderer/home-gate.js`
- `apps/dify-desktop/renderer/home-runtime.js`
- `apps/dify-desktop/renderer/home-shared.js`
- `apps/dify-desktop/renderer/home-templates.js`
- `apps/dify-desktop/renderer/workflow/config-ui.js`
- `apps/dify-desktop/renderer/workflow/diagnostics-ui.js`
- `apps/dify-desktop/renderer/workflow/elements.js`
- `apps/dify-desktop/renderer/workflow/panels-ui.js`
- `apps/dify-desktop/renderer/workflow/preflight-ui.js`
- `apps/dify-desktop/renderer/workflow/template-ui.js`
- `apps/dify-desktop/renderer/workflow/template-utils.js`
- `apps/dify-desktop/tests-node/renderer_home_structure.test.js`
- `apps/dify-desktop/tests-node/workflow_canvas_viewport.test.js`

---

## Completed Work In This Batch

### 1. Fluent / touch / DPI refactor

Files:

- `apps/dify-desktop/main_window_support.js`
- `apps/dify-desktop/renderer/fluent-init.js`
- `apps/dify-desktop/renderer/fluent-shell.css`
- `apps/dify-desktop/renderer/workflow/canvas.js`
- `apps/dify-desktop/renderer/workflow/canvas_interactions.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_nodes.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_viewport.mjs`
- `apps/dify-desktop/renderer/workflow/canvas_consts.mjs`

What was done:

- larger responsive window defaults
- touch-target sizing improvements
- responsive Fluent shell layout
- touch node drag / pan / minimap behavior
- high-DPI minimap bitmap sizing
- fit-to-view / anchored zoom

### 2. Single-window dual-tab shell

Files:

- `apps/dify-desktop/renderer/index.html`
- `apps/dify-desktop/renderer/home-app.js`
- `apps/dify-desktop/renderer/fluent-shell.css`
- `apps/dify-desktop/renderer/fluent-init.js`
- `apps/dify-desktop/renderer/workflow/app.js`
- `apps/dify-desktop/tests/main-ui.spec.js`

What was done:

- `Home` + `Workflow Studio` inside one main window
- `Workflow Studio` embedded via iframe
- parent bridge reuse in embedded workflow mode
- old standalone workflow path kept available separately

### 3. Home page modularization

Files:

- `apps/dify-desktop/renderer/home-shared.js`
- `apps/dify-desktop/renderer/home-templates.js`
- `apps/dify-desktop/renderer/home-runtime.js`
- `apps/dify-desktop/renderer/home-gate.js`
- `apps/dify-desktop/renderer/home-app.js`
- `apps/dify-desktop/tests-node/renderer_home_structure.test.js`

What was done:

- moved home page away from one huge inline script
- split by concerns
- added home-ready signal for test stability

### 4. Workflow modularization (stable pushed portion)

Files:

- `apps/dify-desktop/renderer/workflow/elements.js`
- `apps/dify-desktop/renderer/workflow/diagnostics-ui.js`
- `apps/dify-desktop/renderer/workflow/preflight-ui.js`
- `apps/dify-desktop/renderer/workflow/template-utils.js`
- `apps/dify-desktop/renderer/workflow/panels-ui.js`
- `apps/dify-desktop/renderer/workflow/template-ui.js`
- `apps/dify-desktop/renderer/workflow/config-ui.js`
- `apps/dify-desktop/renderer/workflow/app-form-ui.js`
- `apps/dify-desktop/renderer/workflow/support-ui.js`

These were pushed in multiple commits up to remote commit `1861dea`.

---

## Earlier Local Breakage That Was Repaired

After the safe remote baseline, there was an intermediate local extraction wave that broke workflow startup before the current batch repaired it.

That broken attempt centered on continuing to separate:

- sandbox helper functions
- compare/review history helpers
- remaining workflow orchestration glue

### Symptoms from the broken intermediate state

The failing runs at that point happened when executing:

- `tests/main-ui.spec.js`
- `tests/workflow-ui.spec.js`
- `npm run test:unit`

Observed failures included:

- workflow baseline graph rendered **0 nodes**
- many workflow UI tests failed in cascade
- repeated runtime initialization-order failures during workflow startup
- explicit runtime errors seen during this phase included:
  - `Cannot access 'renderNodeConfigEditor' before initialization`
  - `Cannot access 'ONLINE_REQUIRED_NODE_TYPES' before initialization`
  - earlier in the same sequence: missing `currentSandboxPresetPayload`
- once workflow startup breaks, most canvas-related UI tests fail secondarily

### Most likely root cause

The next extraction wave went too far into **ordering-sensitive orchestration code** in `workflow/app.js`.

The failures in that intermediate state were most likely caused by:

- initialization order / TDZ issues from moving functions below code that now references them earlier
- partially completed support-module extraction with mixed local/global references
- constants and helper functions used before declaration after refactors

This is **not** the old `config-ui.js` issue anymore; that part was stabilized and already pushed.

The important current-state note is:

- the ordering / TDZ breakage above has been repaired in the batch landed at `27b683b`
- keep this section only as historical context in case similar startup regressions reappear later

---

## Recommended Recovery Strategy

### Recommended current pickup

1. Start from current branch commit:
   - `27b683b`
2. Re-run the committed validation set:
   - `npm run test:unit`
   - `npm run test:workflow-ui`
3. Continue modularization in smaller slices from that point

### Conservative fallback baseline

If a future regression requires backing up to the last independently known remote-safe checkpoint, use:

- branch: `codex/electron-fluent-touch-dpi-20260309`
- commit: `1861dea`

### If continuing from the current committed batch

Focus first on the local unpushed workflow extraction state:

- `apps/dify-desktop/renderer/workflow/app.js`
- `apps/dify-desktop/renderer/workflow/support-ui.js`
- `apps/dify-desktop/renderer/workflow/app-form-ui.js`
- `apps/dify-desktop/renderer/workflow/config-ui.js`

Likely fixes needed:

- restore stable initialization order
- avoid any function/const TDZ by either:
  - moving constants back above early callers
  - or deferring module initialization until after all required symbols exist
- keep orchestration functions in `app.js` until extraction boundaries are clearer

Search targets:

- `renderNodeConfigEditor`
- `syncCanvasPanels`
- `refreshOfflineBoundaryHint`
- `ONLINE_REQUIRED_NODE_TYPES`
- `currentSandboxPresetPayload`
- `createWorkflowSupportUi`

### Safer alternative split plan

From the current committed batch (`27b683b`), keep:

- keep `workflow/app.js` as orchestration root
- only continue extracting:
  - pure utilities
  - stateless row renderers
  - isolated UI form helpers
- do **not** move orchestration/state coupling pieces until there is an explicit dependency interface

---

## Validation History

### Current committed batch

Validated from the current branch on 2026-03-10:

- `npm run test:unit`
- `npm run test:workflow-ui`

Result:

- pass

### Known-good earlier remote baseline

The following were green at the safe remote baseline:

- `npm run test:unit`
- `tests/main-ui.spec.js`
- `tests/workflow-ui.spec.js`
- `npm run smoke`
- `npm run build:win:dir`

### Historical broken point

The repaired broken point happened **after** the already-stable remote baseline and **before** the current committed batch restored startup order.

---

## Useful Commands

Safe baseline:

```powershell
git checkout codex/electron-fluent-touch-dpi-20260309
git reset --hard 1861dea
```

Frontend verification:

```powershell
cd apps\dify-desktop
npm run test:unit
npm run test:workflow-ui
npm run smoke
npm run build:win:dir
```

---

## Suggested Next Step

Best next pickup:

1. start from current commit `27b683b`
2. rerun:
   - `tests/workflow-ui.spec.js`
   - `tests/main-ui.spec.js`
   - `npm run test:unit`
3. continue modularization in smaller steps
4. keep state-coupled orchestration in `workflow/app.js` until explicit dependency seams are clearer
5. once green, push a new commit on:
   - `codex/electron-fluent-touch-dpi-20260309`

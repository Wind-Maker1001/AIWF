# Electron Capability Inventory

Date: 2026-03-21

This inventory turns Electron retirement into an executable backlog.

Machine-readable companion inventory:

- `contracts/desktop/electron_compatibility_inventory.v1.json`

2026-05-08 review update:

- unresolved Electron-only capabilities were re-reviewed and moved into the machine-readable inventory above
- the JSON inventory is now the CI-enforced source for owner / migration target / remove_by / blocker completeness
- this markdown file remains the human-readable explanation layer

Meaning:

- `covered`: WinUI already has a usable primary-path equivalent
- `partial`: WinUI has some of the flow, but not the full admin/governance surface
- `missing`: capability is still effectively Electron-only
- `compat-hidden`: capability is still Electron-only, but no longer appears on the default Workflow Studio surface; it now requires explicit `--workflow-admin` or `?legacyAdmin=1`

2026-03-21 shrink already landed:

- governance, diagnostics, publish, compare, review, and pack-management surfaces are no longer part of the default Electron compatibility entry
- this is containment, not migration completion

Routing rule confirmed on 2026-03-21:

- technically heavy semantics, policy engines, queue/sandbox orchestration, lineage/diff computation, and publish pipelines should move to backend-owned services
- workflows that need frequent manual adjustment should stay in frontend-owned clients, but should consume backend truth rather than define their own runtime truth
- when a capability contains both, the engine moves backend and the human adjustment surface stays frontend

## A. WinUI Already Covers The Primary User Path

| Capability | WinUI status | Evidence | Disposition | Review date |
| --- | --- | --- | --- | --- |
| Main desktop launch shell | covered | `App.xaml.cs` | keep in WinUI only | 2026-04-05 |
| Workspace run inputs | covered | `MainWindow.RunFlow.cs` | keep in WinUI only | 2026-04-05 |
| Bridge health check | covered | `MainWindow.RunFlow.cs` | keep in WinUI only | 2026-04-05 |
| Run submission | covered | `MainWindow.RunFlow.cs` | keep in WinUI only | 2026-04-05 |
| Result binding and artifact list | covered | `MainWindow.Results.Binding.cs` | keep in WinUI only | 2026-04-05 |
| Canvas navigation shell | covered | `MainWindow.Navigation.cs` | keep in WinUI only | 2026-04-19 |
| Canvas interaction shell | covered | `MainWindow.Canvas.*.cs` | keep in WinUI only | 2026-04-19 |
| WinUI publish / bundle / msix release path | covered | `ops/scripts/release_frontend_productize.ps1` | keep in WinUI only | 2026-04-19 |

## B. Electron-Only Capabilities With Routing Decisions

| Capability | Current status | Electron surface | WinUI status | Disposition | Owner | Target date | Blocker |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Workflow Studio advanced diagnostics panel | covered (`compat-hidden`) | `renderer/workflow/diagnostics-*.js` | covered | backendize diagnostics aggregation; keep only a thin frontend viewer if needed | runtime/backend | 2026-04-19 | WinUI diagnostics viewer over lifecycle run history, timeline, failure aggregation, and rust runtime stats landed on 2026-05-28 |
| Audit timeline / failure summary / review history | partial (`compat-hidden`) | `renderer/workflow/audit-*.js`, `review-queue-*.js` | partial | backendize audit/query model; frontend consumes query APIs | governance/backend | 2026-04-19 | WinUI review history date filters and export landed on 2026-05-29; remaining gap is broader audit/timeline export parity and offline_local mirror retirement |
| Review queue and manual review admin surface | partial (`compat-hidden`) | `renderer/workflow/review-queue-*.js` | partial | keep manual decision UI in frontend; backendize review state and transitions | governance/frontend + backend | 2026-04-19 | WinUI manual review queue/history plus review history date filters and export landed by 2026-05-29; remaining gap is broader governance parity and offline_local provider retirement |
| Sandbox alerts / rules / autofix management | partial (`compat-hidden`) | `renderer/workflow/sandbox-*.js`, `panels-ui-governance-*` | partial | backendize policy engine/versioning; keep rule editing, mute, and override UI in frontend | governance/backend + frontend | 2026-05-19 | WinUI governance viewer/editor for current rules, versions, rollback, mute helper, and autofix state/history override landed on 2026-03-22; remaining gap is runtime mirror retirement and any Electron-only deep debug tooling |
| Workflow version cache / compare UI | covered (`compat-hidden`) | `renderer/workflow/version-cache-*.js` | covered | backendize version index/diff computation; keep result viewer thin | workflow/platform backend | 2026-05-19 | WinUI version compare viewer and local cache parity landed on 2026-05-23 |
| Queue control / run history admin panels | partial (`compat-hidden` for admin controls) | `renderer/workflow/run-queue-*.js`, `panels-ui-*.js` | partial | backendize queue control semantics; keep operator control panel in frontend | workflow/platform backend + frontend | 2026-04-19 | WinUI has run path, not admin queue tooling |
| Quality rule set center | partial (`compat-hidden`) | `renderer/workflow/quality-rule-set-*.js` | partial | backendize rule storage/evaluation/versioning; keep frequent rule editing in frontend | governance/backend + frontend | 2026-05-19 | WinUI governance list/save/delete editor landed on 2026-03-22; offline_local provider retirement still pending |
| Template marketplace / pack install/export | covered (`compat-hidden` for pack management) | `renderer/workflow/template-ui*.js` | covered | keep in frontend; this is human-curated workflow authoring, not backend runtime truth | workflow/product frontend | 2026-05-19 | WinUI local-only template marketplace, pack install/export, shared local template storage migration, and builtin template parity landed on 2026-05-27 |
| Publish workflow app surface | covered (`compat-hidden`) | `renderer/workflow/app-publish-ui.js` | covered | backendize publish pipeline/registry semantics; keep publish form in frontend | workflow/product backend + frontend | 2026-05-19 | WinUI publish workflow app form landed on 2026-05-23 |
| Baseline compare / lineage helper surfaces | partial (`compat-hidden`) | `renderer/workflow/support-ui-*.js` | partial | backendize lineage/compare computation; frontend remains a viewer/filter surface | workflow/governance backend + frontend | 2026-05-19 | WinUI baseline list/save/compare viewer landed on 2026-05-29; remaining gap is historical lineage persistence/query parity and compare report export parity |
| Workflow debug API shell | covered (`compat-hidden` + release-disabled) | `renderer/workflow/debug-api-ui.js` | covered | keep only as dev-only frontend over debug endpoints, or delete once backend/debug contracts are sufficient | frontend/devex | 2026-05-19 | WinUI dev-only workflow debug shell landed on 2026-05-28 with explicit opt-in gating and graph helper parity |

## C. Electron Surfaces That Can Stay Longer As Compatibility-Only

| Capability | Why it may remain | Rule |
| --- | --- | --- |
| Workflow Studio compatibility entrypoint | still useful for legacy node/config/admin work while WinUI catches up | no new primary user features |
| Legacy Electron installer/portable packaging | still useful for existing users who rely on current shell | compatibility-only release path only |
| Dev/debug helpers tied to Electron runtime | may still reduce debugging cost during transition | keep off primary onboarding and primary release path |

## D. Electron Surfaces That Should Be Deleted Instead Of Ported

| Capability | Reason | Review date |
| --- | --- | --- |
| Embedded dual-tab shell language that implies co-primary status | already architecturally incorrect after WinUI-primary decision | 2026-04-05 |
| Any new Electron-only release wrapper or default launcher | directly violates convergence rules | immediate |
| Any new Electron-only contract/admin panel without explicit migration target | creates new retirement debt | immediate |

## E. Execution Checklist

### By 2026-04-05

- confirm which Electron-only capabilities are truly still needed
- assign owners to every item in section B
- decide for each item: migrate, retain, or delete

### By 2026-04-19

- migrate or remove the highest-value admin/diagnostic surfaces first
- remove any remaining default-facing references that still imply Electron parity

### By 2026-05-19

- re-review every retained compatibility surface
- any retained item must have an explicit blocker and next date

### By 2026-06-18

- either remove Electron compatibility release from active guidance
- or publish a blocker review explaining why it still exists

## F. Small-Project Interpretation

Because this project is mainly distributed through personal/friend sideload:

- not every Electron-only capability needs immediate migration
- but every Electron-only capability must still be deliberately classified
- the goal is to prevent maintenance drift, not to force arbitrary deletion speed

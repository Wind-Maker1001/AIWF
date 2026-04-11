# Electron Compatibility Retirement Plan

Date: 2026-03-21

## Current Role

`apps/dify-desktop` is a secondary compatibility frontend.

It exists to cover:

- Workflow Studio compatibility
- transition-only diagnostics and governance panels
- temporary packaging continuity while WinUI becomes the only primary frontend

It is no longer allowed to define the main desktop roadmap.

## Hard Rules Effective 2026-03-21

Allowed in Electron:

- compatibility fixes
- migration helpers that reduce WinUI adoption risk
- diagnostics and governance tools that are not yet ported
- packaging continuity work that is explicitly temporary

Not allowed in Electron:

- new primary user-facing features
- new canonical workflow/runtime contracts
- new default release entrypoints
- re-exposing governance or diagnostics panels on the default compatibility entry
- indefinite compatibility fallbacks without removal criteria

Routing rule confirmed on 2026-03-21:

- technically heavy behavior should move behind backend-owned contracts
- frequently adjusted human workflows should remain frontend surfaces
- hybrid capabilities should split engine/backend from editor-or-operator UI/frontend

## Retirement Phases

### Phase A: Freeze

Start: 2026-03-21

Rules:

- Electron remains supported only as a compatibility shell.
- All default launch and release entrypoints remain WinUI-first.
- New Electron work must state why the same change does not belong in WinUI.

Exit:

- role boundaries are documented and enforced in scripts/docs

### Phase B: Compatibility Inventory

Target date: 2026-04-05

Deliverables:

- inventory of Electron-only capabilities still lacking WinUI equivalents
- each item tagged with:
  - owner
  - migration target
  - remove_by
  - blocker

Inventory:

- [electron_capability_inventory_20260321.md](electron_capability_inventory_20260321.md)
- [archive/fallback_retirement_backlog_20260322.md](archive/fallback_retirement_backlog_20260322.md)

Exit:

- every remaining Electron-only surface has an owner and disposition

### Phase C: Default Surface Shrink

Target date: 2026-04-19

Deliverables:

- remove Electron from any remaining default onboarding/release language
- move Electron release guidance fully under compatibility-only docs
- keep only the minimum compatibility release path
- require explicit `--workflow-admin` or `?legacyAdmin=1` for governance/admin panels

Exit:

- a new contributor cannot mistake Electron for a co-primary frontend
- the default `--workflow` path no longer exposes governance/admin surfaces as normal user features

### Phase D: Compatibility-Only Maintenance

Start: 2026-04-20
Review checkpoint: 2026-05-19

Rules:

- Electron changes require explicit compatibility justification
- Electron package outputs are secondary artifacts, not the main release artifact
- high-risk compatibility-only code must carry a deletion condition

Exit:

- remaining Electron surfaces are small, bounded, and clearly transitional

### Phase E: Remove Or Re-justify

Review date: 2026-06-18

Decision:

- if WinUI covers required user paths, remove Electron compatibility release from active release guidance
- otherwise publish a blocker review explaining:
  - what still depends on Electron
  - why it is still blocked
  - the next dated checkpoint

## Removal Triggers

Electron release and compatibility scope should be removed when all of the following are true:

- WinUI covers the normal user run path
- WinUI covers the default release path
- Workflow Studio compatibility needs are either migrated or intentionally dropped
- no critical diagnostics capability depends uniquely on Electron

## Small-Project Guidance

Because this project is distributed mainly via personal/friend sideload rather than enterprise deployment:

- retirement pressure should focus on reducing maintenance cost, not satisfying enterprise process
- if a small Electron compatibility surface remains useful, it may stay longer
- but it must remain clearly secondary and must not retake default entrypoint status

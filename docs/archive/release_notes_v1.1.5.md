# AIWF v1.1.5 Release Notes

Release Date: 2026-02-26
Commit: 4865fe5

## Highlights
- Landed governance core in desktop workflow runtime.
- Added role-based graph authorization (`owner/analyst/reviewer`).
- Added AI budget hard gates (calls/tokens/cost per run).
- Added SLA evaluation for workflow and node execution time.
- Added lineage summary extraction in workflow results.
- Kept default anti-hallucination policy for data-class inputs.

## New Files
- `apps/dify-desktop/workflow_governance.js`
- `apps/dify-desktop/tests-node/workflow_engine_governance.test.js`
- `ops/scripts/acceptance_production_matrix.ps1`
- `ops/scripts/check_offline_readiness.ps1`
- `ops/config/governance_profile.example.json`

## Runtime Changes
- `apps/dify-desktop/workflow_engine.js`
  - returns `governance`, `lineage`, `sla` in run result.
  - blocks forbidden graphs before execution.
- `apps/dify-desktop/workflow_chiplets/builtin_chiplets.js`
  - enforces AI budget gates in `ai_strategy_v1` and `ai_refine`.
- `apps/dify-desktop/renderer/workflow/app.js`
  - run summary now shows SLA status, lineage edge count, AI call count.

## Validation
- Unit tests: `49/49` pass (`npm run -s test:unit`).
- Production acceptance matrix passed:
  - unit
  - smoke
  - acceptance real samples
  - office gate

## Notes
- `v1.1.4` remains unchanged as previous release tag.
- This `v1.1.5` is the governance + production hardening release.

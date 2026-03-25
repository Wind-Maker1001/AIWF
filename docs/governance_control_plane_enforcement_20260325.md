# Governance Control Plane Enforcement (2026-03-25)

This note records the enforcement decision after the 2026-03-24 follow-up review.

## Decision

- `base-java` remains the only `job lifecycle control plane`
- `glue-python` remains the `governance state control plane`
- current work does not migrate governance state back into `base-java`

The short reason is simple: the current risk is uncontrolled growth of governance semantics, not lack of a single process.

## What is enforced now

- `apps/glue-python/aiwf/governance_surface.py` is the single metadata source for glue-owned governance surfaces
- each governance capability declares `route_prefix`, `owned_route_prefixes`, `state_owner`, `source_of_truth`, and `control_plane_role`
- each governance capability explicitly keeps `lifecycle_mutation_allowed = false`
- `apps/glue-python/app.py` now derives governance capability metadata from `governance_surface.py` instead of maintaining a second handwritten metadata map
- `apps/dify-native-winui` now primes and consumes the governance control-plane boundary before governance-page queries, so WinUI governance requests can follow boundary-owned route metadata instead of relying only on handwritten endpoint assumptions
- `apps/dify-desktop` governance stores now resolve glue governance routes through `workflow_governance.js` control-plane support instead of treating handwritten `/governance/*` paths as the primary authority
- `contracts/governance/governance_capabilities.v1.json` and the desktop / WinUI generated capability constants are now regenerated from `apps/glue-python/aiwf/governance_surface.py` through `ops/scripts/export_governance_capabilities.ps1`
- `ops/scripts/check_governance_control_plane_boundary.ps1` validates that runtime `/governance/*` routes stay covered by the declared surface metadata

## Drift that now fails

- adding a new `/governance/*` route without registering it in the governance surface metadata
- changing a governance surface to a non-`governance_state` role
- changing `state_owner` away from `glue-python`
- changing `job_lifecycle_control_plane_owner` away from `base-java`
- allowing lifecycle mutation through governance metadata
- creating duplicate or non-governance route ownership prefixes

## What this does not mean

This does not make `glue-python` the owner of job lifecycle.

It means the repository now admits the real split and constrains it:

- lifecycle stays in Java
- governance state stays in Python
- drift between runtime routes and declared governance ownership is no longer silent

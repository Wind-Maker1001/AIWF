# AIWF Architecture Authority Charter (2026-04-25)

This document is the current top-level implementation authority for AIWF architecture convergence.

Use this document to decide:

- who owns state truth
- who owns lifecycle mutation
- who owns executable workflow semantics
- which surfaces are compatibility-only and must shrink over time

Historical reviews, inventories, and follow-up notes remain useful context, but this charter is the active decision baseline.

## Platform Shape

AIWF is not treated as a single-backend / single-frontend application.

The target operating model is:

- `apps/base-java` owns the lifecycle plane
- `apps/glue-python` owns the governance plane
- `apps/accel-rust` owns workflow / node-config / operator executable semantics
- `apps/dify-native-winui` is the only primary frontend allowed to grow the main user path
- `apps/dify-desktop` remains a compatibility shell with explicit retirement pressure

## Authority Rules

### 1. Control planes

- `base-java` owns job / step / artifact lifecycle plus `job_context` transport.
- `glue-python` owns governance state persistence and governance read/write APIs.
- New lifecycle semantics must not be added to `glue-python`.
- Governance state must not be reintroduced as a default desktop-local owner.

### 2. Executable semantics

- `accel-rust` is the sole executable authority for workflow validation, workflow draft/reference execution, node-config contract execution, and operator catalog semantics.
- JS / Python / C# may keep editor hints, migration helpers, formatting, and presentation logic.
- JS / Python / C# must not become long-lived second interpreters for workflow / node-config / operator semantics.

### 3. Frontend growth

- WinUI is the primary frontend.
- Electron is a compatibility shell only.
- Electron may keep compatibility entrypoints, transition-only admin surfaces, and dev/debug helpers with explicit disposition.
- Electron must not gain new default-facing primary-path semantics.

## Canonical Object Boundaries

Do not use `workflow graph` as a cross-lifecycle currency object.

The system should converge around these explicit objects:

- `WorkflowDefinition`
- `WorkflowVersionSnapshot`
- `PublishedWorkflowApp`
- `WorkflowRunReferenceRequest`
- `WorkflowExecutionEnvelope`
- `TemplateArtifact`

Rules:

- `workflow_definition` is the canonical workflow payload field.
- `graph` is allowed only as an import / migration alias.
- `graph` must not appear as the canonical persisted or public API field for workflow save / publish / version / template routes.

## Adapter And Fallback Discipline

- `offline_local` and `base_api` are adapters, not semantic authorities.
- Adapters may differ in transport and availability handling only.
- Adapters must consume the same validated workflow envelope and must not extend top-level workflow semantics.

Every active compatibility fallback must have:

- `owner`
- `reason`
- `added_at`
- `remove_by`
- `success_metric`
- `kill_condition`

The active inventory lives in:

- `contracts/governance/fallback_inventory.v1.json`

Fallbacks without explicit metadata are architecture violations.

## Generated Consumer Rule

When a backend-owned manifest or contract is consumed by frontend or shell code:

- the manifest/contract remains the source authority
- consumers should be generated or validated thin clients
- handwritten consumers must not duplicate route ownership, policy ownership, or executable validation semantics

This rule currently applies most strongly to:

- governance capability metadata
- node-config contract coverage
- operator catalog / manifest consumption

## Current Implementation Priorities

### Phase 1

- keep this charter as the top-level authority entrypoint
- keep `fallback_inventory.v1.json` current
- enforce canonical workflow API checks and fallback metadata checks in CI

### Phase 2

- delete duplicated runtime interpreters outside Rust
- converge manifest -> generated consumer chains for governance and node-config consumers

### Phase 3

- move engine-heavy Electron surfaces behind backend-owned services
- keep only thin WinUI / Electron viewers and editors

### Phase 4

- delete compatibility and mirror shells that no longer have an active blocker

## Non-Negotiable Review Checklist

Every new architecture-significant change must answer:

- state owner:
- lifecycle owner:
- semantic owner:
- frontend owner:
- migration target:
- remove_by / kill_condition (if compatibility fallback exists):

If any of the above is missing, the change is not architecture-ready.

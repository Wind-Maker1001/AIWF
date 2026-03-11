# Backend Chiplet Decoupling Status

## Summary

The backend chiplet/domain decoupling work is now effectively complete.

The control plane (`base-java`), flow runtime (`glue-python`), and operator runtime (`accel-rust`) all expose and consume a shared domain/catalog capability model, and the latest full local CI run passed successfully.

Validation log:

- `ops/logs/ci/ci_full_chiplet_pure_20260311_140205.log`

## Final State

### 1. `base-java`

`base-java` now acts as a chiplet-aware control plane.

It exposes:

- `GET /api/v1/backend/capabilities`

This aggregates:

- glue flow domains
- accel published operator domains
- accel workflow operator domains

Key files:

- `apps/base-java/src/main/java/com/aiwf/base/service/BackendCapabilitiesService.java`
- `apps/base-java/src/main/java/com/aiwf/base/web/BackendController.java`
- `apps/base-java/src/main/java/com/aiwf/base/glue/GlueClient.java`
- `apps/base-java/src/main/java/com/aiwf/base/glue/GlueGateway.java`

### 2. `glue-python`

`glue-python` now uses an explicit runtime catalog and runtime state boundary instead of relying on scattered global registry access.

Completed layers:

- flow registry is domain-aware
- input readers are domain-aware
- preprocess transforms / filters / stages are domain-aware
- cleaning core artifacts are domain-aware
- office artifacts are domain-aware
- extension loading is runtime-scoped
- registry event collection is runtime-scoped

Runtime files:

- `apps/glue-python/aiwf/runtime_state.py`
- `apps/glue-python/aiwf/runtime_catalog.py`

Provider/domain bootstrap files:

- `apps/glue-python/aiwf/domains/ingest.py`
- `apps/glue-python/aiwf/flows/domains/cleaning.py`
- `apps/glue-python/aiwf/flows/domains/cleaning_core_artifacts.py`
- `apps/glue-python/aiwf/flows/domains/cleaning_office_artifacts.py`

Core registry files:

- `apps/glue-python/aiwf/flows/registry.py`
- `apps/glue-python/aiwf/ingest.py`
- `apps/glue-python/aiwf/preprocess_registry.py`
- `apps/glue-python/aiwf/flows/cleaning_artifacts.py`
- `apps/glue-python/aiwf/flows/office_artifacts.py`
- `apps/glue-python/aiwf/extensions.py`
- `apps/glue-python/aiwf/registry_events.py`
- `apps/glue-python/aiwf/capabilities.py`
- `apps/glue-python/app.py`

Practical outcome:

- app execution and capability collection both go through the runtime catalog
- built-in provider registration no longer depends on import-time side effects
- runtime state is no longer shared implicitly across all code paths

### 3. `accel-rust`

`accel-rust` now uses a unified operator catalog and domain-owned routing / workflow registration structure.

Completed layers:

- shared operator metadata catalog
- capabilities built from catalog metadata
- workflow resolution metadata built from catalog metadata
- workflow trace includes catalog/domain resolution info
- HTTP operator exposure split into domain route fragments
- workflow step handler registration split into domain-owned descriptor slices

Core files:

- `apps/accel-rust/src/operator_catalog.rs`
- `apps/accel-rust/src/governance_ops/contracts/stats.rs`
- `apps/accel-rust/src/http/routes.rs`
- `apps/accel-rust/src/operators/workflow/engine.rs`
- `apps/accel-rust/src/operators/workflow/runner.rs`
- `apps/accel-rust/src/operators/workflow/support.rs`
- `apps/accel-rust/src/operators/workflow/types.rs`

Route fragments:

- `apps/accel-rust/src/http/routes/system.rs`
- `apps/accel-rust/src/http/routes/transform.rs`
- `apps/accel-rust/src/http/routes/table.rs`
- `apps/accel-rust/src/http/routes/integration.rs`
- `apps/accel-rust/src/http/routes/storage_schema.rs`
- `apps/accel-rust/src/http/routes/analysis.rs`
- `apps/accel-rust/src/http/routes/governance.rs`
- `apps/accel-rust/src/http/routes/orchestration.rs`

Workflow domain slices:

- `apps/accel-rust/src/operators/workflow/engine_domains/transform.rs`
- `apps/accel-rust/src/operators/workflow/engine_domains/table.rs`
- `apps/accel-rust/src/operators/workflow/engine_domains/integration.rs`
- `apps/accel-rust/src/operators/workflow/engine_domains/storage_schema.rs`
- `apps/accel-rust/src/operators/workflow/engine_domains/analysis.rs`
- `apps/accel-rust/src/operators/workflow/engine_domains/governance.rs`

Practical outcome:

- operator metadata is no longer duplicated across workflow resolution and capabilities
- HTTP route ownership is domain-organized instead of centralized in one long route table
- workflow step registration is domain-organized instead of centralized in one monolithic registry

## Completion Assessment

### What is complete

- shared backend domain/capability vocabulary across all three backend layers
- domain-owned backend route organization
- domain-aware flow/provider registration
- explicit runtime catalog boundary in Python flow runtime
- runtime-scoped registry/event/extension state in Python
- full local verification passing after all decoupling changes

### What is not considered blocking anymore

There are still internal implementation choices that could be further refined, but they are no longer meaningful blockers for the backend decoupling goal:

- some registry APIs still keep module-level helper functions
- `base-java` remains a control plane rather than a chiplet runtime itself

Those are acceptable implementation details, not architecture-level coupling failures.

## Validation

### Targeted checks

- `apps/glue-python`: runtime catalog / registry / extension / preprocess / artifact tests passed
- `apps/accel-rust`: workflow/bin tests passed
- `apps/base-java`: tests passed after capability aggregation wiring

### Repository checks

- `Quick CI` passed after route/runtime decoupling work
- `Full CI` passed after runtime-state purity changes

Latest full verification:

- `ops/logs/ci/ci_full_chiplet_pure_20260311_140205.log`

## Conclusion

The backend chiplet decoupling effort is complete enough to treat as finished work.

The backend now has:

- a domain-aware control plane
- a runtime-scoped, domain-aware flow runtime
- a catalog-driven, domain-owned operator runtime

At this point, additional work would be optimization or architectural taste refinement, not unfinished primary decoupling work.

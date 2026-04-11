# AIWF Authority And Execution Convergence (2026-04-06)

This note is the current authority summary for the workflow convergence cutover.

## Owners

- `apps/base-java` owns job lifecycle control plane and `job_context` transport.
- `apps/glue-python` owns governance state control plane and governance write/read APIs.
- `apps/accel-rust` is the sole executable authority for workflow validation and workflow execution surfaces.
- `apps/dify-native-winui` is the primary frontend allowed to keep growing authoring UX.
- `apps/dify-desktop` is a compatibility shell only.

## Workflow Authority

- The canonical workflow payload field is `workflow_definition`.
- `graph` is no longer a canonical storage or governance API field.
- Workflow version snapshots store `workflow_definition` only.
- Workflow app publications store metadata plus `published_version_id`; they do not own workflow semantics.
- Template artifacts persist `workflow_definition` as the canonical template field.

## Execution Rules

- `/operators/workflow_contract_v1/validate` is the only workflow validation authority.
- `/operators/workflow_draft_run_v1` and `/operators/workflow_reference_run_v1` are the authoritative execution surfaces.
- Desktop draft/reference run paths fail closed if the authoritative Rust execution surface is unavailable.
- Desktop no longer supports JS draft execution compatibility fallback.

## Electron Freeze

- Electron must not add new workflow semantics, node validators, publish rules, or long-lived local compatibility providers.
- Electron may keep UX adapters, import/export compatibility, and authoritative error presentation.

## Local Shell Objects

- Local template library and local run/audit history remain supported product objects.
- Workflow queue, queue control, node cache, node cache metrics, and template marketplace container files are treated as local shell objects.
- Local shell objects should stay minimally shaped and must not regain long-term `schema_version` formalization.

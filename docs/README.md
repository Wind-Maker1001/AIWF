# AIWF Documentation Hub

Use this page as the canonical documentation entrypoint for the current repository.

## How To Use This Map

- Start with [quickstart.md](quickstart.md) if you want the fastest current local startup path.
- Start with [../README.md](../README.md) if you need the repository overview and common commands first.
- Use [archive/README.md](archive/README.md) when you want dated reviews and earlier migration reasoning.
- Treat compatibility docs as secondary paths, not primary onboarding.
- Treat `archive/` as historical context, not active design authority.

## Start Here

- Repository overview: [../README.md](../README.md)
- Quickstart: [quickstart.md](quickstart.md)
- Native WinUI quickstart: [quickstart_native_winui.md](quickstart_native_winui.md)
- Backend quickstart: [quickstart_backend.md](quickstart_backend.md)
- Verification guide: [verification.md](verification.md)
- Authority and execution convergence: [authority_execution_convergence_20260406.md](authority_execution_convergence_20260406.md)

## Current Architecture and Boundary Docs

- Governance control-plane boundary: [governance_control_plane_boundary_20260324.md](governance_control_plane_boundary_20260324.md)
- Governance control-plane enforcement: [governance_control_plane_enforcement_20260325.md](governance_control_plane_enforcement_20260325.md)
- Authority and execution convergence: [authority_execution_convergence_20260406.md](authority_execution_convergence_20260406.md)
- Node-config contract authority: [node_config_contract_authority_20260324.md](node_config_contract_authority_20260324.md)
- Capability ownership matrix: [capability_ownership_matrix_20260320.md](capability_ownership_matrix_20260320.md)
- Frontend convergence decision: [frontend_convergence_decision_20260320.md](frontend_convergence_decision_20260320.md)

## Current Inventories, Plans, and Backlogs

- Electron capability inventory: [electron_capability_inventory_20260321.md](electron_capability_inventory_20260321.md)
- Desktop workflow app refactor plan: [desktop_workflow_app_refactor_plan.md](desktop_workflow_app_refactor_plan.md)
- Workflow frontend layering guide: [workflow_frontend_layering_guide_20260320.md](workflow_frontend_layering_guide_20260320.md)

## Reviews and Dated Assessments

- Dated reviews and earlier planning snapshots remain available as historical context under [archive/README.md](archive/README.md).

## Runtime, Delivery, and Operations

- Native WinUI delivery: [offline_delivery_native_winui.md](offline_delivery_native_winui.md)
- Personal sideload certificate: [personal_sideload_certificate_20260321.md](personal_sideload_certificate_20260321.md)
- Regression quality guidance: [regression_quality.md](regression_quality.md)
- Rust new-ops performance gate: [perf_gate_new_ops.md](perf_gate_new_ops.md)
- Glue-python regression checklist: [glue_python_regression_checklist.md](glue_python_regression_checklist.md)
- Cleaning rules: [cleaning_rules.md](cleaning_rules.md)
- Latest checked-in release notes: [release_notes_v1.1.6.md](release_notes_v1.1.6.md)

## Integration and Reference

- Dify local integration: [dify_local_integration.md](dify_local_integration.md)
- Dify adapter v1: [dify_adapter_v1.md](dify_adapter_v1.md)
- Dify workflow HTTP node template: [dify_workflow_http_node_template.md](dify_workflow_http_node_template.md)
- Rust extension operators v1: [rust_extension_ops_v1.md](rust_extension_ops_v1.md)
- Finance template usage: [finance_template_v1.md](finance_template_v1.md)
- Backend chiplet decoupling: [backend_chiplet_decoupling.md](backend_chiplet_decoupling.md)

## Compatibility-Only Paths

- Desktop offline quickstart: [quickstart_desktop_offline.md](quickstart_desktop_offline.md)
- Electron compatibility guide: [dify_desktop_app.md](dify_desktop_app.md)
- Electron offline bundle delivery: [offline_delivery_minimal.md](offline_delivery_minimal.md)
- Electron compatibility retirement plan: [electron_compatibility_retirement_plan_20260321.md](electron_compatibility_retirement_plan_20260321.md)

## Archive and Historical Context

- Archived handoff and snapshot docs: [archive/README.md](archive/README.md)

## Documentation Maintenance Rules

- Active top-level docs in `docs/` must be linked from this page.
- New architecture docs should state whether they are a decision, plan, inventory, review, or cognition report.
- New compatibility docs must explicitly say they are compatibility-only or transitional.
- New handoff or snapshot docs should go under `docs/archive/` unless they are still active authority.
- If a dated review is no longer useful for current design or migration reasoning, archive it instead of keeping it as a top-level active doc.

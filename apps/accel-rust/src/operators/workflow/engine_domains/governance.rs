use super::*;

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "runtime_stats_v1", handler: workflow_runtime_stats_v1_handler },
    WorkflowStepDefinition { op: "capabilities_v1", handler: workflow_capabilities_v1_handler },
    WorkflowStepDefinition { op: "io_contract_v1", handler: workflow_io_contract_v1_handler },
    WorkflowStepDefinition { op: "failure_policy_v1", handler: workflow_failure_policy_v1_handler },
    WorkflowStepDefinition { op: "incremental_plan_v1", handler: workflow_incremental_plan_v1_handler },
    WorkflowStepDefinition { op: "tenant_isolation_v1", handler: workflow_tenant_isolation_v1_handler },
    WorkflowStepDefinition { op: "operator_policy_v1", handler: workflow_operator_policy_v1_handler },
    WorkflowStepDefinition { op: "optimizer_adaptive_v2", handler: workflow_optimizer_adaptive_v2_handler },
    WorkflowStepDefinition { op: "vector_index_v2_build", handler: workflow_vector_index_v2_build_handler },
    WorkflowStepDefinition { op: "vector_index_v2_search", handler: workflow_vector_index_v2_search_handler },
    WorkflowStepDefinition { op: "vector_index_v2_eval", handler: workflow_vector_index_v2_eval_handler },
    WorkflowStepDefinition { op: "stream_reliability_v1", handler: workflow_stream_reliability_v1_handler },
    WorkflowStepDefinition { op: "lineage_provenance_v1", handler: workflow_lineage_provenance_v1_handler },
    WorkflowStepDefinition { op: "contract_regression_v1", handler: workflow_contract_regression_v1_handler },
    WorkflowStepDefinition { op: "perf_baseline_v1", handler: workflow_perf_baseline_v1_handler },
];

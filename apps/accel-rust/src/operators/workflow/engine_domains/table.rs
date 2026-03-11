use super::*;

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "join_rows_v1", handler: workflow_join_rows_v1_handler },
    WorkflowStepDefinition { op: "join_rows_v2", handler: workflow_join_rows_v2_handler },
    WorkflowStepDefinition { op: "join_rows_v3", handler: workflow_join_rows_v3_handler },
    WorkflowStepDefinition { op: "join_rows_v4", handler: workflow_join_rows_v4_handler },
    WorkflowStepDefinition { op: "aggregate_rows_v1", handler: workflow_aggregate_rows_v1_handler },
    WorkflowStepDefinition { op: "aggregate_rows_v2", handler: workflow_aggregate_rows_v2_handler },
    WorkflowStepDefinition { op: "aggregate_rows_v3", handler: workflow_aggregate_rows_v3_handler },
    WorkflowStepDefinition { op: "aggregate_rows_v4", handler: workflow_aggregate_rows_v4_handler },
    WorkflowStepDefinition { op: "quality_check_v1", handler: workflow_quality_check_v1_handler },
    WorkflowStepDefinition { op: "quality_check_v2", handler: workflow_quality_check_v2_handler },
    WorkflowStepDefinition { op: "quality_check_v3", handler: workflow_quality_check_v3_handler },
    WorkflowStepDefinition { op: "quality_check_v4", handler: workflow_quality_check_v4_handler },
    WorkflowStepDefinition { op: "window_rows_v1", handler: workflow_window_rows_v1_handler },
];

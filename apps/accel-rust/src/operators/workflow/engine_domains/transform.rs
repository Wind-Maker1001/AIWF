use super::*;

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "transform_rows_v2", handler: workflow_transform_rows_v2_handler },
    WorkflowStepDefinition { op: "transform_rows_v3", handler: workflow_transform_rows_v3_handler },
    WorkflowStepDefinition { op: "text_preprocess_v2", handler: workflow_text_preprocess_v2_handler },
    WorkflowStepDefinition { op: "compute_metrics", handler: workflow_compute_metrics_handler },
    WorkflowStepDefinition { op: "normalize_schema_v1", handler: workflow_normalize_schema_v1_handler },
    WorkflowStepDefinition { op: "entity_extract_v1", handler: workflow_entity_extract_v1_handler },
    WorkflowStepDefinition { op: "aggregate_pushdown_v1", handler: workflow_aggregate_pushdown_v1_handler },
];

use super::*;
use super::super::custom::{
    workflow_ai_audit_handler, workflow_ai_refine_handler, workflow_clean_md_handler,
    workflow_compute_rust_handler, workflow_ingest_files_handler, workflow_manual_review_handler,
    workflow_md_output_handler,
};

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "cleaning", handler: workflow_cleaning_handler },
    WorkflowStepDefinition { op: "ingest_files", handler: workflow_ingest_files_handler },
    WorkflowStepDefinition { op: "clean_md", handler: workflow_clean_md_handler },
    WorkflowStepDefinition { op: "compute_rust", handler: workflow_compute_rust_handler },
    WorkflowStepDefinition { op: "manual_review", handler: workflow_manual_review_handler },
    WorkflowStepDefinition { op: "ai_refine", handler: workflow_ai_refine_handler },
    WorkflowStepDefinition { op: "ai_audit", handler: workflow_ai_audit_handler },
    WorkflowStepDefinition { op: "md_output", handler: workflow_md_output_handler },
    WorkflowStepDefinition { op: "transform_rows_v2", handler: workflow_transform_rows_v2_handler },
    WorkflowStepDefinition { op: "transform_rows_v3", handler: workflow_transform_rows_v3_handler },
    WorkflowStepDefinition { op: "text_preprocess_v2", handler: workflow_text_preprocess_v2_handler },
    WorkflowStepDefinition { op: "compute_metrics", handler: workflow_compute_metrics_handler },
    WorkflowStepDefinition { op: "normalize_schema_v1", handler: workflow_normalize_schema_v1_handler },
    WorkflowStepDefinition { op: "entity_extract_v1", handler: workflow_entity_extract_v1_handler },
    WorkflowStepDefinition { op: "aggregate_pushdown_v1", handler: workflow_aggregate_pushdown_v1_handler },
];

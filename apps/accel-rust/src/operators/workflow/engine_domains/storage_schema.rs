use super::*;

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "load_rows_v2", handler: workflow_load_rows_v2_handler },
    WorkflowStepDefinition { op: "load_rows_v3", handler: workflow_load_rows_v3_handler },
    WorkflowStepDefinition { op: "schema_registry_v2_check_compat", handler: workflow_schema_registry_v2_check_compat_handler },
    WorkflowStepDefinition { op: "schema_registry_v2_suggest_migration", handler: workflow_schema_registry_v2_suggest_migration_handler },
    WorkflowStepDefinition { op: "schema_registry_v1_register", handler: workflow_schema_registry_v1_register_handler },
    WorkflowStepDefinition { op: "schema_registry_v1_get", handler: workflow_schema_registry_v1_get_handler },
    WorkflowStepDefinition { op: "schema_registry_v1_infer", handler: workflow_schema_registry_v1_infer_handler },
    WorkflowStepDefinition { op: "schema_registry_v2_register", handler: workflow_schema_registry_v2_register_handler },
    WorkflowStepDefinition { op: "schema_registry_v2_get", handler: workflow_schema_registry_v2_get_handler },
    WorkflowStepDefinition { op: "schema_registry_v2_infer", handler: workflow_schema_registry_v2_infer_handler },
    WorkflowStepDefinition { op: "feature_store_v1_upsert", handler: workflow_feature_store_v1_upsert_handler },
    WorkflowStepDefinition { op: "feature_store_v1_get", handler: workflow_feature_store_v1_get_handler },
    WorkflowStepDefinition { op: "stream_state_v1_save", handler: workflow_stream_state_v1_save_handler },
    WorkflowStepDefinition { op: "stream_state_v1_load", handler: workflow_stream_state_v1_load_handler },
    WorkflowStepDefinition { op: "stream_state_v2", handler: workflow_stream_state_v2_handler },
    WorkflowStepDefinition { op: "parquet_io_v2", handler: workflow_parquet_io_v2_handler },
];

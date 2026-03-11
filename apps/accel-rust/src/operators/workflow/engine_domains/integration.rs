use super::*;

pub(super) const WORKFLOW_STEP_DEFS: &[WorkflowStepDefinition] = &[
    WorkflowStepDefinition { op: "plugin_exec_v1", handler: workflow_plugin_exec_v1_handler },
    WorkflowStepDefinition { op: "plugin_health_v1", handler: workflow_plugin_health_v1_handler },
    WorkflowStepDefinition { op: "plugin_registry_v1", handler: workflow_plugin_registry_v1_handler },
    WorkflowStepDefinition { op: "plugin_operator_v1", handler: workflow_plugin_operator_v1_handler },
    WorkflowStepDefinition { op: "rules_package_publish_v1", handler: workflow_rules_package_publish_v1_handler },
    WorkflowStepDefinition { op: "rules_package_get_v1", handler: workflow_rules_package_get_v1_handler },
    WorkflowStepDefinition { op: "udf_wasm_v1", handler: workflow_udf_wasm_v1_handler },
    WorkflowStepDefinition { op: "udf_wasm_v2", handler: workflow_udf_wasm_v2_handler },
];

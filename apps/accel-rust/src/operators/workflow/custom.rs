use super::support::{parse_workflow_input, serialize_workflow_output};
use super::*;

pub(super) fn workflow_transform_rows_v2_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<TransformRowsReq>(input)?;
    let resp = run_transform_rows_v2_with_cache(
        req,
        None,
        Some(&state.transform_cache),
        Some(&state.metrics),
    )?;
    serialize_workflow_output(resp)
}

pub(super) fn workflow_plugin_health_v1_handler(
    _: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<PluginHealthReq>(input)?;
    let plugin = safe_pkg_token(&req.plugin)?;
    let details = run_plugin_healthcheck(&plugin, req.tenant_id.as_deref())?;
    Ok(json!({
        "ok": true,
        "operator": "plugin_health_v1",
        "status": "done",
        "plugin": plugin,
        "details": details
    }))
}

pub(super) fn workflow_schema_registry_v2_register_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaRegisterReq>(input)?;
    let mut resp = run_schema_registry_register_v1(state, req)?;
    resp.operator = "schema_registry_v2_register".to_string();
    serialize_workflow_output(resp)
}

pub(super) fn workflow_schema_registry_v2_get_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaGetReq>(input)?;
    let mut resp = run_schema_registry_get_v1(state, req)?;
    resp.operator = "schema_registry_v2_get".to_string();
    serialize_workflow_output(resp)
}

pub(super) fn workflow_schema_registry_v2_infer_handler(
    state: &AppState,
    input: Value,
) -> Result<Value, String> {
    let req = parse_workflow_input::<SchemaInferReq>(input)?;
    let mut resp = run_schema_registry_infer_v1(state, req)?;
    resp.operator = "schema_registry_v2_infer".to_string();
    serialize_workflow_output(resp)
}

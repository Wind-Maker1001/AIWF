use super::*;

pub(crate) fn run_schema_registry_register_v1(
    state: &AppState,
    req: SchemaRegisterReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    let resp = run_schema_registry_register_local(&req)?;
    if let Ok(mut reg) = state.schema_registry.lock() {
        reg.insert(key, req.schema.clone());
    }
    Ok(resp)
}

pub(crate) fn run_schema_registry_get_v1(
    state: &AppState,
    req: SchemaGetReq,
) -> Result<SchemaRegistryResp, String> {
    let key = schema_registry_key(&req.name, &req.version)?;
    if let Ok(reg) = state.schema_registry.lock()
        && let Some(schema) = reg.get(&key).cloned()
    {
        return Ok(SchemaRegistryResp {
            ok: true,
            operator: "schema_registry_v1_get".to_string(),
            status: "done".to_string(),
            name: req.name,
            version: req.version,
            schema,
        });
    }
    run_schema_registry_get_local(&req)
}

pub(crate) fn run_schema_registry_infer_v1(
    state: &AppState,
    req: SchemaInferReq,
) -> Result<SchemaInferResp, String> {
    let resp = run_schema_registry_infer_local(&req)?;
    if let (Some(name), Some(version)) = (req.name.as_ref(), req.version.as_ref())
        && let Ok(key) = schema_registry_key(name, version)
        && let Ok(mut reg) = state.schema_registry.lock()
    {
        reg.insert(key, resp.schema.clone());
    }
    Ok(resp)
}

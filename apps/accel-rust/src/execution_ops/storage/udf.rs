use super::*;

pub(crate) fn run_udf_wasm_v2(req: UdfWasmV2Req) -> Result<Value, String> {
    if let Ok(token) = env::var("AIWF_UDF_V2_TOKEN")
        && !token.trim().is_empty()
    {
        let got = req.signed_token.clone().unwrap_or_default();
        if got != token {
            return Err("udf_wasm_v2 invalid signed_token".to_string());
        }
    }
    let op = req.op.clone().unwrap_or_else(|| "identity".to_string());
    if let Some(allow) = req.allowed_ops.as_ref()
        && !allow.iter().any(|x| x.eq_ignore_ascii_case(&op))
    {
        return Err(format!("udf_wasm_v2 op not allowed: {op}"));
    }
    let mut out = run_udf_wasm_v1(UdfWasmReq {
        run_id: req.run_id.clone(),
        rows: req.rows,
        field: req.field,
        output_field: req.output_field,
        op: Some(op),
        wasm_base64: req.wasm_base64,
    })?;
    let max_bytes = req.max_output_bytes.unwrap_or(1_000_000).max(4096);
    let mut truncated = 0usize;
    while serde_json::to_vec(&out).map(|v| v.len()).unwrap_or(0) > max_bytes {
        let Some(rows) = out.get_mut("rows").and_then(|v| v.as_array_mut()) else {
            break;
        };
        if rows.pop().is_none() {
            break;
        }
        truncated += 1;
    }
    if let Some(stats) = out.get_mut("stats").and_then(|v| v.as_object_mut()) {
        stats.insert("max_output_bytes".to_string(), json!(max_bytes));
        stats.insert("truncated_rows".to_string(), json!(truncated));
        stats.insert("udf_v2".to_string(), json!(true));
    }
    if let Some(obj) = out.as_object_mut() {
        obj.insert("operator".to_string(), json!("udf_wasm_v2"));
    }
    Ok(out)
}

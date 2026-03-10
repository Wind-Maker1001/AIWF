use crate::{
    api_types::{ExplainPlanV1Req, ExplainPlanV2Req, IncrementalPlanV1Req, RuntimeStatsV1Req},
    execution_ops::run_explain_plan_v1,
    misc_ops::read_stream_checkpoint,
    operators::transform::TransformRowsReq,
    transform_support::{transform_cache_key, value_to_f64},
};
use accel_rust::app_state::AppState;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::run_runtime_stats_v1;

pub(crate) fn canonicalize_value(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(canonicalize_value).collect()),
        Value::Object(o) => {
            let mut bm: BTreeMap<String, Value> = BTreeMap::new();
            for (k, v) in o {
                bm.insert(k.clone(), canonicalize_value(v));
            }
            let mut out = Map::new();
            for (k, v) in bm {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

pub(crate) fn incremental_fingerprint(operator: &str, input: &Value) -> String {
    let c = canonicalize_value(input);
    let s = serde_json::to_vec(&json!({"operator": operator, "input": c})).unwrap_or_default();
    let mut h = Sha256::new();
    h.update(&s);
    format!("{:x}", h.finalize())
}

pub(crate) fn run_incremental_plan_v1(
    state: &AppState,
    req: IncrementalPlanV1Req,
) -> Result<Value, String> {
    let operator = req.operator.trim().to_lowercase();
    if operator.is_empty() {
        return Err("incremental_plan_v1 requires operator".to_string());
    }
    let fingerprint = incremental_fingerprint(&operator, &req.input);
    let mut cache_hit = false;
    let mut cache_key = String::new();
    if operator == "transform_rows_v2"
        && let Ok(parsed) = serde_json::from_value::<TransformRowsReq>(req.input.clone())
    {
        cache_key = transform_cache_key(&parsed);
        if let Ok(guard) = state.transform_cache.lock() {
            cache_hit = guard.contains_key(&cache_key);
        }
    }
    let resume_checkpoint = req
        .checkpoint_key
        .as_deref()
        .map(read_stream_checkpoint)
        .transpose()?
        .flatten();
    Ok(json!({
        "ok": true,
        "operator": "incremental_plan_v1",
        "status": "done",
        "run_id": req.run_id,
        "target_operator": operator,
        "fingerprint": fingerprint,
        "cache_key": cache_key,
        "cache_hit": cache_hit,
        "resume_checkpoint": resume_checkpoint,
        "strategy": if cache_hit { "cache_reuse" } else if resume_checkpoint.is_some() { "resume_from_checkpoint" } else { "full_recompute" }
    }))
}

pub(crate) fn run_explain_plan_v2(req: ExplainPlanV2Req) -> Result<Value, String> {
    let v1 = run_explain_plan_v1(ExplainPlanV1Req {
        run_id: req.run_id.clone(),
        steps: req.steps.clone(),
        rows: req.rows.clone(),
        actual_stats: req.actual_stats.clone(),
        persist_feedback: req.persist_feedback,
    })?;
    let mut out = v1.as_object().cloned().unwrap_or_default();
    let mut actual_total_ms = 0.0f64;
    let mut stage = Vec::new();
    if let Some(actuals) = req.actual_stats {
        for a in actuals {
            let Some(o) = a.as_object() else { continue };
            let op = o
                .get("operator")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let est = o.get("estimated_ms").and_then(value_to_f64).unwrap_or(0.0);
            let act = o.get("actual_ms").and_then(value_to_f64).unwrap_or(0.0);
            actual_total_ms += act.max(0.0);
            stage.push(json!({
                "operator": op,
                "estimated_ms": est,
                "actual_ms": act,
                "ratio": if est > 0.0 { act / est } else { 0.0 }
            }));
        }
    }
    out.insert("operator".to_string(), json!("explain_plan_v2"));
    out.insert(
        "actual_total_ms".to_string(),
        json!((actual_total_ms * 100.0).round() / 100.0),
    );
    out.insert("stage_stats".to_string(), Value::Array(stage));
    if req.include_runtime_stats.unwrap_or(false) {
        let rs = run_runtime_stats_v1(RuntimeStatsV1Req {
            run_id: req.run_id,
            op: "summary".to_string(),
            operator: None,
            ok: None,
            error_code: None,
            duration_ms: None,
            rows_in: None,
            rows_out: None,
        })?;
        out.insert("runtime_stats".to_string(), rs);
    }
    Ok(Value::Object(out))
}

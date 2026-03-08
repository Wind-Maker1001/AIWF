use crate::*;
use serde_json::{Map, Value, json};
use std::collections::HashSet;

use super::types::{TransformRowsReq, TransformRowsV3Req};
use super::v2::run_transform_rows_v2;

pub(crate) fn collect_expr_lineage(expr: &Value, out: &mut HashSet<String>) {
    if let Some(o) = expr.as_object() {
        if let Some(f) = o.get("field").and_then(|v| v.as_str()) {
            out.insert(f.to_string());
        }
        if let Some(arr) = o.get("args").and_then(|v| v.as_array()) {
            for a in arr {
                collect_expr_lineage(a, out);
            }
        }
        if let Some(v) = o.get("expr") {
            collect_expr_lineage(v, out);
        }
        if let Some(v) = o.get("then") {
            collect_expr_lineage(v, out);
        }
        if let Some(v) = o.get("else") {
            collect_expr_lineage(v, out);
        }
    }
}

fn eval_expr_v3(row: &Map<String, Value>, expr: &Value) -> Result<Value, String> {
    if let Some(o) = expr.as_object() {
        if let Some(c) = o.get("const") {
            return Ok(c.clone());
        }
        if let Some(field) = o.get("field").and_then(|v| v.as_str()) {
            return Ok(row.get(field).cloned().unwrap_or(Value::Null));
        }
        if let Some(op) = o.get("op").and_then(|v| v.as_str()) {
            let args = o
                .get("args")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let vals = args
                .iter()
                .map(|a| eval_expr_v3(row, a))
                .collect::<Result<Vec<_>, _>>()?;
            let low = op.to_ascii_lowercase();
            let as_bool = |v: &Value| -> bool {
                if let Some(b) = v.as_bool() {
                    b
                } else if let Some(n) = value_to_f64(v) {
                    n != 0.0
                } else {
                    !value_to_string(v).trim().is_empty()
                }
            };
            let num2 = |i: usize| -> f64 { vals.get(i).and_then(value_to_f64).unwrap_or(0.0) };
            return Ok(match low.as_str() {
                "add" => json!(vals.iter().filter_map(value_to_f64).sum::<f64>()),
                "sub" => json!(num2(0) - num2(1)),
                "mul" => json!(vals.iter().filter_map(value_to_f64).product::<f64>()),
                "div" => {
                    let d = num2(1);
                    if d.abs() < f64::EPSILON {
                        Value::Null
                    } else {
                        json!(num2(0) / d)
                    }
                }
                "eq" => {
                    json!(vals.first().map(value_to_string) == vals.get(1).map(value_to_string))
                }
                "ne" => {
                    json!(vals.first().map(value_to_string) != vals.get(1).map(value_to_string))
                }
                "gt" => json!(num2(0) > num2(1)),
                "gte" => json!(num2(0) >= num2(1)),
                "lt" => json!(num2(0) < num2(1)),
                "lte" => json!(num2(0) <= num2(1)),
                "and" => json!(vals.iter().all(as_bool)),
                "or" => json!(vals.iter().any(as_bool)),
                "not" => json!(!vals.first().map(as_bool).unwrap_or(false)),
                "concat" => {
                    let s = vals
                        .iter()
                        .map(value_to_string)
                        .collect::<Vec<_>>()
                        .join("");
                    Value::String(s)
                }
                "lower" => Value::String(
                    vals.first()
                        .map(value_to_string)
                        .unwrap_or_default()
                        .to_lowercase(),
                ),
                "upper" => Value::String(
                    vals.first()
                        .map(value_to_string)
                        .unwrap_or_default()
                        .to_uppercase(),
                ),
                "trim" => Value::String(
                    vals.first()
                        .map(value_to_string)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                ),
                _ => return Err(format!("unsupported expr op: {op}")),
            });
        }
        if let Some(cond) = o.get("if") {
            let c = eval_expr_v3(row, cond)?;
            let passed = c
                .as_bool()
                .unwrap_or_else(|| value_to_f64(&c).unwrap_or(0.0) != 0.0);
            if passed {
                return eval_expr_v3(row, o.get("then").unwrap_or(&Value::Null));
            }
            return eval_expr_v3(row, o.get("else").unwrap_or(&Value::Null));
        }
        if let Some(inner) = o.get("expr") {
            return eval_expr_v3(row, inner);
        }
    }
    if expr.is_string() || expr.is_number() || expr.is_boolean() || expr.is_null() {
        return Ok(expr.clone());
    }
    Err("invalid expr v3".to_string())
}

pub(crate) fn run_transform_rows_v3(req: TransformRowsV3Req) -> Result<TransformRowsResp, String> {
    let base = run_transform_rows_v2(TransformRowsReq {
        run_id: req.run_id.clone(),
        tenant_id: req.tenant_id.clone(),
        trace_id: req.trace_id.clone(),
        traceparent: req.traceparent.clone(),
        rows: req.rows.clone(),
        rules: req.rules.clone(),
        rules_dsl: req.rules_dsl.clone(),
        quality_gates: req.quality_gates.clone(),
        schema_hint: req.schema_hint.clone(),
        input_uri: req.input_uri.clone(),
        output_uri: None,
        request_signature: req.request_signature.clone(),
        idempotency_key: req.idempotency_key.clone(),
    })?;
    let mut rows = base.rows.clone();
    let specs = req.computed_fields_v3.unwrap_or_default();
    let mut lineage: Map<String, Value> = Map::new();
    let filter_expr = req.filter_expr_v3.clone();
    let mut filtered = 0usize;
    if !specs.is_empty() || filter_expr.is_some() {
        let mut out_rows = Vec::new();
        for row in rows {
            let Some(mut obj) = row.as_object().cloned() else {
                continue;
            };
            let mut pass = true;
            if let Some(expr) = filter_expr.as_ref() {
                let cond = eval_expr_v3(&obj, expr)?;
                pass = cond
                    .as_bool()
                    .unwrap_or_else(|| value_to_f64(&cond).unwrap_or(0.0) != 0.0);
            }
            if !pass {
                filtered += 1;
                continue;
            }
            for item in &specs {
                let Some(m) = item.as_object() else { continue };
                let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("").trim();
                let expr = m.get("expr").cloned().unwrap_or(Value::Null);
                if name.is_empty() {
                    continue;
                }
                let v = eval_expr_v3(&obj, &expr)?;
                obj.insert(name.to_string(), v);
                let mut deps = HashSet::new();
                collect_expr_lineage(&expr, &mut deps);
                lineage.insert(
                    name.to_string(),
                    Value::Array(deps.into_iter().map(Value::String).collect()),
                );
            }
            out_rows.push(Value::Object(obj));
        }
        rows = out_rows;
    }
    let mut resp = base;
    resp.operator = "transform_rows_v3".to_string();
    resp.rows = rows;
    let out_len = resp.rows.len();
    if let Some(q) = resp.quality.as_object_mut() {
        q.insert("output_rows".to_string(), json!(out_len));
        q.insert("filtered_by_expr_v3".to_string(), json!(filtered));
    }
    resp.stats.output_rows = out_len;
    if let Some(a) = resp.audit.as_object_mut() {
        a.insert("lineage_v3".to_string(), Value::Object(lineage));
        a.insert("computed_fields_v3".to_string(), json!(specs.len()));
    }
    if let Some(uri) = req.output_uri {
        save_rows_to_uri(&uri, &resp.rows)?;
    }
    Ok(resp)
}

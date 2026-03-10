use crate::{
    analysis_ops::parse_time_order_key,
    api_types::{OptimizerV1Req, WindowRowsV1Req},
    transform_support::{value_to_f64, value_to_string_or_null},
};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

pub(crate) fn run_window_rows_v1(req: WindowRowsV1Req) -> Result<Value, String> {
    if req.functions.is_empty() {
        return Err("window_rows_v1 requires at least one function".to_string());
    }
    let partition_fields = req.partition_by.unwrap_or_default();
    let mut groups: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object().cloned() else {
            continue;
        };
        let key = if partition_fields.is_empty() {
            "__all__".to_string()
        } else {
            partition_fields
                .iter()
                .map(|f| value_to_string_or_null(obj.get(f)))
                .collect::<Vec<_>>()
                .join("|")
        };
        groups.entry(key).or_default().push(obj);
    }
    let mut out = Vec::new();
    for (_pk, mut rows) in groups {
        rows.sort_by(|a, b| {
            let av = value_to_string_or_null(a.get(&req.order_by));
            let bv = value_to_string_or_null(b.get(&req.order_by));
            parse_time_order_key(&av).cmp(&parse_time_order_key(&bv))
        });
        let order_vals = rows
            .iter()
            .map(|r| value_to_string_or_null(r.get(&req.order_by)))
            .collect::<Vec<_>>();
        let mut dense_rank = 0usize;
        let mut last_val = String::new();
        let mut last_rank = 0usize;
        for i in 0..rows.len() {
            let mut row = rows[i].clone();
            let ov = &order_vals[i];
            if i == 0 || *ov != last_val {
                dense_rank += 1;
                last_rank = i + 1;
                last_val = ov.clone();
            }
            for f in &req.functions {
                let Some(cfg) = f.as_object() else {
                    continue;
                };
                let op = cfg
                    .get("op")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_lowercase();
                let as_name = cfg
                    .get("as")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if as_name.is_empty() {
                    continue;
                }
                let val = match op.as_str() {
                    "row_number" => json!(i + 1),
                    "rank" => json!(last_rank),
                    "dense_rank" => json!(dense_rank),
                    "lag" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let off = cfg.get("offset").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        if i >= off {
                            rows[i - off].get(field).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "lead" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let off = cfg.get("offset").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
                        if i + off < rows.len() {
                            rows[i + off].get(field).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    }
                    "moving_avg" => {
                        let field = cfg.get("field").and_then(|v| v.as_str()).unwrap_or("");
                        let window =
                            cfg.get("window").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
                        let w = window.max(1);
                        let start = i.saturating_sub(w - 1);
                        let vals = rows[start..=i]
                            .iter()
                            .filter_map(|x| x.get(field).and_then(value_to_f64))
                            .collect::<Vec<_>>();
                        if vals.is_empty() {
                            Value::Null
                        } else {
                            json!(vals.iter().sum::<f64>() / vals.len() as f64)
                        }
                    }
                    _ => Value::Null,
                };
                row.insert(as_name, val);
            }
            out.push(Value::Object(row));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "window_rows_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out
    }))
}

pub(crate) fn run_optimizer_v1(req: OptimizerV1Req) -> Result<Value, String> {
    let row_count = req
        .row_count_hint
        .unwrap_or_else(|| req.rows.as_ref().map(|r| r.len()).unwrap_or(0));
    let avg_cols = req
        .rows
        .as_ref()
        .map(|rows| {
            if rows.is_empty() {
                0.0
            } else {
                rows.iter()
                    .filter_map(|r| r.as_object().map(|o| o.len() as f64))
                    .sum::<f64>()
                    / rows.len() as f64
            }
        })
        .unwrap_or(0.0);
    let mut engine = if row_count >= 120_000 && avg_cols >= 4.0 {
        "columnar_arrow_v1"
    } else if row_count >= 40_000 {
        "columnar_v1"
    } else {
        "row_v1"
    };
    if req.prefer_arrow.unwrap_or(false) && row_count >= 20_000 {
        engine = "columnar_arrow_v1";
    }
    let join_strategy = if row_count >= 100_000 {
        "sort_merge"
    } else if row_count >= 20_000 {
        "hash"
    } else {
        "auto"
    };
    let aggregate_mode = if row_count >= 60_000 {
        "approx_hybrid"
    } else {
        "exact"
    };
    Ok(json!({
        "ok": true,
        "operator": "optimizer_v1",
        "status": "done",
        "run_id": req.run_id,
        "plan": {
            "execution_engine": engine,
            "join_strategy": join_strategy,
            "aggregate_mode": aggregate_mode,
            "row_count": row_count,
            "avg_columns": avg_cols
        },
        "hints": {
            "join": req.join_hint,
            "aggregate": req.aggregate_hint
        }
    }))
}

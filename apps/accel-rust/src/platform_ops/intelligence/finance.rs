use super::*;

pub(crate) fn run_timeseries_forecast_v1(req: TimeSeriesForecastReq) -> Result<Value, String> {
    let horizon = req.horizon.unwrap_or(3).clamp(1, 60);
    let method = req
        .method
        .unwrap_or_else(|| "naive_drift".to_string())
        .to_lowercase();
    let mut rows = req
        .rows
        .into_iter()
        .filter_map(|r| r.as_object().cloned())
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        parse_time_order_key(&value_to_string_or_null(a.get(&req.time_field))).cmp(
            &parse_time_order_key(&value_to_string_or_null(b.get(&req.time_field))),
        )
    });
    let vals = rows
        .iter()
        .filter_map(|o| o.get(&req.value_field).and_then(value_to_f64))
        .collect::<Vec<_>>();
    if vals.is_empty() {
        return Err("timeseries_forecast_v1 requires non-empty numeric series".to_string());
    }
    let first = vals[0];
    let last = *vals.last().unwrap_or(&first);
    let drift = if vals.len() > 1 {
        (last - first) / (vals.len() as f64 - 1.0)
    } else {
        0.0
    };
    let mut forecast = Vec::new();
    for h in 1..=horizon {
        let pred = if method == "naive_last" {
            last
        } else {
            last + drift * h as f64
        };
        forecast.push(json!({"step": h, "prediction": pred}));
    }
    Ok(
        json!({"ok": true, "operator": "timeseries_forecast_v1", "status": "done", "run_id": req.run_id, "method": method, "forecast": forecast}),
    )
}

pub(crate) fn run_finance_ratio_v1(req: FinanceRatioReq) -> Result<Value, String> {
    let mut out = Vec::new();
    for r in req.rows {
        let Some(mut o) = r.as_object().cloned() else {
            continue;
        };
        let ca = o
            .get("current_assets")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let cl = o
            .get("current_liabilities")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let debt = o.get("total_debt").and_then(value_to_f64).unwrap_or(0.0);
        let equity = o.get("total_equity").and_then(value_to_f64).unwrap_or(0.0);
        let rev = o.get("revenue").and_then(value_to_f64).unwrap_or(0.0);
        let ni = o.get("net_income").and_then(value_to_f64).unwrap_or(0.0);
        let ocf = o
            .get("operating_cash_flow")
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        let qr = if cl.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ca / cl)
        };
        let d2e = if equity.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(debt / equity)
        };
        let nm = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ni / rev)
        };
        let ocf_margin = if rev.abs() < f64::EPSILON {
            Value::Null
        } else {
            json!(ocf / rev)
        };
        o.insert("ratio_current".to_string(), qr);
        o.insert("ratio_debt_to_equity".to_string(), d2e);
        o.insert("ratio_net_margin".to_string(), nm);
        o.insert("ratio_ocf_margin".to_string(), ocf_margin);
        out.push(Value::Object(o));
    }
    Ok(
        json!({"ok": true, "operator": "finance_ratio_v1", "status": "done", "run_id": req.run_id, "rows": out}),
    )
}

pub(crate) fn run_anomaly_explain_v1(req: AnomalyExplainReq) -> Result<Value, String> {
    let th = req.threshold.unwrap_or(0.8);
    let mut anomalies = Vec::new();
    for (idx, r) in req.rows.iter().enumerate() {
        let Some(o) = r.as_object() else { continue };
        let score = o
            .get(&req.score_field)
            .and_then(value_to_f64)
            .unwrap_or(0.0);
        if score < th {
            continue;
        }
        let mut contrib = Vec::new();
        for (k, v) in o {
            if k == &req.score_field {
                continue;
            }
            if let Some(n) = value_to_f64(v) {
                contrib.push((k.clone(), n.abs()));
            }
        }
        contrib.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        anomalies.push(json!({
            "row_index": idx,
            "score": score,
            "top_contributors": contrib.into_iter().take(3).map(|(k,v)| json!({"field":k,"importance":v})).collect::<Vec<_>>()
        }));
    }
    Ok(
        json!({"ok": true, "operator": "anomaly_explain_v1", "status": "done", "run_id": req.run_id, "anomalies": anomalies}),
    )
}

use super::*;

pub(crate) fn run_time_series_v1(req: TimeSeriesReq) -> Result<Value, String> {
    let window = req.window.unwrap_or(3).max(1);
    let groups = req.group_by.unwrap_or_default();
    let mut grouped: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for r in req.rows {
        let Some(obj) = r.as_object() else { continue };
        let k = if groups.is_empty() {
            "__all__".to_string()
        } else {
            groups
                .iter()
                .map(|g| value_to_string_or_null(obj.get(g)))
                .collect::<Vec<_>>()
                .join("|")
        };
        grouped.entry(k).or_default().push(obj.clone());
    }
    let mut out = Vec::new();
    for (_k, mut rows) in grouped {
        rows.sort_by(|a, b| {
            let av = value_to_string_or_null(a.get(&req.time_field));
            let bv = value_to_string_or_null(b.get(&req.time_field));
            parse_time_order_key(&av).cmp(&parse_time_order_key(&bv))
        });
        for i in 0..rows.len() {
            let mut row = rows[i].clone();
            let cur = row
                .get(&req.value_field)
                .and_then(value_to_f64)
                .unwrap_or(0.0);
            let start = i.saturating_sub(window - 1);
            let win = rows[start..=i]
                .iter()
                .filter_map(|r| r.get(&req.value_field).and_then(value_to_f64))
                .collect::<Vec<_>>();
            let ma = if win.is_empty() {
                Value::Null
            } else {
                json!(win.iter().sum::<f64>() / win.len() as f64)
            };
            let mom = if i >= 1 {
                let prev = rows[i - 1]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                json!(cur - prev)
            } else {
                Value::Null
            };
            let yoy = if i >= 12 {
                let prev = rows[i - 12]
                    .get(&req.value_field)
                    .and_then(value_to_f64)
                    .unwrap_or(0.0);
                if prev.abs() < f64::EPSILON {
                    Value::Null
                } else {
                    json!((cur - prev) / prev)
                }
            } else {
                Value::Null
            };
            row.insert("ts_moving_avg".to_string(), ma);
            row.insert("ts_mom".to_string(), mom);
            row.insert("ts_yoy".to_string(), yoy);
            out.push(Value::Object(row));
        }
    }
    Ok(json!({
        "ok": true,
        "operator": "time_series_v1",
        "status": "done",
        "run_id": req.run_id,
        "rows": out,
        "stats": {"window": window}
    }))
}

pub(crate) fn parse_time_order_key(s: &str) -> i64 {
    let t = s.trim();
    if let Ok(v) = t.parse::<i64>() {
        return v;
    }
    let fmts_dt = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];
    for f in fmts_dt {
        if let Ok(dt) = NaiveDateTime::parse_from_str(t, f) {
            return dt.and_utc().timestamp();
        }
    }
    let fmts_d = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m", "%Y/%m", "%Y%m"];
    for f in fmts_d {
        let parsed = if f == "%Y-%m" {
            NaiveDate::parse_from_str(&format!("{t}-01"), "%Y-%m-%d")
        } else if f == "%Y/%m" {
            NaiveDate::parse_from_str(&format!("{t}/01"), "%Y/%m/%d")
        } else if f == "%Y%m" {
            NaiveDate::parse_from_str(&format!("{t}01"), "%Y%m%d")
        } else {
            NaiveDate::parse_from_str(t, f)
        };
        if let Ok(d) = parsed {
            return d
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp())
                .unwrap_or(i64::MAX - 1);
        }
    }
    i64::MAX
}

pub(crate) fn run_stats_v1(req: StatsReq) -> Result<Value, String> {
    let pairs = req
        .rows
        .iter()
        .filter_map(|r| {
            let o = r.as_object()?;
            let x = o.get(&req.x_field).and_then(value_to_f64)?;
            let y = o.get(&req.y_field).and_then(value_to_f64)?;
            Some((x, y))
        })
        .collect::<Vec<_>>();
    if pairs.len() < 2 {
        return Err("stats_v1 requires at least 2 numeric pairs".to_string());
    }
    let n = pairs.len() as f64;
    let sum_x = pairs.iter().map(|(x, _)| *x).sum::<f64>();
    let sum_y = pairs.iter().map(|(_, y)| *y).sum::<f64>();
    let mean_x = sum_x / n;
    let mean_y = sum_y / n;
    let sxy = pairs
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let sxx = pairs.iter().map(|(x, _)| (x - mean_x).powi(2)).sum::<f64>();
    let syy = pairs.iter().map(|(_, y)| (y - mean_y).powi(2)).sum::<f64>();
    let corr = if sxx <= 0.0 || syy <= 0.0 {
        0.0
    } else {
        sxy / (sxx.sqrt() * syy.sqrt())
    };
    let slope = if sxx <= 0.0 { 0.0 } else { sxy / sxx };
    let intercept = mean_y - slope * mean_x;
    let mut residual_ss = 0.0;
    for (x, y) in &pairs {
        let pred = intercept + slope * *x;
        residual_ss += (*y - pred).powi(2);
    }
    let dof = (pairs.len() as f64 - 2.0).max(1.0);
    let stderr = if sxx <= 0.0 {
        f64::INFINITY
    } else {
        (residual_ss / dof / sxx).sqrt()
    };
    let t = if !stderr.is_finite() || stderr <= 0.0 {
        0.0
    } else {
        slope / stderr
    };
    let p = p_value_from_t(t.abs(), dof);
    let tcrit = if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        dist.inverse_cdf(0.975)
    } else {
        1.96
    };
    let ci_low = slope - tcrit * stderr;
    let ci_high = slope + tcrit * stderr;
    let robust_median_y = {
        let mut ys = pairs.iter().map(|(_, y)| *y).collect::<Vec<_>>();
        ys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        ys[ys.len() / 2]
    };
    Ok(json!({
        "ok": true,
        "operator": "stats_v1",
        "status": "done",
        "run_id": req.run_id,
        "metrics": {
            "count": pairs.len(),
            "correlation": corr,
            "slope": slope,
            "intercept": intercept,
            "mean_x": mean_x,
            "mean_y": mean_y,
            "slope_stderr": stderr,
            "slope_t": t,
            "slope_p_value": p,
            "slope_ci95": [ci_low, ci_high],
            "median_y": robust_median_y
        }
    }))
}

pub(crate) fn p_value_from_t(t_abs: f64, dof: f64) -> f64 {
    if !dof.is_finite() || dof <= 0.0 {
        return 1.0;
    }
    if let Ok(dist) = StudentsT::new(0.0, 1.0, dof) {
        2.0 * (1.0 - dist.cdf(t_abs.max(0.0)))
    } else {
        1.0
    }
}

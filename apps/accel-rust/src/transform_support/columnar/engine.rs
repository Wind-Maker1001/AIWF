use super::*;

pub(crate) fn resolve_transform_engine(rules: &Value) -> String {
    if let Some(v) = rule_get(rules, "execution_engine").and_then(|x| x.as_str()) {
        let t = v.trim().to_ascii_lowercase();
        if !t.is_empty() {
            return t;
        }
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "auto_v1".to_string())
}

pub(crate) fn estimate_rule_complexity(rules: &Value) -> usize {
    let casts = rule_get(rules, "casts")
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0);
    let filters = rule_get(rules, "filters")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let dedup = rule_get(rules, "deduplicate_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let sort = rule_get(rules, "sort_by")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let has_agg = if rule_get(rules, "aggregate").is_some() {
        2
    } else {
        0
    };
    casts + filters + dedup + sort + has_agg
}

#[derive(Clone)]
struct EngineProfile {
    medium_rows_threshold: usize,
    large_rows_threshold: usize,
    medium_complexity_threshold: usize,
    medium_bytes_threshold: usize,
    large_bytes_threshold: usize,
    row_cost_per_row: f64,
    columnar_cost_per_row: f64,
    arrow_cost_per_row: f64,
    complexity_weight: f64,
}

fn default_engine_profile() -> EngineProfile {
    EngineProfile {
        medium_rows_threshold: 20_000,
        large_rows_threshold: 120_000,
        medium_complexity_threshold: 8,
        medium_bytes_threshold: 12 * 1024 * 1024,
        large_bytes_threshold: 48 * 1024 * 1024,
        row_cost_per_row: 1.0,
        columnar_cost_per_row: 0.9,
        arrow_cost_per_row: 0.8,
        complexity_weight: 0.08,
    }
}

fn load_engine_profile() -> EngineProfile {
    let default = default_engine_profile();
    let path = env::var("AIWF_RUST_ENGINE_PROFILE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            Path::new(".")
                .join("conf")
                .join("transform_engine_profile.json")
                .to_string_lossy()
                .to_string()
        });
    let txt = match fs::read_to_string(&path) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let v: Value = match serde_json::from_str(&txt) {
        Ok(v) => v,
        Err(_) => return default,
    };
    let get_u = |k: &str, dv: usize| -> usize {
        v.get(k)
            .and_then(|x| x.as_u64())
            .map(|x| x as usize)
            .unwrap_or(dv)
    };
    let get_f = |k: &str, dv: f64| -> f64 { v.get(k).and_then(|x| x.as_f64()).unwrap_or(dv) };
    EngineProfile {
        medium_rows_threshold: get_u("medium_rows_threshold", default.medium_rows_threshold),
        large_rows_threshold: get_u("large_rows_threshold", default.large_rows_threshold),
        medium_complexity_threshold: get_u(
            "medium_complexity_threshold",
            default.medium_complexity_threshold,
        ),
        medium_bytes_threshold: get_u("medium_bytes_threshold", default.medium_bytes_threshold),
        large_bytes_threshold: get_u("large_bytes_threshold", default.large_bytes_threshold),
        row_cost_per_row: get_f("row_cost_per_row", default.row_cost_per_row),
        columnar_cost_per_row: get_f("columnar_cost_per_row", default.columnar_cost_per_row),
        arrow_cost_per_row: get_f("arrow_cost_per_row", default.arrow_cost_per_row),
        complexity_weight: get_f("complexity_weight", default.complexity_weight),
    }
}

pub(crate) fn auto_select_engine(
    input_rows: usize,
    estimated_bytes: usize,
    rules: &Value,
) -> (String, String) {
    let p = load_engine_profile();
    let complexity = estimate_rule_complexity(rules);
    let row_cost =
        p.row_cost_per_row * input_rows as f64 * (1.0 + p.complexity_weight * complexity as f64);
    let col_cost = p.columnar_cost_per_row
        * input_rows as f64
        * (1.0 + 0.6 * p.complexity_weight * complexity as f64);
    let arrow_cost = p.arrow_cost_per_row
        * input_rows as f64
        * (1.0 + 0.5 * p.complexity_weight * complexity as f64);
    if input_rows >= p.large_rows_threshold || estimated_bytes >= p.large_bytes_threshold {
        return (
            "columnar_arrow_v1".to_string(),
            format!(
                "auto: cost large rows={} bytes={} complexity={} row={:.2} col={:.2} arrow={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, arrow_cost
            ),
        );
    }
    if input_rows >= p.medium_rows_threshold
        || estimated_bytes >= p.medium_bytes_threshold
        || complexity >= p.medium_complexity_threshold
    {
        let (eng, best) = if col_cost <= row_cost {
            ("columnar_v1", col_cost)
        } else {
            ("row_v1", row_cost)
        };
        return (
            eng.to_string(),
            format!(
                "auto: cost medium rows={} bytes={} complexity={} row={:.2} col={:.2} choose={:.2}",
                input_rows, estimated_bytes, complexity, row_cost, col_cost, best
            ),
        );
    }
    (
        "row_v1".to_string(),
        format!(
            "auto: cost small rows={} complexity={} row={:.2} col={:.2}",
            input_rows, complexity, row_cost, col_cost
        ),
    )
}

pub(crate) fn request_prefers_columnar(req: &TransformRowsReq) -> bool {
    if let Some(rules) = req.rules.as_ref() {
        let eng = resolve_transform_engine(rules);
        return eng == "columnar_v1" || eng == "columnar_arrow_v1" || eng == "auto_v1";
    }
    env::var("AIWF_RUST_TRANSFORM_ENGINE")
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "columnar_v1" || t == "columnar_arrow_v1" || t == "auto_v1"
        })
        .unwrap_or(false)
}

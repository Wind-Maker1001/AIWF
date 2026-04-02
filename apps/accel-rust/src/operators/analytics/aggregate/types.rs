use super::helpers::parse_percentile_op;
use super::*;

#[derive(Deserialize)]
pub(crate) struct AggregateRowsReq {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV2Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV3Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
    pub approx_sample_size: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct AggregateRowsResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub stats: Value,
}

#[derive(Deserialize)]
pub(crate) struct AggregateRowsV4Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<Value>,
    pub approx_sample_size: Option<usize>,
    pub verify_exact: Option<bool>,
    pub parallel_workers: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckReq {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV2Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
    pub metrics: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV3Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct QualityCheckV4Req {
    pub run_id: Option<String>,
    pub rows: Vec<Value>,
    pub rules: Value,
    pub rules_dsl: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct QualityCheckResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub run_id: Option<String>,
    pub passed: bool,
    pub report: Value,
}

#[derive(Clone)]
pub(crate) struct AggSpec {
    pub op: String,
    pub field: Option<String>,
    pub as_name: String,
}

#[derive(Default, Clone)]
pub(crate) struct AggBucket {
    pub group_vals: Map<String, Value>,
    pub count: u64,
    pub sums: HashMap<String, f64>,
    pub min: HashMap<String, f64>,
    pub max: HashMap<String, f64>,
}

pub(crate) fn parse_agg_specs(specs: &[Value]) -> Result<Vec<AggSpec>, String> {
    let mut out = Vec::new();
    for s in specs {
        let Some(obj) = s.as_object() else {
            return Err("aggregate spec must be object".to_string());
        };
        let op = obj
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if op.is_empty() {
            return Err("aggregate spec missing op".to_string());
        }
        let field = obj
            .get("field")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let as_name = obj
            .get("as")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| match &field {
                Some(f) => format!("{f}_{op}"),
                None => format!("_{op}"),
            });
        match op.as_str() {
            "count" => {}
            "sum" | "avg" | "min" | "max" | "count_distinct" | "stddev" => {
                if field.is_none() {
                    return Err(format!("aggregate op {op} requires field"));
                }
            }
            _ => {
                if parse_percentile_op(&op).is_none() {
                    return Err(format!("unsupported aggregate op: {op}"));
                }
                if field.is_none() {
                    return Err(format!("aggregate op {op} requires field"));
                }
            }
        }
        out.push(AggSpec { op, field, as_name });
    }
    if out.is_empty() {
        return Err("aggregates is empty".to_string());
    }
    Ok(out)
}

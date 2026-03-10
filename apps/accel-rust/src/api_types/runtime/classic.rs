use super::*;

#[derive(Deserialize)]
pub(crate) struct UdfWasmReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) field: String,
    pub(crate) output_field: String,
    pub(crate) op: Option<String>,
    pub(crate) wasm_base64: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TimeSeriesReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) time_field: String,
    pub(crate) value_field: String,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) window: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct StatsReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) x_field: String,
    pub(crate) y_field: String,
}

#[derive(Deserialize)]
pub(crate) struct EntityLinkReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) field: String,
    pub(crate) id_field: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TableReconstructReq {
    pub(crate) run_id: Option<String>,
    pub(crate) lines: Option<Vec<String>>,
    pub(crate) text: Option<String>,
    pub(crate) delimiter: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct FeatureStoreUpsertReq {
    pub(crate) run_id: Option<String>,
    pub(crate) key_field: String,
    pub(crate) rows: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct FeatureStoreGetReq {
    pub(crate) run_id: Option<String>,
    pub(crate) key: String,
}

#[derive(Deserialize)]
pub(crate) struct RuleSimulatorReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) rules: Value,
    pub(crate) candidate_rules: Value,
}

#[derive(Deserialize)]
pub(crate) struct ConstraintSolverReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) constraints: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct ChartDataPrepReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) category_field: String,
    pub(crate) value_field: String,
    pub(crate) series_field: Option<String>,
    pub(crate) top_n: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct DiffAuditReq {
    pub(crate) run_id: Option<String>,
    pub(crate) left_rows: Vec<Value>,
    pub(crate) right_rows: Vec<Value>,
    pub(crate) keys: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct VectorIndexBuildReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) id_field: String,
    pub(crate) text_field: String,
}

#[derive(Deserialize)]
pub(crate) struct VectorIndexSearchReq {
    pub(crate) run_id: Option<String>,
    pub(crate) query: String,
    pub(crate) top_k: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct EvidenceRankReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) time_field: Option<String>,
    pub(crate) source_field: Option<String>,
    pub(crate) relevance_field: Option<String>,
    pub(crate) consistency_field: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct FactCrosscheckReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) claim_field: String,
    pub(crate) source_field: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TimeSeriesForecastReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) time_field: String,
    pub(crate) value_field: String,
    pub(crate) horizon: Option<usize>,
    pub(crate) method: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct FinanceRatioReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct AnomalyExplainReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) score_field: String,
    pub(crate) threshold: Option<f64>,
}

#[derive(Deserialize)]
pub(crate) struct TemplateBindReq {
    pub(crate) run_id: Option<String>,
    pub(crate) template_text: String,
    pub(crate) data: Value,
}

#[derive(Deserialize)]
pub(crate) struct ProvenanceSignReq {
    pub(crate) run_id: Option<String>,
    pub(crate) payload: Value,
    pub(crate) prev_hash: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct StreamStateSaveReq {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
    pub(crate) state: Value,
    pub(crate) offset: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct StreamStateLoadReq {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
}

#[derive(Deserialize)]
pub(crate) struct QueryLangReq {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) query: String,
}

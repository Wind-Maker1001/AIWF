use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Deserialize)]
pub(crate) struct WindowRowsV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) partition_by: Option<Vec<String>>,
    pub(crate) order_by: String,
    pub(crate) functions: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct OptimizerV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) row_count_hint: Option<usize>,
    pub(crate) prefer_arrow: Option<bool>,
    pub(crate) join_hint: Option<Value>,
    pub(crate) aggregate_hint: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct ParquetIoV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) path: String,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) parquet_mode: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) predicate_field: Option<String>,
    pub(crate) predicate_eq: Option<Value>,
    pub(crate) partition_by: Option<Vec<String>>,
    pub(crate) compression: Option<String>,
    pub(crate) recursive: Option<bool>,
    pub(crate) schema_mode: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct StreamStateV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) stream_key: String,
    pub(crate) state: Option<Value>,
    pub(crate) offset: Option<u64>,
    pub(crate) checkpoint_version: Option<u64>,
    pub(crate) expected_version: Option<u64>,
    pub(crate) backend: Option<String>,
    pub(crate) db_path: Option<String>,
    pub(crate) event_ts_ms: Option<i64>,
    pub(crate) max_late_ms: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct UdfWasmV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) field: String,
    pub(crate) output_field: String,
    pub(crate) op: Option<String>,
    pub(crate) wasm_base64: Option<String>,
    pub(crate) max_output_bytes: Option<usize>,
    pub(crate) signed_token: Option<String>,
    pub(crate) allowed_ops: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub(crate) struct ExplainPlanV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) steps: Vec<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) actual_stats: Option<Vec<Value>>,
    pub(crate) persist_feedback: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct ExplainPlanV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) steps: Vec<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) actual_stats: Option<Vec<Value>>,
    pub(crate) persist_feedback: Option<bool>,
    pub(crate) include_runtime_stats: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct CapabilitiesV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) include_ops: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub(crate) struct IoContractV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) operator: String,
    pub(crate) input: Value,
    pub(crate) strict: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct FailurePolicyV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) operator: Option<String>,
    pub(crate) error: String,
    pub(crate) status_code: Option<u16>,
    pub(crate) attempts: Option<u32>,
    pub(crate) max_retries: Option<u32>,
}

#[derive(Deserialize)]
pub(crate) struct IncrementalPlanV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) operator: String,
    pub(crate) input: Value,
    pub(crate) checkpoint_key: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TenantIsolationV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) tenant_id: Option<String>,
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) max_rows: Option<usize>,
    pub(crate) max_payload_bytes: Option<usize>,
    pub(crate) max_workflow_steps: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct OperatorPolicyV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) tenant_id: Option<String>,
    pub(crate) allow: Option<Vec<String>>,
    pub(crate) deny: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub(crate) struct OptimizerAdaptiveV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) operator: Option<String>,
    pub(crate) row_count_hint: Option<usize>,
    pub(crate) prefer_arrow: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct VectorIndexBuildV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) shard: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) id_field: String,
    pub(crate) text_field: String,
    pub(crate) metadata_fields: Option<Vec<String>>,
    pub(crate) replace: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct VectorIndexSearchV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) query: String,
    pub(crate) top_k: Option<usize>,
    pub(crate) shard: Option<String>,
    pub(crate) filter_eq: Option<Value>,
    pub(crate) rerank_meta_field: Option<String>,
    pub(crate) rerank_meta_weight: Option<f64>,
}

#[derive(Deserialize)]
pub(crate) struct VectorIndexEvalV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) shard: Option<String>,
    pub(crate) top_k: Option<usize>,
    pub(crate) cases: Vec<Value>,
}

#[derive(Deserialize)]
pub(crate) struct StreamReliabilityV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) stream_key: String,
    pub(crate) msg_id: Option<String>,
    pub(crate) row: Option<Value>,
    pub(crate) error: Option<String>,
    pub(crate) checkpoint: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct LineageProvenanceV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rules: Option<Value>,
    pub(crate) computed_fields_v3: Option<Vec<Value>>,
    pub(crate) workflow_steps: Option<Vec<Value>>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) payload: Option<Value>,
    pub(crate) prev_hash: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct ContractRegressionV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) operators: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub(crate) struct PerfBaselineV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) operator: Option<String>,
    pub(crate) p95_ms: Option<u128>,
    pub(crate) max_p95_ms: Option<u128>,
}

#[derive(Deserialize)]
pub(crate) struct StreamWindowV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) event_time_field: String,
    pub(crate) window_ms: u64,
    pub(crate) watermark_ms: Option<u64>,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) value_field: Option<String>,
    pub(crate) trigger: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct StreamWindowV2Req {
    pub(crate) run_id: Option<String>,
    pub(crate) stream_key: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) event_time_field: String,
    pub(crate) window_type: Option<String>,
    pub(crate) window_ms: u64,
    pub(crate) slide_ms: Option<u64>,
    pub(crate) session_gap_ms: Option<u64>,
    pub(crate) watermark_ms: Option<u64>,
    pub(crate) allowed_lateness_ms: Option<u64>,
    pub(crate) group_by: Option<Vec<String>>,
    pub(crate) value_field: Option<String>,
    pub(crate) trigger: Option<String>,
    pub(crate) emit_late_side: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct ColumnarEvalV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) select_fields: Option<Vec<String>>,
    pub(crate) filter_eq: Option<Value>,
    pub(crate) limit: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct SketchV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) kind: Option<String>,
    pub(crate) state: Option<Value>,
    pub(crate) rows: Option<Vec<Value>>,
    pub(crate) field: Option<String>,
    pub(crate) topk_n: Option<usize>,
    pub(crate) merge_state: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct RuntimeStatsV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) op: String,
    pub(crate) operator: Option<String>,
    pub(crate) ok: Option<bool>,
    pub(crate) error_code: Option<String>,
    pub(crate) duration_ms: Option<u128>,
    pub(crate) rows_in: Option<usize>,
    pub(crate) rows_out: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct AggregatePushdownReq {
    pub(crate) run_id: Option<String>,
    pub(crate) source_type: String,
    pub(crate) source: String,
    pub(crate) from: Option<String>,
    pub(crate) group_by: Vec<String>,
    pub(crate) aggregates: Vec<Value>,
    pub(crate) where_sql: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct AggregatePushdownResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) sql: String,
    pub(crate) rows: Vec<Value>,
    pub(crate) stats: Value,
}

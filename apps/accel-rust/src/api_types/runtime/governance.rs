use super::*;

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

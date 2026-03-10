use crate::{
    api_types::{
        ContractRegressionV1Req, ErrResp, ExplainPlanV1Req, ExplainPlanV2Req,
        LineageProvenanceV1Req, OptimizerV1Req, ParquetIoV2Req, PerfBaselineV1Req, SaveRowsReq,
        StreamReliabilityV1Req, StreamStateV2Req, TransformRowsStreamReq, UdfWasmV2Req,
        VectorIndexBuildV2Req, VectorIndexEvalV2Req, VectorIndexSearchV2Req, WindowRowsV1Req,
    },
    execution_ops::{
        run_explain_plan_v1, run_optimizer_v1, run_parquet_io_v2, run_save_rows_v1,
        run_stream_state_v2, run_transform_rows_v2_stream, run_udf_wasm_v2, run_window_rows_v1,
    },
    governance_ops::{
        run_contract_regression_v1, run_explain_plan_v2, run_lineage_provenance_v1,
        run_perf_baseline_v1, run_stream_reliability_v1, run_vector_index_build_v2,
        run_vector_index_eval_v2, run_vector_index_search_v2,
    },
    operators::{
        analytics::{
            AggregateRowsV4Req, QualityCheckV4Req, run_aggregate_rows_v4, run_quality_check_v4,
        },
        join::JoinRowsV4Req,
        join::run_join_rows_v4,
        workflow::{LineageV3Req, WorkflowRunReq, run_lineage_v3},
    },
    transform_support::{enforce_tenant_payload_quota, tenant_max_workflow_steps_for},
};
use accel_rust::app_state::AppState;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};

#[path = "advanced/governance.rs"]
mod governance;
pub(crate) use governance::{
    contract_regression_v1_operator, lineage_provenance_v1_operator, perf_baseline_v1_operator,
    stream_reliability_v1_operator, vector_index_build_v2_operator, vector_index_eval_v2_operator,
    vector_index_search_v2_operator,
};

#[path = "advanced/compute.rs"]
mod compute;
pub(crate) use compute::{
    aggregate_rows_v4_operator, explain_plan_v1_operator, explain_plan_v2_operator,
    join_rows_v4_operator, lineage_v3_operator, optimizer_v1_operator, parquet_io_v2_operator,
    quality_check_v4_operator, stream_state_v2_operator, udf_wasm_v2_operator,
    window_rows_v1_operator,
};

#[path = "advanced/workflow.rs"]
mod workflow;
pub(crate) use workflow::{
    save_rows_v1_operator, transform_rows_v2_stream_operator, workflow_run_operator,
};

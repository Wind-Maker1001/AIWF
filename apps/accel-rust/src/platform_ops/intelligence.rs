use crate::{
    analysis_ops::parse_time_order_key,
    api_types::{
        AnomalyExplainReq, EvidenceRankReq, FactCrosscheckReq, FinanceRatioReq, ProvenanceSignReq,
        TemplateBindReq, TimeSeriesForecastReq, VectorIndexBuildReq, VectorIndexSearchReq,
    },
    platform_ops::storage::{load_kv_store, save_kv_store, vector_index_store_path},
    transform_support::{
        unix_now_sec, utc_now_iso, value_to_f64, value_to_string, value_to_string_or_null,
    },
};
use regex::Regex;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

mod vector;
pub(crate) use vector::{run_vector_index_build_v1, run_vector_index_search_v1};

mod reasoning;
pub(crate) use reasoning::{run_evidence_rank_v1, run_fact_crosscheck_v1};

mod finance;
pub(crate) use finance::{
    run_anomaly_explain_v1, run_finance_ratio_v1, run_timeseries_forecast_v1,
};

mod template;
pub(crate) use template::{run_provenance_sign_v1, run_template_bind_v1};

use crate::{
    operators::transform::TransformRowsReq,
    platform_ops::{load_kv_store, operator_policy_store_path, tenant_isolation_store_path},
};
use accel_rust::app_state::{AppState, ServiceMetrics};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

mod common;
pub(crate) use common::{can_cancel_status, collapse_ws};

mod tenant;
pub(crate) use tenant::{
    enforce_tenant_payload_quota, operator_allowed_for_tenant, release_tenant_slot,
    tenant_max_concurrency, tenant_max_payload_bytes, tenant_max_rows, tenant_max_workflow_steps_for,
    try_acquire_tenant_slot,
};

mod trace;
pub(crate) use trace::{
    cleanup_task_flag, is_cancelled, resolve_trace_id, verify_request_signature,
};

mod sql;
pub(crate) use sql::{validate_readonly_query, validate_sql_identifier, validate_where_clause};

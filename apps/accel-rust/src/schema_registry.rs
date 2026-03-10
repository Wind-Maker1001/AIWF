use crate::{
    api_types::{
        SchemaCompatReq, SchemaCompatResp, SchemaGetReq, SchemaInferReq, SchemaInferResp,
        SchemaMigrationSuggestReq, SchemaMigrationSuggestResp, SchemaRegisterReq,
        SchemaRegistryResp,
    },
    misc_ops::safe_pkg_token,
};
use accel_rust::{
    app_state::AppState,
    metrics::{acquire_file_lock, release_file_lock},
};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
};

mod store;
#[cfg(test)]
pub(crate) use store::load_schema_registry_store;
pub(crate) use store::{
    run_schema_registry_get_local, run_schema_registry_infer_local,
    run_schema_registry_register_local, schema_registry_key,
};

mod ops;
pub(crate) use ops::{
    run_schema_registry_get_v1, run_schema_registry_infer_v1, run_schema_registry_register_v1,
};

mod compat;
pub(crate) use compat::{
    run_schema_registry_check_compat_v2, run_schema_registry_suggest_migration_v2,
};

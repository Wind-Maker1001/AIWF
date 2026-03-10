use crate::app_state::{TaskState, TaskStoreConfig};
use odbc_api::{ConnectionOptions, Cursor, Environment, IntoParameter, buffers::TextRowSet};
use serde_json::{Value, json};
use std::{collections::HashMap, env, fs, path::PathBuf, process::Command};

mod base_api;
mod config;
mod local;
mod odbc;
mod shared;
mod sqlcmd;

use self::{base_api::*, odbc::*, shared::*, sqlcmd::*};

pub use config::{
    parse_sqlserver_conn_str, resolve_task_store_backend, task_store_config_from_env,
};
pub use local::{load_tasks_from_store, persist_tasks_to_store, prune_tasks};
pub use sqlcmd::{escape_tsql, run_sqlcmd_query};

pub fn task_store_remote_enabled(cfg: &TaskStoreConfig) -> bool {
    if !cfg.remote_enabled {
        return false;
    }
    match cfg.backend.as_str() {
        "odbc" => true,
        "sqlcmd" => true,
        _ => cfg
            .base_api_url
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty()),
    }
}

pub fn probe_remote_task_store(cfg: &TaskStoreConfig) -> bool {
    match cfg.backend.as_str() {
        "odbc" => odbc_probe_task_store(cfg),
        "sqlcmd" => sqlcmd_probe_task_store(cfg),
        _ => base_api_probe_task_store(cfg),
    }
}

pub fn task_store_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    if !task_store_remote_enabled(cfg) {
        return;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_upsert_task(task, cfg),
        "sqlcmd" => sqlcmd_upsert_task(task, cfg),
        _ => base_api_upsert_task(task, cfg),
    }
}

pub fn task_store_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    if !task_store_remote_enabled(cfg) {
        return None;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_get_task(task_id, cfg),
        "sqlcmd" => sqlcmd_get_task(task_id, cfg),
        _ => base_api_get_task(task_id, cfg),
    }
}

pub fn task_store_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    if !task_store_remote_enabled(cfg) {
        return None;
    }
    match cfg.backend.as_str() {
        "odbc" => odbc_cancel_task(task_id, cfg),
        "sqlcmd" => sqlcmd_cancel_task(task_id, cfg),
        _ => base_api_cancel_task(task_id, cfg),
    }
}

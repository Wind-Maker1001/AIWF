use crate::{
    api_types::{
        PluginExecReq, PluginExecResp, PluginManifest, PluginOperatorV1Req, PluginRegistryV1Req,
    },
    load_kv_store, resolve_trace_id, safe_pkg_token, save_kv_store, utc_now_iso,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

mod paths;
pub(crate) use paths::{
    append_plugin_audit, load_plugin_registry_store, plugin_dir, plugin_runtime_store_path,
    plugin_tenant_running_map, save_plugin_registry_store,
};

mod registry;
pub(crate) use registry::run_plugin_registry_v1;

mod operator;
pub(crate) use operator::run_plugin_operator_v1;

mod manifest;
pub(crate) use manifest::{load_plugin_manifest, run_plugin_healthcheck};

mod policy;
pub(crate) use policy::{
    enforce_plugin_allowlist, enforce_plugin_command_allowlist, plugin_enabled_for_tenant,
    read_pipe_capped, verify_plugin_signature,
};

mod exec;
pub(crate) use exec::run_plugin_exec_v1;

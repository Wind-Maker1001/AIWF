use crate::app_state::{TaskState, TaskStoreConfig};
use odbc_api::{ConnectionOptions, Cursor, Environment, IntoParameter, buffers::TextRowSet};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    env, fs,
    path::PathBuf,
    process::Command,
};

pub fn task_store_config_from_env() -> TaskStoreConfig {
    let ttl_sec = env::var("AIWF_RUST_TASK_TTL_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24 * 60 * 60);
    let max_tasks = env::var("AIWF_RUST_TASK_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let store_path = env::var("AIWF_RUST_TASK_STORE_PATH").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(PathBuf::from(t))
        }
    });
    let base_api_url = env::var("AIWF_BASE_URL").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let base_api_key = env::var("AIWF_API_KEY").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let remote_enabled = env::var("AIWF_RUST_TASK_STORE_REMOTE")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    let backend = env::var("AIWF_RUST_TASK_STORE_BACKEND")
        .unwrap_or_else(|_| "base_api".to_string())
        .trim()
        .to_lowercase();
    let sql_host = env::var("AIWF_SQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let sql_port = env::var("AIWF_SQL_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(1433);
    let sql_db = env::var("AIWF_SQL_DB").unwrap_or_else(|_| "AIWF".to_string());
    let sql_user = env::var("AIWF_SQL_USER").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let sql_password = env::var("AIWF_SQL_PASSWORD").ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });
    let sql_use_windows_auth = env::var("AIWF_SQL_USE_WINDOWS_AUTH")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");
    TaskStoreConfig {
        ttl_sec,
        max_tasks,
        store_path,
        remote_enabled,
        backend,
        base_api_url,
        base_api_key,
        sql_host,
        sql_port,
        sql_db,
        sql_user,
        sql_password,
        sql_use_windows_auth,
    }
}

pub fn resolve_task_store_backend(mut cfg: TaskStoreConfig) -> TaskStoreConfig {
    if !cfg.remote_enabled {
        return cfg;
    }
    let backend = cfg.backend.clone();
    let mut cands: Vec<&str> = Vec::new();
    match backend.as_str() {
        "odbc" => cands.extend(["odbc", "sqlcmd", "base_api"]),
        "sqlcmd" => cands.extend(["sqlcmd", "odbc", "base_api"]),
        "base_api" => cands.extend(["base_api", "sqlcmd", "odbc"]),
        _ => cands.extend(["base_api", "sqlcmd", "odbc"]),
    }
    for b in cands {
        let mut probe_cfg = cfg.clone();
        probe_cfg.backend = b.to_string();
        if b == "sqlcmd" && !is_sqlcmd_available() {
            continue;
        }
        if b == "base_api"
            && probe_cfg
                .base_api_url
                .as_ref()
                .is_none_or(|u| u.trim().is_empty())
        {
            continue;
        }
        if probe_remote_task_store(&probe_cfg) {
            cfg.backend = b.to_string();
            return cfg;
        }
    }
    cfg.remote_enabled = false;
    cfg
}

pub fn load_tasks_from_store(path: Option<&PathBuf>) -> HashMap<String, TaskState> {
    let Some(p) = path else {
        return HashMap::new();
    };
    let Ok(bytes) = fs::read(p) else {
        return HashMap::new();
    };
    let mut out: HashMap<String, TaskState> = serde_json::from_slice(&bytes).unwrap_or_default();
    let cfg = task_store_config_from_env();
    let _ = prune_tasks(&mut out, &cfg);
    out
}

pub fn persist_tasks_to_store(tasks: &HashMap<String, TaskState>, path: Option<&PathBuf>) {
    let Some(p) = path else {
        return;
    };
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(buf) = serde_json::to_vec_pretty(tasks) {
        let _ = fs::write(p, buf);
    }
}

pub fn prune_tasks(tasks: &mut HashMap<String, TaskState>, cfg: &TaskStoreConfig) -> usize {
    if tasks.is_empty() {
        return 0;
    }
    let now = utc_now_epoch_string().parse::<u64>().unwrap_or(0);
    let mut removed = 0usize;
    if cfg.ttl_sec > 0 && now > 0 {
        let before = tasks.len();
        tasks.retain(|_, t| now.saturating_sub(task_epoch(t)) <= cfg.ttl_sec);
        removed += before.saturating_sub(tasks.len());
    }

    if cfg.max_tasks > 0 && tasks.len() > cfg.max_tasks {
        let mut ids = tasks
            .iter()
            .map(|(k, t)| (k.clone(), task_epoch(t)))
            .collect::<Vec<_>>();
        ids.sort_by_key(|(_, ts)| *ts);
        let drop_n = tasks.len().saturating_sub(cfg.max_tasks);
        for (id, _) in ids.into_iter().take(drop_n) {
            if tasks.remove(&id).is_some() {
                removed += 1;
            }
        }
    }
    removed
}

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

pub fn run_sqlcmd_query(cfg: &TaskStoreConfig, query: &str) -> Result<String, String> {
    let mut cmd = Command::new("sqlcmd");
    cmd.arg("-S")
        .arg(format!("{},{}", cfg.sql_host, cfg.sql_port))
        .arg("-d")
        .arg(cfg.sql_db.clone())
        .arg("-W")
        .arg("-h")
        .arg("-1")
        .arg("-Q")
        .arg(query);

    if cfg.sql_use_windows_auth {
        cmd.arg("-E");
    } else {
        let user = cfg.sql_user.clone().unwrap_or_default();
        let pwd = cfg.sql_password.clone().unwrap_or_default();
        cmd.arg("-U").arg(user).arg("-P").arg(pwd);
    }
    let out = cmd.output().map_err(|e| format!("run sqlcmd: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "sqlcmd failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

pub fn escape_tsql(s: &str) -> String {
    s.replace('\'', "''")
}

pub fn parse_sqlserver_conn_str(s: &str) -> TaskStoreConfig {
    let mut cfg = task_store_config_from_env();
    let trimmed = s.trim();
    let (main, query) = trimmed.split_once('?').unwrap_or((trimmed, ""));
    let (host_port, db) = main.split_once('/').unwrap_or((main, "AIWF"));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "1433"));
    cfg.sql_host = host.to_string();
    cfg.sql_port = port.parse::<u16>().unwrap_or(1433);
    cfg.sql_db = db.to_string();
    let params = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .collect::<HashMap<_, _>>();
    if let Some(u) = params.get("user") {
        cfg.sql_user = Some((*u).to_string());
    }
    if let Some(p) = params.get("password") {
        cfg.sql_password = Some((*p).to_string());
    }
    cfg.sql_use_windows_auth = params
        .get("windows_auth")
        .map(|v| *v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    cfg
}

fn is_sqlcmd_available() -> bool {
    if cfg!(windows) {
        if let Ok(out) = Command::new("where").arg("sqlcmd").output() {
            return out.status.success();
        }
        false
    } else {
        if let Ok(out) = Command::new("which").arg("sqlcmd").output() {
            return out.status.success();
        }
        false
    }
}

fn task_epoch(task: &TaskState) -> u64 {
    task.updated_at
        .parse::<u64>()
        .ok()
        .or_else(|| task.created_at.parse::<u64>().ok())
        .unwrap_or(0)
}

fn utc_now_epoch_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{ts}")
}

fn base_api_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let Some(base) = &cfg.base_api_url else {
        return false;
    };
    let url = format!("{}/actuator/health", base.trim_end_matches('/'));
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let Ok(resp) = req.call() else {
        return false;
    };
    let Ok(v) = resp.into_json::<Value>() else {
        return false;
    };
    v.get("status")
        .and_then(|x| x.as_str())
        .map(|s| s.eq_ignore_ascii_case("UP"))
        .unwrap_or(false)
}

fn base_api_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let Some(base) = &cfg.base_api_url else {
        return;
    };
    let url = format!("{}/api/v1/runtime/tasks/upsert", base.trim_end_matches('/'));
    let payload = json!({
        "task_id": task.task_id,
        "tenant_id": task.tenant_id,
        "operator": task.operator,
        "status": task.status,
        "created_at": task.created_at.parse::<u64>().unwrap_or(0),
        "updated_at": task.updated_at.parse::<u64>().unwrap_or(0),
        "result": task.result.clone(),
        "error": task.error.clone(),
        "idempotency_key": task.idempotency_key,
        "attempts": task.attempts,
        "source": "accel-rust"
    });
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let _ = req.send_json(payload);
}

fn base_api_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::get(&url);
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.call().ok()?;
    let body: Value = resp.into_json().ok()?;
    let task = body.get("task")?;
    parse_task_from_runtime_row(task)
}

fn base_api_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let Some(base) = &cfg.base_api_url else {
        return None;
    };
    let url = format!(
        "{}/api/v1/runtime/tasks/{}/cancel",
        base.trim_end_matches('/'),
        task_id
    );
    let mut req = ureq::post(&url).set("Content-Type", "application/json");
    if let Some(k) = &cfg.base_api_key {
        req = req.set("X-API-Key", k);
    }
    let resp = req.send_string("{}").ok()?;
    resp.into_json().ok()
}

fn parse_task_from_runtime_row(task: &Value) -> Option<TaskState> {
    let task_id = task.get("task_id")?.as_str()?.to_string();
    let tenant_id = task
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let operator = task
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("transform_rows_v2")
        .to_string();
    let status = task
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("queued")
        .to_string();
    let created_at = task
        .get("created_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let updated_at = task
        .get("updated_at_epoch")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .to_string();
    let result = task
        .get("result_json")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok());
    let error = task
        .get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let idempotency_key = task
        .get("idempotency_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let attempts = task.get("attempts").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    Some(TaskState {
        task_id,
        tenant_id,
        operator,
        status,
        created_at,
        updated_at,
        result,
        error,
        idempotency_key,
        attempts,
    })
}

fn odbc_conn_str(cfg: &TaskStoreConfig) -> String {
    if cfg.sql_use_windows_auth {
        format!(
            "Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{},{};Database={};Trusted_Connection=Yes;Encrypt=no;TrustServerCertificate=yes;",
            cfg.sql_host, cfg.sql_port, cfg.sql_db
        )
    } else {
        format!(
            "Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{},{};Database={};Uid={};Pwd={};Encrypt=no;TrustServerCertificate=yes;",
            cfg.sql_host,
            cfg.sql_port,
            cfg.sql_db,
            cfg.sql_user.clone().unwrap_or_default(),
            cfg.sql_password.clone().unwrap_or_default()
        )
    }
}

fn run_odbc_query_first_text(
    cfg: &TaskStoreConfig,
    query: &str,
    params: &[String],
) -> Result<Option<String>, String> {
    let env = Environment::new().map_err(|e| format!("odbc env: {e}"))?;
    let conn = env
        .connect_with_connection_string(&odbc_conn_str(cfg), ConnectionOptions::default())
        .map_err(|e| format!("odbc connect: {e}"))?;
    let param_buf: Vec<_> = params.iter().map(|p| p.as_str().into_parameter()).collect();
    let maybe_cursor = conn
        .execute(query, param_buf.as_slice())
        .map_err(|e| format!("odbc execute: {e}"))?;
    let Some(mut cursor) = maybe_cursor else {
        return Ok(None);
    };
    let buffers = TextRowSet::for_cursor(8, &mut cursor, Some(16384))
        .map_err(|e| format!("odbc buffer: {e}"))?;
    let mut row_set_cursor = cursor
        .bind_buffer(buffers)
        .map_err(|e| format!("odbc bind: {e}"))?;
    if let Some(batch) = row_set_cursor
        .fetch()
        .map_err(|e| format!("odbc fetch: {e}"))?
        && batch.num_rows() > 0
        && let Some(txt) = batch.at(0, 0)
    {
        return Ok(Some(String::from_utf8_lossy(txt).to_string()));
    }
    Ok(None)
}

fn run_odbc_exec(cfg: &TaskStoreConfig, query: &str, params: &[String]) -> Result<(), String> {
    let env = Environment::new().map_err(|e| format!("odbc env: {e}"))?;
    let conn = env
        .connect_with_connection_string(&odbc_conn_str(cfg), ConnectionOptions::default())
        .map_err(|e| format!("odbc connect: {e}"))?;
    let param_buf: Vec<_> = params.iter().map(|p| p.as_str().into_parameter()).collect();
    let _ = conn
        .execute(query, param_buf.as_slice())
        .map_err(|e| format!("odbc execute: {e}"))?;
    Ok(())
}

fn odbc_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN N'0' ELSE N'1' END;";
    match run_odbc_query_first_text(cfg, q, &[]) {
        Ok(Some(v)) => v.trim() == "1",
        _ => false,
    }
}

fn odbc_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let task_id = task.task_id.clone();
    let tenant_id = task.tenant_id.clone();
    let operator = task.operator.clone();
    let status = task.status.clone();
    let created = task.created_at.parse::<u64>().unwrap_or(0).to_string();
    let updated = task.updated_at.parse::<u64>().unwrap_or(0).to_string();
    let result_json = task
        .result
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let error = task.error.clone().unwrap_or_default();
    let source = "accel-rust".to_string();
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
DECLARE @tenant_id NVARCHAR(128)=?;\
DECLARE @operator NVARCHAR(128)=?;\
DECLARE @status NVARCHAR(64)=?;\
DECLARE @created_at_epoch BIGINT=CAST(? AS BIGINT);\
DECLARE @updated_at_epoch BIGINT=CAST(? AS BIGINT);\
DECLARE @result_json NVARCHAR(MAX)=?;\
DECLARE @error NVARCHAR(MAX)=?;\
DECLARE @source NVARCHAR(64)=?;\
IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=@task_id)\
BEGIN\
  UPDATE dbo.workflow_tasks\
  SET tenant_id=@tenant_id,operator=@operator,status=@status,\
      created_at_epoch=@created_at_epoch,updated_at_epoch=@updated_at_epoch,\
      result_json=@result_json,error=@error,source=@source\
  WHERE task_id=@task_id;\
END\
ELSE\
BEGIN\
  INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source)\
  VALUES (@task_id,@tenant_id,@operator,@status,@created_at_epoch,@updated_at_epoch,@result_json,@error,@source);\
END";
    let params = vec![
        task_id,
        tenant_id,
        operator,
        status,
        created,
        updated,
        result_json,
        error,
        source,
    ];
    let _ = run_odbc_exec(cfg, q, &params);
}

fn odbc_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
SELECT TOP 1 task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error\
FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = run_odbc_query_first_text(cfg, q, &[task_id.to_string()]).ok()??;
    let s = out.trim();
    if s.is_empty() {
        return None;
    }
    let row: Value = serde_json::from_str(s).ok()?;
    parse_task_from_runtime_row(&row)
}

fn odbc_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let now = utc_now_epoch_string().parse::<u64>().unwrap_or(0).to_string();
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
DECLARE @now BIGINT=CAST(? AS BIGINT);\
UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch=@now\
WHERE task_id=@task_id AND status IN (N'queued',N'running');\
SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = run_odbc_query_first_text(cfg, q, &[task_id.to_string(), now]).ok()??;
    let row: Value = serde_json::from_str(out.trim()).ok()?;
    let status = row
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some(json!({
        "ok": true,
        "task_id": row.get("task_id").and_then(|v| v.as_str()).unwrap_or(""),
        "cancelled": status == "cancelled",
        "status": status
    }))
}

fn sqlcmd_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN 0 ELSE 1 END AS ok_flag;";
    let Ok(out) = run_sqlcmd_query(cfg, q) else {
        return false;
    };
    out.trim().ends_with('1')
}

fn sqlcmd_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
    let task_id = escape_tsql(&task.task_id);
    let tenant_id = escape_tsql(&task.tenant_id);
    let operator = escape_tsql(&task.operator);
    let status = escape_tsql(&task.status);
    let created = task.created_at.parse::<u64>().unwrap_or(0);
    let updated = task.updated_at.parse::<u64>().unwrap_or(0);
    let result_json = task
        .result
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let result_json = escape_tsql(&result_json);
    let error = escape_tsql(task.error.as_deref().unwrap_or(""));
    let q = format!(
        "SET NOCOUNT ON; IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=N'{task_id}') BEGIN UPDATE dbo.workflow_tasks SET tenant_id=N'{tenant_id}',operator=N'{operator}',status=N'{status}',created_at_epoch={created},updated_at_epoch={updated},result_json=N'{result_json}',error=N'{error}',source=N'accel-rust' WHERE task_id=N'{task_id}'; END ELSE BEGIN INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source) VALUES (N'{task_id}',N'{tenant_id}',N'{operator}',N'{status}',{created},{updated},N'{result_json}',N'{error}',N'accel-rust'); END"
    );
    let _ = run_sqlcmd_query(cfg, &q);
}

fn sqlcmd_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let task_id = escape_tsql(task_id);
    let q = format!(
        "SET NOCOUNT ON; SELECT TOP 1 task_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    );
    let out = run_sqlcmd_query(cfg, &q).ok()?;
    let s = out.trim();
    if s.is_empty() {
        return None;
    }
    let row: Value = serde_json::from_str(s).ok()?;
    parse_task_from_runtime_row(&row)
}

fn sqlcmd_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
    let task_id = escape_tsql(task_id);
    let now = utc_now_epoch_string().parse::<u64>().unwrap_or(0);
    let q = format!(
        "SET NOCOUNT ON; UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch={now} WHERE task_id=N'{task_id}' AND status IN (N'queued',N'running'); SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    );
    let out = run_sqlcmd_query(cfg, &q).ok()?;
    let row: Value = serde_json::from_str(out.trim()).ok()?;
    let status = row
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some(json!({
        "ok": true,
        "task_id": row.get("task_id").and_then(|v| v.as_str()).unwrap_or(""),
        "cancelled": status == "cancelled",
        "status": status
    }))
}

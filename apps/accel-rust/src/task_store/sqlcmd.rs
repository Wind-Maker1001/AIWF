use super::*;

fn write_sqlcmd_input_file(query: &str) -> Result<PathBuf, String> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("sqlcmd temp file clock error: {e}"))?
        .as_nanos();
    let path = env::temp_dir().join(format!(
        "aiwf_sqlcmd_{}_{}.sql",
        std::process::id(),
        stamp
    ));
    let mut bytes = Vec::with_capacity(query.len() + 3);
    bytes.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
    bytes.extend_from_slice(query.as_bytes());
    fs::write(&path, bytes).map_err(|e| format!("sqlcmd temp file write: {e}"))?;
    Ok(path)
}

fn normalize_sqlcmd_output(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .replace("\\\r\n", "\\")
        .replace("\\\n", "\\")
        .replace('\r', "")
        .replace('\n', "")
}

fn sqlcmd_upsert_task_query(
    task_id: &str,
    tenant_id: &str,
    operator: &str,
    status: &str,
    created: u64,
    updated: u64,
    result_json: &str,
    error: &str,
    idempotency_key: &str,
    attempts: u32,
) -> String {
    format!(
        "SET NOCOUNT ON; IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=N'{task_id}') BEGIN UPDATE dbo.workflow_tasks SET tenant_id=N'{tenant_id}',operator=N'{operator}',status=N'{status}',updated_at_epoch={updated},result_json=N'{result_json}',error=N'{error}',source=N'accel-rust',idempotency_key=N'{idempotency_key}',attempts={attempts} WHERE task_id=N'{task_id}'; END ELSE BEGIN INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source,idempotency_key,attempts) VALUES (N'{task_id}',N'{tenant_id}',N'{operator}',N'{status}',{created},{updated},N'{result_json}',N'{error}',N'accel-rust',N'{idempotency_key}',{attempts}); END"
    )
}

pub fn run_sqlcmd_query(cfg: &TaskStoreConfig, query: &str) -> Result<String, String> {
    let query_file = write_sqlcmd_input_file(query)?;
    let mut cmd = Command::new("sqlcmd");
    cmd.arg("-S")
        .arg(format!("{},{}", cfg.sql_host, cfg.sql_port))
        .arg("-d")
        .arg(cfg.sql_db.clone())
        .arg("-b")
        .arg("-W")
        .arg("-h")
        .arg("-1")
        .arg("-y")
        .arg("0")
        .arg("-Y")
        .arg("0")
        .arg("-w")
        .arg("65535")
        .arg("-i")
        .arg(&query_file);

    if cfg.sql_use_windows_auth {
        cmd.arg("-E");
    } else {
        let user = cfg.sql_user.clone().unwrap_or_default();
        let pwd = cfg.sql_password.clone().unwrap_or_default();
        cmd.arg("-U").arg(user).arg("-P").arg(pwd);
    }
    let out = cmd.output().map_err(|e| format!("run sqlcmd: {e}"))?;
    let _ = fs::remove_file(&query_file);
    if !out.status.success() {
        let stderr = normalize_sqlcmd_output(&out.stderr).trim().to_string();
        let stdout = normalize_sqlcmd_output(&out.stdout).trim().to_string();
        let detail = if !stderr.is_empty() && !stdout.is_empty() {
            format!("{stderr} | stdout: {stdout}")
        } else if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit status {}", out.status)
        };
        return Err(format!(
            "sqlcmd failed: {}",
            detail
        ));
    }
    Ok(normalize_sqlcmd_output(&out.stdout))
}

pub fn escape_tsql(s: &str) -> String {
    s.replace('\'', "''")
}

pub(super) fn sqlcmd_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN 0 ELSE 1 END AS ok_flag;";
    let Ok(out) = run_sqlcmd_query(cfg, q) else {
        return false;
    };
    out.trim().ends_with('1')
}

pub(super) fn sqlcmd_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) -> Result<(), String> {
    let task_id = escape_tsql(&task.task_id);
    let tenant_id = escape_tsql(&task.tenant_id);
    let operator = escape_tsql(&task.operator);
    let status = escape_tsql(&task.status);
    let created = task_time_epoch(&task.created_at).unwrap_or(0);
    let updated = task_time_epoch(&task.updated_at)
        .unwrap_or_else(|| task_time_epoch(&task.created_at).unwrap_or(0));
    let result_json = task
        .result
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let result_json = escape_tsql(&result_json);
    let error = escape_tsql(task.error.as_deref().unwrap_or(""));
    let idempotency_key = escape_tsql(&task.idempotency_key);
    let attempts = task.attempts;
    let q = sqlcmd_upsert_task_query(
        &task_id,
        &tenant_id,
        &operator,
        &status,
        created,
        updated,
        &result_json,
        &error,
        &idempotency_key,
        attempts,
    );
    run_sqlcmd_query(cfg, &q).map(|_| ())
}

pub(super) fn sqlcmd_get_task_query(task_id: &str) -> String {
    let task_id = escape_tsql(task_id);
    format!(
        "SET NOCOUNT ON; SELECT TOP 1 task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,idempotency_key,attempts FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    )
}

pub(super) fn sqlcmd_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<TaskState>, String> {
    let q = sqlcmd_get_task_query(task_id);
    let out = run_sqlcmd_query(cfg, &q)?;
    let s = out.trim();
    if s.is_empty() {
        return Ok(None);
    }
    let row: Value =
        serde_json::from_str(s).map_err(|e| format!("sqlcmd get task json parse: {e}"))?;
    parse_task_from_runtime_row(&row)
        .map(Some)
        .ok_or_else(|| "sqlcmd get task: invalid task payload".to_string())
}

pub(super) fn sqlcmd_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<Value>, String> {
    let task_id = escape_tsql(task_id);
    let now = utc_now_epoch_string().parse::<u64>().unwrap_or(0);
    let q = format!(
        "SET NOCOUNT ON; UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch={now} WHERE task_id=N'{task_id}' AND status IN (N'queued',N'running'); SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    );
    let out = run_sqlcmd_query(cfg, &q)?;
    let trimmed = out.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let row: Value =
        serde_json::from_str(trimmed).map_err(|e| format!("sqlcmd cancel task json parse: {e}"))?;
    let status = row
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Ok(Some(json!({
        "ok": true,
        "task_id": row.get("task_id").and_then(|v| v.as_str()).unwrap_or(""),
        "cancelled": status == "cancelled",
        "status": status
    })))
}

#[cfg(test)]
mod tests {
    use super::{sqlcmd_get_task_query, sqlcmd_upsert_task_query};

    #[test]
    fn sqlcmd_get_task_query_keeps_tenant_id() {
        let query = sqlcmd_get_task_query("task-123");
        assert!(query.contains("tenant_id"));
        assert!(query.contains("idempotency_key"));
        assert!(query.contains("attempts"));
        assert!(query.contains("WHERE task_id=N'task-123'"));
    }

    #[test]
    fn sqlcmd_upsert_query_preserves_created_at_on_update() {
        let query = sqlcmd_upsert_task_query(
            "task-123",
            "tenant-a",
            "transform_rows_v2",
            "running",
            111,
            222,
            "{}",
            "",
            "idem-1",
            3,
        );
        assert!(query.contains("UPDATE dbo.workflow_tasks SET tenant_id=N'tenant-a'"));
        assert!(!query.contains("UPDATE dbo.workflow_tasks SET tenant_id=N'tenant-a',operator=N'transform_rows_v2',status=N'running',created_at_epoch=111"));
        assert!(query.contains("VALUES (N'task-123',N'tenant-a',N'transform_rows_v2',N'running',111,222"));
    }
}

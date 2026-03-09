use super::*;

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

pub(super) fn sqlcmd_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN 0 ELSE 1 END AS ok_flag;";
    let Ok(out) = run_sqlcmd_query(cfg, q) else {
        return false;
    };
    out.trim().ends_with('1')
}

pub(super) fn sqlcmd_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) {
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

pub(super) fn sqlcmd_get_task_query(task_id: &str) -> String {
    let task_id = escape_tsql(task_id);
    format!(
        "SET NOCOUNT ON; SELECT TOP 1 task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error FROM dbo.workflow_tasks WHERE task_id=N'{task_id}' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;"
    )
}

pub(super) fn sqlcmd_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<TaskState> {
    let q = sqlcmd_get_task_query(task_id);
    let out = run_sqlcmd_query(cfg, &q).ok()?;
    let s = out.trim();
    if s.is_empty() {
        return None;
    }
    let row: Value = serde_json::from_str(s).ok()?;
    parse_task_from_runtime_row(&row)
}

pub(super) fn sqlcmd_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Option<Value> {
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

#[cfg(test)]
mod tests {
    use super::sqlcmd_get_task_query;

    #[test]
    fn sqlcmd_get_task_query_keeps_tenant_id() {
        let query = sqlcmd_get_task_query("task-123");
        assert!(query.contains("tenant_id"));
        assert!(query.contains("WHERE task_id=N'task-123'"));
    }
}

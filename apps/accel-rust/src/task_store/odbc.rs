use super::*;

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

pub(super) fn odbc_probe_task_store(cfg: &TaskStoreConfig) -> bool {
    let q = "SET NOCOUNT ON; SELECT CASE WHEN OBJECT_ID('dbo.workflow_tasks','U') IS NULL THEN N'0' ELSE N'1' END;";
    match run_odbc_query_first_text(cfg, q, &[]) {
        Ok(Some(v)) => v.trim() == "1",
        _ => false,
    }
}

pub(super) fn odbc_upsert_task(task: &TaskState, cfg: &TaskStoreConfig) -> Result<(), String> {
    let task_id = task.task_id.clone();
    let tenant_id = task.tenant_id.clone();
    let operator = task.operator.clone();
    let status = task.status.clone();
    let created = task_time_epoch(&task.created_at).unwrap_or(0).to_string();
    let updated = task_time_epoch(&task.updated_at)
        .unwrap_or_else(|| task_time_epoch(&task.created_at).unwrap_or(0))
        .to_string();
    let result_json = task
        .result
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let error = task.error.clone().unwrap_or_default();
    let source = "accel-rust".to_string();
    let idempotency_key = task.idempotency_key.clone();
    let attempts = task.attempts.to_string();
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
DECLARE @idempotency_key NVARCHAR(256)=?;\
DECLARE @attempts INT=CAST(? AS INT);\
IF EXISTS (SELECT 1 FROM dbo.workflow_tasks WHERE task_id=@task_id)\
BEGIN\
  UPDATE dbo.workflow_tasks\
  SET tenant_id=@tenant_id,operator=@operator,status=@status,\
      updated_at_epoch=@updated_at_epoch,\
      result_json=@result_json,error=@error,source=@source,\
      idempotency_key=@idempotency_key,attempts=@attempts\
  WHERE task_id=@task_id;\
END\
ELSE\
BEGIN\
  INSERT INTO dbo.workflow_tasks (task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,source,idempotency_key,attempts)\
  VALUES (@task_id,@tenant_id,@operator,@status,@created_at_epoch,@updated_at_epoch,@result_json,@error,@source,@idempotency_key,@attempts);\
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
        idempotency_key,
        attempts,
    ];
    run_odbc_exec(cfg, q, &params)
}

pub(super) fn odbc_get_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<TaskState>, String> {
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
SELECT TOP 1 task_id,tenant_id,operator,status,created_at_epoch,updated_at_epoch,result_json,error,idempotency_key,attempts\
FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = match run_odbc_query_first_text(cfg, q, &[task_id.to_string()])? {
        Some(value) => value,
        None => return Ok(None),
    };
    let s = out.trim();
    if s.is_empty() {
        return Ok(None);
    }
    let row: Value =
        serde_json::from_str(s).map_err(|e| format!("odbc get task json parse: {e}"))?;
    parse_task_from_runtime_row(&row)
        .map(Some)
        .ok_or_else(|| "odbc get task: invalid task payload".to_string())
}

pub(super) fn odbc_cancel_task(task_id: &str, cfg: &TaskStoreConfig) -> Result<Option<Value>, String> {
    let now = utc_now_epoch_string()
        .parse::<u64>()
        .unwrap_or(0)
        .to_string();
    let q = "SET NOCOUNT ON;\
DECLARE @task_id NVARCHAR(128)=?;\
DECLARE @now BIGINT=CAST(? AS BIGINT);\
UPDATE dbo.workflow_tasks SET status=N'cancelled',updated_at_epoch=@now\
WHERE task_id=@task_id AND status IN (N'queued',N'running');\
SELECT TOP 1 task_id,status FROM dbo.workflow_tasks WHERE task_id=@task_id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;";
    let out = match run_odbc_query_first_text(cfg, q, &[task_id.to_string(), now])? {
        Some(value) => value,
        None => return Ok(None),
    };
    let row: Value = serde_json::from_str(out.trim())
        .map_err(|e| format!("odbc cancel task json parse: {e}"))?;
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

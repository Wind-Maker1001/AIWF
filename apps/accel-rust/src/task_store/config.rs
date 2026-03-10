use super::*;
use crate::config::env_truthy;
use url::Url;

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
    let remote_enabled = env_truthy("AIWF_RUST_TASK_STORE_REMOTE");
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
    let sql_use_windows_auth = env_truthy("AIWF_SQL_USE_WINDOWS_AUTH");
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
    let mut fallback_backend: Option<String> = None;
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
        if fallback_backend.is_none() {
            fallback_backend = Some(b.to_string());
        }
        if probe_remote_task_store(&probe_cfg) {
            cfg.backend = b.to_string();
            return cfg;
        }
    }
    if let Some(backend) = fallback_backend {
        cfg.backend = backend;
    }
    cfg
}

pub fn parse_sqlserver_conn_str(s: &str) -> TaskStoreConfig {
    let mut cfg = task_store_config_from_env();
    let trimmed = s.trim();
    let normalized = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("sqlserver://{trimmed}")
    };
    if let Ok(url) = Url::parse(&normalized) {
        if let Some(host) = url.host_str() {
            cfg.sql_host = host.to_string();
        }
        if let Some(port) = url.port() {
            cfg.sql_port = port;
        }
        let db = url.path().trim_start_matches('/');
        if !db.is_empty() {
            cfg.sql_db = db.to_string();
        }
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "user" => cfg.sql_user = Some(value.into_owned()),
                "password" => cfg.sql_password = Some(value.into_owned()),
                "windows_auth" => {
                    let value = value.trim().to_ascii_lowercase();
                    cfg.sql_use_windows_auth =
                        matches!(value.as_str(), "1" | "true" | "yes" | "on");
                }
                _ => {}
            }
        }
        return cfg;
    }
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

#[cfg(test)]
mod tests {
    use super::{parse_sqlserver_conn_str, resolve_task_store_backend, task_store_config_from_env};
    use crate::app_state::TaskStoreConfig;
    use std::env;

    #[test]
    fn remote_task_store_flag_accepts_truthy_values() {
        let prev = env::var("AIWF_RUST_TASK_STORE_REMOTE").ok();
        unsafe {
            env::set_var("AIWF_RUST_TASK_STORE_REMOTE", "1");
        }
        let cfg = task_store_config_from_env();
        assert!(cfg.remote_enabled);
        unsafe {
            if let Some(v) = prev {
                env::set_var("AIWF_RUST_TASK_STORE_REMOTE", v);
            } else {
                env::remove_var("AIWF_RUST_TASK_STORE_REMOTE");
            }
        }
    }

    #[test]
    fn resolve_backend_keeps_remote_enabled_when_probe_fails() {
        let cfg = TaskStoreConfig {
            ttl_sec: 3600,
            max_tasks: 1000,
            store_path: None,
            remote_enabled: true,
            backend: "base_api".to_string(),
            base_api_url: Some("http://127.0.0.1:1".to_string()),
            base_api_key: None,
            sql_host: "127.0.0.1".to_string(),
            sql_port: 1433,
            sql_db: "AIWF".to_string(),
            sql_user: None,
            sql_password: None,
            sql_use_windows_auth: false,
        };
        let resolved = resolve_task_store_backend(cfg);
        assert!(resolved.remote_enabled);
        assert_eq!(resolved.backend, "base_api");
    }

    #[test]
    fn parse_sqlserver_conn_str_handles_url_encoded_query_values() {
        let cfg = parse_sqlserver_conn_str("db.example.com:1444/AIWF?user=svc&password=p%40ss%26word&windows_auth=yes");
        assert_eq!(cfg.sql_host, "db.example.com");
        assert_eq!(cfg.sql_port, 1444);
        assert_eq!(cfg.sql_db, "AIWF");
        assert_eq!(cfg.sql_user.as_deref(), Some("svc"));
        assert_eq!(cfg.sql_password.as_deref(), Some("p@ss&word"));
        assert!(cfg.sql_use_windows_auth);
    }
}

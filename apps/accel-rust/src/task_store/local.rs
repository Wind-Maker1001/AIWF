use super::*;

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

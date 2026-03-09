use super::*;

pub(crate) fn run_join_rows_v3(req: JoinRowsV3Req) -> Result<JoinRowsResp, String> {
    let left_n = req.left_rows.len();
    let right_n = req.right_rows.len();
    let chunk_size = req.chunk_size.unwrap_or(50_000).max(1000);
    let auto_threshold = env::var("AIWF_JOIN_V3_AUTO_SORTMERGE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(120_000);
    let strategy = req
        .strategy
        .unwrap_or_else(|| "auto".to_string())
        .to_lowercase();
    let selected = if strategy == "auto" {
        if left_n.saturating_add(right_n) >= auto_threshold {
            "sort_merge"
        } else {
            "hash"
        }
    } else {
        strategy.as_str()
    };
    let mut spill_written = false;
    let mut spill_file = None::<String>;
    if left_n.saturating_add(right_n) > chunk_size
        && let Some(path) = req.spill_path.as_ref()
    {
        let p = PathBuf::from(path);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create spill dir: {e}"))?;
        }
        let payload = json!({
            "strategy": selected,
            "left_rows": left_n,
            "right_rows": right_n,
            "chunk_size": chunk_size,
            "created_at": utc_now_iso()
        });
        fs::write(
            &p,
            serde_json::to_string_pretty(&payload)
                .map_err(|e| format!("serialize spill marker: {e}"))?,
        )
        .map_err(|e| format!("write spill marker: {e}"))?;
        spill_written = true;
        spill_file = Some(p.to_string_lossy().to_string());
    }
    let mut resp = run_join_rows_v2(JoinRowsV2Req {
        run_id: req.run_id,
        left_rows: req.left_rows,
        right_rows: req.right_rows,
        left_on: req.left_on,
        right_on: req.right_on,
        join_type: req.join_type,
    })?;
    resp.operator = "join_rows_v3".to_string();
    let mut stats = resp.stats.as_object().cloned().unwrap_or_default();
    stats.insert("strategy".to_string(), json!(selected));
    stats.insert("spill_written".to_string(), json!(spill_written));
    if let Some(sp) = spill_file {
        stats.insert("spill_file".to_string(), json!(sp));
    }
    stats.insert("chunk_size".to_string(), json!(chunk_size));
    resp.stats = Value::Object(stats);
    Ok(resp)
}

pub(crate) fn run_join_rows_v4(req: JoinRowsV4Req) -> Result<JoinRowsResp, String> {
    let left_keys = parse_join_keys(&req.left_on)?;
    let right_keys = parse_join_keys(&req.right_on)?;
    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("join_rows_v4 requires non-empty join keys".to_string());
    }
    let bloom_enabled = req.enable_bloom.unwrap_or(true);
    let bloom_field_right = req
        .bloom_field
        .clone()
        .unwrap_or_else(|| right_keys[0].clone());
    let bloom_field_left = req
        .bloom_field
        .clone()
        .unwrap_or_else(|| left_keys[0].clone());
    let right_index = if bloom_enabled {
        req.right_rows
            .iter()
            .filter_map(|r| r.as_object())
            .map(|o| value_to_string_or_null(o.get(&bloom_field_right)))
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let filtered_left = if bloom_enabled {
        req.left_rows
            .iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                let k = value_to_string_or_null(obj.get(&bloom_field_left));
                if right_index.contains(&k) {
                    Some(r.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        req.left_rows.clone()
    };
    let auto_strategy = if filtered_left.len() + req.right_rows.len() > 120_000 {
        "sort_merge".to_string()
    } else {
        "hash".to_string()
    };
    let mut out = run_join_rows_v3(JoinRowsV3Req {
        run_id: req.run_id.clone(),
        left_rows: filtered_left,
        right_rows: req.right_rows.clone(),
        left_on: req.left_on.clone(),
        right_on: req.right_on.clone(),
        join_type: req.join_type.clone(),
        strategy: req.strategy.clone().or(Some(auto_strategy)),
        spill_path: req.spill_path.clone(),
        chunk_size: req.chunk_size,
    })?;
    out.operator = "join_rows_v4".to_string();
    let left_after_bloom = out
        .stats
        .get("left_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if let Some(stats) = out.stats.as_object_mut() {
        stats.insert("bloom_enabled".to_string(), json!(bloom_enabled));
        stats.insert("bloom_field_left".to_string(), json!(bloom_field_left));
        stats.insert("bloom_field_right".to_string(), json!(bloom_field_right));
        stats.insert("left_rows_input".to_string(), json!(req.left_rows.len()));
        stats.insert("left_rows_after_bloom".to_string(), json!(left_after_bloom));
    }
    Ok(out)
}

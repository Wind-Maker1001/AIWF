use super::*;

pub(crate) fn run_transform_rows_v2_with_cache(
    req: TransformRowsReq,
    cancel_flag: Option<Arc<AtomicBool>>,
    cache: Option<&Arc<Mutex<HashMap<String, TransformCacheEntry>>>>,
    metrics: Option<&Arc<Mutex<ServiceMetrics>>>,
) -> Result<TransformRowsResp, String> {
    verify_request_signature(&req)?;
    let cache_enabled = transform_cache_enabled();
    let now = unix_now_sec();
    let max_entries = transform_cache_max_entries();
    let ttl_sec = transform_cache_ttl_sec();
    let key = if cache_enabled {
        Some(transform_cache_key(&req))
    } else {
        None
    };

    if cache_enabled
        && let (Some(cache_ref), Some(cache_key)) = (cache, key.as_ref())
        && let Ok(mut guard) = cache_ref.lock()
    {
        let evicted = prune_transform_cache_entries(&mut guard, now, max_entries);
        if evicted > 0
            && let Some(m) = metrics
            && let Ok(mut mg) = m.lock()
        {
            mg.transform_cache_evict_total += evicted as u64;
        }
        if let Some(entry) = guard.get_mut(cache_key)
            && entry.expires_at_epoch > now
        {
            entry.hits += 1;
            entry.last_hit_epoch = now;
            let mut resp = entry.resp.clone();
            resp.audit["cache"] = json!({
                "enabled": true,
                "hit": true,
                "hits": entry.hits,
                "ttl_sec": ttl_sec
            });
            if let Some(uri) = req.output_uri.clone() {
                save_rows_to_uri(&uri, &resp.rows)?;
            }
            if let Some(m) = metrics
                && let Ok(mut mg) = m.lock()
            {
                mg.transform_cache_hit_total += 1;
            }
            return Ok(resp);
        }
        if let Some(m) = metrics
            && let Ok(mut mg) = m.lock()
        {
            mg.transform_cache_miss_total += 1;
        }
    }

    let mut resp = run_transform_rows_v2_with_cancel(req.clone(), cancel_flag)?;
    if cache_enabled
        && let (Some(cache_ref), Some(cache_key)) = (cache, key.as_ref())
        && let Ok(mut guard) = cache_ref.lock()
    {
        let evicted = prune_transform_cache_entries(&mut guard, now, max_entries);
        if evicted > 0
            && let Some(m) = metrics
            && let Ok(mut mg) = m.lock()
        {
            mg.transform_cache_evict_total += evicted as u64;
        }
        guard.insert(
            cache_key.to_string(),
            TransformCacheEntry {
                resp: resp.clone(),
                expires_at_epoch: now + ttl_sec,
                last_hit_epoch: now,
                hits: 0,
            },
        );
        let evicted_after = prune_transform_cache_entries(&mut guard, now, max_entries);
        if evicted_after > 0
            && let Some(m) = metrics
            && let Ok(mut mg) = m.lock()
        {
            mg.transform_cache_evict_total += evicted_after as u64;
        }
    }
    resp.audit["cache"] = json!({
        "enabled": cache_enabled,
        "hit": false,
        "ttl_sec": ttl_sec
    });
    Ok(resp)
}

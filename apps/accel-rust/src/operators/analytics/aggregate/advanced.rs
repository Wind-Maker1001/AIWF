use super::helpers::aggregate_group_v3;
use super::*;

pub(crate) fn run_aggregate_rows_v3(req: AggregateRowsV3Req) -> Result<AggregateRowsResp, String> {
    let sample_size = req.approx_sample_size.unwrap_or(1024).clamp(64, 1_000_000);
    let mut buckets: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    let mut group_cache: HashMap<String, Map<String, Value>> = HashMap::new();
    for row in req.rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let mut group_vals = Map::new();
        let mut key_parts = Vec::new();
        for g in &req.group_by {
            let v = obj.get(g).cloned().unwrap_or(Value::Null);
            key_parts.push(value_to_string_or_null(Some(&v)));
            group_vals.insert(g.clone(), v);
        }
        let key = key_parts.join("\u{1f}");
        group_cache.entry(key.clone()).or_insert(group_vals);
        buckets.entry(key).or_default().push(obj.clone());
    }
    let mut out = Vec::new();
    for (k, rows) in buckets {
        let group_vals = group_cache.get(&k).cloned().unwrap_or_default();
        out.push(aggregate_group_v3(
            &group_vals,
            &rows,
            &req.aggregates,
            sample_size,
        ));
    }
    Ok(AggregateRowsResp {
        ok: true,
        operator: "aggregate_rows_v3".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        stats: json!({"groups": out.len(), "output_rows": out.len(), "approx_sample_size": sample_size}),
        rows: out,
    })
}

pub(crate) fn run_aggregate_rows_v4(req: AggregateRowsV4Req) -> Result<AggregateRowsResp, String> {
    let workers = req.parallel_workers.unwrap_or(1).clamp(1, 16);
    let mut out = if workers == 1 {
        run_aggregate_rows_v3(AggregateRowsV3Req {
            run_id: req.run_id.clone(),
            rows: req.rows.clone(),
            group_by: req.group_by.clone(),
            aggregates: req.aggregates.clone(),
            approx_sample_size: req.approx_sample_size,
        })?
    } else {
        let sample_size = req.approx_sample_size.unwrap_or(1024).clamp(64, 1_000_000);
        let mut buckets: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
        let mut group_cache: HashMap<String, Map<String, Value>> = HashMap::new();
        for row in &req.rows {
            let Some(obj) = row.as_object() else {
                continue;
            };
            let mut group_vals = Map::new();
            let mut key_parts = Vec::new();
            for g in &req.group_by {
                let v = obj.get(g).cloned().unwrap_or(Value::Null);
                key_parts.push(value_to_string_or_null(Some(&v)));
                group_vals.insert(g.clone(), v);
            }
            let key = key_parts.join("\u{1f}");
            group_cache.entry(key.clone()).or_insert(group_vals);
            buckets.entry(key).or_default().push(obj.clone());
        }
        let mut groups = buckets
            .into_iter()
            .map(|(k, rows)| (group_cache.get(&k).cloned().unwrap_or_default(), rows))
            .collect::<Vec<_>>();
        let chunk = (groups.len() / workers).max(1);
        let mut joins = Vec::new();
        while !groups.is_empty() {
            let take = groups.drain(0..groups.len().min(chunk)).collect::<Vec<_>>();
            let specs = req.aggregates.clone();
            joins.push(std::thread::spawn(move || {
                take.into_iter()
                    .map(|(group_vals, rows)| {
                        aggregate_group_v3(&group_vals, &rows, &specs, sample_size)
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut rows = Vec::new();
        for h in joins {
            let part = h
                .join()
                .map_err(|_| "aggregate_rows_v4 parallel worker panicked".to_string())?;
            rows.extend(part);
        }
        AggregateRowsResp {
            ok: true,
            operator: "aggregate_rows_v3".to_string(),
            status: "done".to_string(),
            run_id: req.run_id.clone(),
            stats: json!({"groups": rows.len(), "output_rows": rows.len(), "approx_sample_size": sample_size, "parallel_workers": workers}),
            rows,
        }
    };
    out.operator = "aggregate_rows_v4".to_string();
    if let Some(stats) = out.stats.as_object_mut() {
        stats.insert("parallel_workers".to_string(), json!(workers));
    }
    if req.verify_exact.unwrap_or(false) {
        let exact = run_aggregate_rows_v2(AggregateRowsV2Req {
            run_id: req.run_id.clone(),
            rows: req.rows,
            group_by: req.group_by,
            aggregates: req.aggregates,
        })?;
        if let Some(stats) = out.stats.as_object_mut() {
            stats.insert("exact_verify".to_string(), json!(true));
            stats.insert("exact_rows".to_string(), json!(exact.rows.len()));
            stats.insert("approx_rows".to_string(), json!(out.rows.len()));
            stats.insert(
                "row_count_delta".to_string(),
                json!((out.rows.len() as i64 - exact.rows.len() as i64).abs()),
            );
        }
    }
    Ok(out)
}

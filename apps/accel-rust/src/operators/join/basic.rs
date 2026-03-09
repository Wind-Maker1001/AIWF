use super::*;

pub(crate) fn run_join_rows_v1(req: JoinRowsReq) -> Result<JoinRowsResp, String> {
    let mut right_index: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    for row in req.right_rows {
        if let Some(obj) = row.as_object() {
            let k = value_to_string_or_null(obj.get(&req.right_on));
            right_index.entry(k).or_default().push(obj.clone());
        }
    }
    let join_type = req
        .join_type
        .unwrap_or_else(|| "inner".to_string())
        .to_lowercase();
    let mut out: Vec<Value> = Vec::new();
    let mut matched = 0usize;
    for row in req.left_rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let k = value_to_string_or_null(obj.get(&req.left_on));
        if let Some(rrs) = right_index.get(&k) {
            for rr in rrs {
                let mut merged = obj.clone();
                for (rk, rv) in rr {
                    if merged.contains_key(rk) {
                        merged.insert(format!("right_{rk}"), rv.clone());
                    } else {
                        merged.insert(rk.clone(), rv.clone());
                    }
                }
                out.push(Value::Object(merged));
                matched += 1;
            }
        } else if join_type == "left" {
            out.push(Value::Object(obj.clone()));
        }
    }
    Ok(JoinRowsResp {
        ok: true,
        operator: "join_rows_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: out,
        stats: json!({"matched_pairs": matched}),
    })
}

pub(crate) fn run_join_rows_v2(req: JoinRowsV2Req) -> Result<JoinRowsResp, String> {
    let left_keys = parse_join_keys(&req.left_on)?;
    let right_keys = parse_join_keys(&req.right_on)?;
    if left_keys.len() != right_keys.len() {
        return Err("left_on and right_on key count mismatch".to_string());
    }
    let join_type = req
        .join_type
        .unwrap_or_else(|| "inner".to_string())
        .to_lowercase();
    if !matches!(
        join_type.as_str(),
        "inner" | "left" | "right" | "full" | "semi" | "anti"
    ) {
        return Err(format!("unsupported join_type: {join_type}"));
    }
    let mut right_index: JoinIndex = HashMap::new();
    for (idx, row) in req.right_rows.into_iter().enumerate() {
        if let Some(obj) = row.as_object() {
            let k = join_key_multi(obj, &right_keys);
            right_index.entry(k).or_default().push((idx, obj.clone()));
        }
    }
    let mut out: Vec<Value> = Vec::new();
    let mut matched_pairs = 0usize;
    let mut matched_right: HashSet<usize> = HashSet::new();
    for row in req.left_rows {
        let Some(obj) = row.as_object() else {
            continue;
        };
        let k = join_key_multi(obj, &left_keys);
        if let Some(rrs) = right_index.get(&k) {
            if join_type == "semi" {
                out.push(Value::Object(obj.clone()));
                matched_pairs += 1;
                for (idx, _) in rrs {
                    matched_right.insert(*idx);
                }
                continue;
            }
            if join_type == "anti" {
                continue;
            }
            for (ridx, rr) in rrs {
                matched_right.insert(*ridx);
                let mut merged = obj.clone();
                for (rk, rv) in rr {
                    if merged.contains_key(rk) {
                        merged.insert(format!("right_{rk}"), rv.clone());
                    } else {
                        merged.insert(rk.clone(), rv.clone());
                    }
                }
                out.push(Value::Object(merged));
                matched_pairs += 1;
            }
        } else if join_type == "left" || join_type == "full" || join_type == "anti" {
            out.push(Value::Object(obj.clone()));
        }
    }
    if join_type == "right" || join_type == "full" {
        for rrs in right_index.into_values() {
            for (ridx, rr) in rrs {
                if !matched_right.contains(&ridx) {
                    out.push(Value::Object(rr));
                }
            }
        }
    }
    Ok(JoinRowsResp {
        ok: true,
        operator: "join_rows_v2".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        rows: out.clone(),
        stats: json!({
            "matched_pairs": matched_pairs,
            "output_rows": out.len(),
            "join_type": join_type,
            "left_keys": left_keys,
            "right_keys": right_keys
        }),
    })
}

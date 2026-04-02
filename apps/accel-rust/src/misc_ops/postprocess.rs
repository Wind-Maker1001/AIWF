use super::*;
use accel_rust::app_state::{TransformRowsResp, TransformRowsStats};
use crate::api_types::PostprocessRowsV1Req;
use crate::transform_support::{is_missing, resolve_trace_id, value_to_string};
use regex::Regex;
use serde_json::{Map, Value, json};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

fn first_non_empty(row: &Map<String, Value>, keys: &[String]) -> Option<Value> {
    for key in keys {
        let Some(value) = row.get(key) else { continue };
        if value.is_null() {
            continue;
        }
        if let Some(text) = value.as_str()
            && text.trim().is_empty()
        {
            continue;
        }
        return Some(value.clone());
    }
    None
}

fn alias_list(schema: &Map<String, Value>, name: &str, defaults: &[&str]) -> Vec<String> {
    if let Some(value) = schema.get(name) {
        if let Some(text) = value.as_str() {
            return vec![text.to_string()];
        }
        if let Some(items) = value.as_array() {
            return items
                .iter()
                .filter_map(|item| item.as_str().map(|text| text.to_string()))
                .collect();
        }
    }
    defaults.iter().map(|item| item.to_string()).collect()
}

fn canonical_evidence_row(row: &Map<String, Value>, schema: &Map<String, Value>) -> Map<String, Value> {
    let claim = first_non_empty(row, &alias_list(schema, "claim_text", &["claim_text", "text", "content"]))
        .unwrap_or(Value::Null);
    let speaker = first_non_empty(row, &alias_list(schema, "speaker", &["speaker", "author", "name"]))
        .unwrap_or(Value::Null);
    let source_url = first_non_empty(row, &alias_list(schema, "source_url", &["source_url", "url", "link"]))
        .unwrap_or(Value::Null);
    let source_title = first_non_empty(row, &alias_list(schema, "source_title", &["source_title", "title", "source_name"]))
        .unwrap_or(Value::Null);
    let published_at = first_non_empty(row, &alias_list(schema, "published_at", &["published_at", "publish_date", "date"]))
        .unwrap_or(Value::Null);
    let stance = first_non_empty(row, &alias_list(schema, "stance", &["stance", "position"]))
        .unwrap_or(Value::Null);
    let confidence = first_non_empty(row, &alias_list(schema, "confidence", &["confidence", "score"]))
        .unwrap_or(Value::Null);

    let source_path = row.get("source_path").cloned().unwrap_or(Value::Null);
    let source_file = row.get("source_file").cloned().unwrap_or(Value::Null);
    let source_type = row.get("source_type").cloned().unwrap_or(Value::Null);
    let chunk_index = row.get("chunk_index").cloned().unwrap_or(Value::Null);
    let page = row.get("page").cloned().unwrap_or(Value::Null);
    let sheet_name = row.get("sheet_name").cloned().unwrap_or(Value::Null);
    let row_index = row.get("row_index").cloned().unwrap_or(Value::Null);

    let key_text = [
        value_to_string(&source_path),
        value_to_string(&page),
        value_to_string(&sheet_name),
        value_to_string(if is_missing(Some(&row_index)) { &chunk_index } else { &row_index }),
        value_to_string(&claim),
    ]
    .join("|");
    let mut hasher = Sha1::new();
    hasher.update(key_text.as_bytes());
    let evidence_id = format!("{:x}", hasher.finalize());

    let mut out = Map::new();
    out.insert("evidence_id".to_string(), Value::String(evidence_id.chars().take(16).collect()));
    out.insert("claim_text".to_string(), claim);
    out.insert("speaker".to_string(), speaker);
    out.insert("source_title".to_string(), source_title);
    out.insert("source_url".to_string(), source_url);
    out.insert("published_at".to_string(), published_at);
    out.insert("stance".to_string(), stance);
    out.insert("confidence".to_string(), confidence);
    out.insert("source_file".to_string(), source_file);
    out.insert("source_path".to_string(), source_path);
    out.insert("source_type".to_string(), source_type);
    out.insert("page".to_string(), page);
    out.insert("sheet_name".to_string(), sheet_name);
    out.insert("row_index".to_string(), row_index);
    out.insert("chunk_index".to_string(), chunk_index);
    out
}

fn split_paragraphs(text: &str) -> Vec<String> {
    text.split("\n\n")
        .map(|part| collapse_ws(part))
        .filter(|part| !part.is_empty())
        .collect()
}

fn split_sentences(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?' | '。' | '！' | '？') {
            let chunk = collapse_ws(&current);
            if !chunk.is_empty() {
                out.push(chunk);
            }
            current.clear();
        }
    }
    let tail = collapse_ws(&current);
    if !tail.is_empty() {
        out.push(tail);
    }
    if out.is_empty() {
        vec![collapse_ws(text)].into_iter().filter(|item| !item.is_empty()).collect()
    } else {
        out
    }
}

fn chunk_text(text: &str, mode: &str, max_chars: usize) -> Vec<String> {
    let source = text.trim();
    if source.is_empty() {
        return Vec::new();
    }
    match mode.trim().to_lowercase().as_str() {
        "" | "none" | "off" => vec![source.to_string()],
        "paragraph" => {
            let chunks = split_paragraphs(source);
            if chunks.is_empty() { vec![source.to_string()] } else { chunks }
        }
        "sentence" => split_sentences(source),
        "fixed" => {
            let size = max_chars.max(1);
            let chars = source.chars().collect::<Vec<_>>();
            let mut out = Vec::new();
            let mut start = 0usize;
            while start < chars.len() {
                let end = (start + size).min(chars.len());
                let chunk = chars[start..end].iter().collect::<String>();
                let normalized = collapse_ws(&chunk);
                if !normalized.is_empty() {
                    out.push(normalized);
                }
                start = end;
            }
            if out.is_empty() { vec![source.to_string()] } else { out }
        }
        _ => vec![source.to_string()],
    }
}

fn infer_topic_key(text: &str, ignore_words: &HashSet<String>) -> String {
    let normalized = Regex::new(r"[^a-z0-9\u4e00-\u9fff\s]+")
        .ok()
        .map(|re| re.replace_all(&text.to_lowercase(), " ").into_owned())
        .unwrap_or_else(|| text.to_lowercase());
    let stop = [
        "the", "a", "an", "is", "are", "of", "to", "and", "or", "in", "on", "for", "we",
        "should", "it", "this", "that", "these", "those", "they", "them",
    ]
    .into_iter()
    .map(|item| item.to_string())
    .collect::<HashSet<_>>();
    normalized
        .split_whitespace()
        .filter(|token| !stop.contains(*token) && !ignore_words.contains(*token))
        .take(8)
        .map(|token| token.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn detect_polarity(text: &str, positive_words: &[String], negative_words: &[String]) -> String {
    let lower = text.to_lowercase();
    let pos = positive_words.iter().any(|word| lower.contains(word));
    let neg = negative_words.iter().any(|word| lower.contains(word));
    if pos && !neg {
        "pro".to_string()
    } else if neg && !pos {
        "con".to_string()
    } else {
        "unknown".to_string()
    }
}

fn apply_conflict_detection(
    rows: Vec<Map<String, Value>>,
    req: &PostprocessRowsV1Req,
) -> (Vec<Map<String, Value>>, usize) {
    if !req.detect_conflicts.unwrap_or(false) {
        return (rows, 0);
    }

    let topic_field = req
        .conflict_topic_field
        .clone()
        .unwrap_or_else(|| "topic".to_string());
    let stance_field = req
        .conflict_stance_field
        .clone()
        .unwrap_or_else(|| "stance".to_string());
    let text_field = req
        .conflict_text_field
        .clone()
        .unwrap_or_else(|| "claim_text".to_string());
    let positive_words = req
        .conflict_positive_words
        .clone()
        .unwrap_or_else(|| vec!["support".to_string(), "true".to_string(), "yes".to_string(), "approve".to_string(), "agree".to_string()])
        .into_iter()
        .map(|item| item.to_lowercase())
        .collect::<Vec<_>>();
    let negative_words = req
        .conflict_negative_words
        .clone()
        .unwrap_or_else(|| vec!["oppose".to_string(), "false".to_string(), "no".to_string(), "reject".to_string(), "disagree".to_string()])
        .into_iter()
        .map(|item| item.to_lowercase())
        .collect::<Vec<_>>();
    let ignore_words = positive_words
        .iter()
        .chain(negative_words.iter())
        .map(|item| item.to_lowercase())
        .collect::<HashSet<_>>();

    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    let mut row_topics = Vec::new();
    let mut row_polarities = Vec::new();

    for row in &rows {
        let mut topic = row
            .get(&topic_field)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if topic.is_empty() {
            topic = infer_topic_key(
                &value_to_string(row.get(&text_field).unwrap_or(&Value::Null)),
                &ignore_words,
            );
        }
        if topic.is_empty() {
            let source = row
                .get("source_path")
                .or_else(|| row.get("source_file"))
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim()
                .to_lowercase();
            if !source.is_empty() {
                topic = format!("src:{source}");
            }
        }
        let polarity_source = row
            .get(&stance_field)
            .filter(|value| !is_missing(Some(value)))
            .unwrap_or_else(|| row.get(&text_field).unwrap_or(&Value::Null));
        let polarity = detect_polarity(&value_to_string(polarity_source), &positive_words, &negative_words);
        let row_index = row_topics.len();
        row_topics.push(topic.clone());
        row_polarities.push(polarity);
        if !topic.is_empty() {
            groups.entry(topic).or_default().push(row_index);
        }
    }

    let mut conflict_topics = HashSet::new();
    for (topic, indexes) in groups {
        let polarities = indexes
            .iter()
            .map(|idx| row_polarities[*idx].clone())
            .collect::<HashSet<_>>();
        if polarities.contains("pro") && polarities.contains("con") {
            conflict_topics.insert(topic);
        }
    }

    let mut marked = 0usize;
    let mut out = Vec::new();
    for (idx, row) in rows.into_iter().enumerate() {
        let mut next = row.clone();
        let topic = row_topics.get(idx).cloned().unwrap_or_default();
        let polarity = row_polarities.get(idx).cloned().unwrap_or_else(|| "unknown".to_string());
        let conflict = !topic.is_empty() && conflict_topics.contains(&topic);
        if conflict {
            marked += 1;
        }
        next.insert("conflict_topic".to_string(), Value::String(topic));
        next.insert("conflict_polarity".to_string(), Value::String(polarity));
        next.insert("conflict_flag".to_string(), Value::Bool(conflict));
        out.push(next);
    }
    (out, marked)
}

pub(crate) fn run_postprocess_rows_v1(req: PostprocessRowsV1Req) -> Result<TransformRowsResp, String> {
    let started = Instant::now();
    let standardize_evidence = req.standardize_evidence.unwrap_or(false);
    let evidence_schema = req
        .evidence_schema
        .as_ref()
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let chunk_mode = req.chunk_mode.clone().unwrap_or_else(|| "none".to_string());
    let chunk_field = req.chunk_field.clone().unwrap_or_else(|| {
        if standardize_evidence { "claim_text".to_string() } else { "text".to_string() }
    });
    let chunk_max_chars = req.chunk_max_chars.unwrap_or(500).max(1);

    let input_rows = req.rows.len();
    let mut rows_out = Vec::new();
    let mut standardized_rows = 0usize;
    let mut chunked_rows_created = 0usize;

    for row in &req.rows {
        let Some(source_row) = row.as_object() else {
            continue;
        };
        let source_text = value_to_string(source_row.get(&chunk_field).unwrap_or(&Value::Null));
        let chunks = chunk_text(&source_text, &chunk_mode, chunk_max_chars);
        let chunk_targets = if chunks.is_empty() { vec![None] } else { chunks.into_iter().map(Some).collect() };
        if chunk_targets.len() > 1 {
            chunked_rows_created += chunk_targets.len().saturating_sub(1);
        }
        for (chunk_index, chunk_value) in chunk_targets.into_iter().enumerate() {
            let mut next = source_row.clone();
            if let Some(text) = chunk_value {
                next.insert(chunk_field.clone(), Value::String(text));
                next.insert("chunk_seq".to_string(), json!(chunk_index));
            }
            if standardize_evidence {
                next = canonical_evidence_row(&next, &evidence_schema);
                standardized_rows += 1;
            }
            rows_out.push(next);
        }
    }

    let (rows_out, conflict_rows_marked) = apply_conflict_detection(rows_out, &req);
    let output_rows = rows_out.len();
    let latency_ms = started.elapsed().as_millis();
    let trace_id = resolve_trace_id(
        req.trace_id.as_deref(),
        req.traceparent.as_deref(),
        &format!(
            "{}:{}:{}:{}",
            req.run_id.clone().unwrap_or_default(),
            input_rows,
            output_rows,
            latency_ms
        ),
    );
    let stage_provenance = vec![
        json!({"stage":"standardize_evidence","enabled":standardize_evidence,"engine":"postprocess_rows_v1"}),
        json!({"stage":"chunk_text","enabled":chunk_mode.trim().to_lowercase() != "none" && chunk_mode.trim().to_lowercase() != "off","engine":"postprocess_rows_v1"}),
        json!({"stage":"detect_conflicts","enabled":req.detect_conflicts.unwrap_or(false),"engine":"postprocess_rows_v1"}),
    ];

    Ok(TransformRowsResp {
        ok: true,
        operator: "postprocess_rows_v1".to_string(),
        status: "done".to_string(),
        run_id: req.run_id.clone(),
        trace_id,
        rows: rows_out.into_iter().map(Value::Object).collect(),
        quality: json!({
            "input_rows": input_rows,
            "output_rows": output_rows,
            "standardized_rows": standardized_rows,
            "chunked_rows_created": chunked_rows_created,
            "conflict_rows_marked": conflict_rows_marked,
        }),
        gate_result: json!({"passed": true, "errors": []}),
        stats: TransformRowsStats {
            input_rows,
            output_rows,
            invalid_rows: 0,
            filtered_rows: 0,
            duplicate_rows_removed: 0,
            latency_ms,
        },
        rust_v2_used: false,
        schema_hint: req.schema_hint.clone(),
        aggregate: None,
        audit: json!({
            "schema": "postprocess_rows_v1.audit.v1",
            "stage_provenance": stage_provenance,
            "standardized_rows": standardized_rows,
            "chunked_rows_created": chunked_rows_created,
            "conflict_rows_marked": conflict_rows_marked,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postprocess_rows_v1_standardizes_and_chunks_and_marks_conflicts() {
        let out = run_postprocess_rows_v1(PostprocessRowsV1Req {
            run_id: Some("pp1".to_string()),
            trace_id: None,
            traceparent: None,
            rows: vec![
                json!({"text":"Tax policy is good. Support it.","source_path":"a.txt"}),
                json!({"text":"Tax policy is bad. Oppose it.","source_path":"b.txt"}),
            ],
            standardize_evidence: Some(true),
            evidence_schema: Some(json!({})),
            chunk_mode: Some("sentence".to_string()),
            chunk_field: Some("text".to_string()),
            chunk_max_chars: Some(500),
            detect_conflicts: Some(true),
            conflict_topic_field: None,
            conflict_stance_field: None,
            conflict_text_field: Some("claim_text".to_string()),
            conflict_positive_words: Some(vec!["support".to_string(), "good".to_string()]),
            conflict_negative_words: Some(vec!["oppose".to_string(), "bad".to_string()]),
            schema_hint: None,
        })
        .expect("postprocess rows");

        assert_eq!(out.operator, "postprocess_rows_v1");
        assert!(out.rows.len() >= 4);
        assert_eq!(
            out.quality.get("chunked_rows_created").and_then(|v| v.as_u64()).unwrap_or(0),
            2
        );
        assert!(out
            .rows
            .iter()
            .any(|row| row.get("conflict_flag").and_then(|v| v.as_bool()).unwrap_or(false)));
        assert!(out
            .rows
            .iter()
            .all(|row| row.get("evidence_id").and_then(|v| v.as_str()).is_some()));
    }
}

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from aiwf.paths import resolve_path_within_root
from aiwf.preprocess_evidence import analyze_debate_row_signals
from aiwf.preprocess_io import _detect_output_format


def _safe_filename(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or "").strip())
    return s.strip("._") or "artifact"


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _pick_markdown_text(row: Dict[str, Any]) -> str:
    for key in ("claim_text", "text", "content", "body", "paragraph"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    parts: List[str] = []
    for k, v in row.items():
        if v is None:
            continue
        sv = str(v).strip()
        if not sv:
            continue
        parts.append(f"{k}: {sv}")
    return " | ".join(parts)


def _summarize_backend_trace(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Dict[str, Any]] = {}
    for item in trace:
        if not isinstance(item, dict):
            continue
        engine = str(item.get("engine") or "unknown").strip() or "unknown"
        bucket = summary.setdefault(
            engine,
            {
                "attempts": 0,
                "ok": 0,
                "warnings": 0,
                "requested": 0,
                "candidate_rows": 0,
                "matched_rows": 0,
                "appended_rows": 0,
                "repaired_rows": 0,
                "repaired_cells": 0,
                "fallback_count": 0,
                "fallbacks": {},
            },
        )
        bucket["attempts"] += 1
        if bool(item.get("ok")):
            bucket["ok"] += 1
        if str(item.get("warning") or "").strip():
            bucket["warnings"] += 1
        if bool(item.get("requested")):
            bucket["requested"] += 1
        try:
            bucket["candidate_rows"] += int(item.get("candidate_rows", 0) or 0)
        except Exception:
            pass
        try:
            bucket["matched_rows"] += int(item.get("matched_rows", 0) or 0)
        except Exception:
            pass
        try:
            bucket["appended_rows"] += int(item.get("appended_rows", 0) or 0)
        except Exception:
            pass
        try:
            bucket["repaired_rows"] += int(item.get("repaired_rows", 0) or 0)
        except Exception:
            pass
        try:
            bucket["repaired_cells"] += int(item.get("repaired_cells", 0) or 0)
        except Exception:
            pass
        for key in (
            "block_rows",
            "structural_source_rows",
            "adjacent_page_rows",
            "leading_source_rows",
            "citation_token_rows",
            "source_url_rows",
            "citation_text_rows",
            "source_marker_rows",
            "multi_source_citation_rows",
        ):
            try:
                bucket[key] = int(bucket.get(key, 0) or 0) + int(item.get(key, 0) or 0)
            except Exception:
                pass
        for flag in ("ftfy_available", "builtin_unicode_repair"):
            if flag in item:
                bucket[flag] = bool(bucket.get(flag)) or bool(item.get(flag))
        fallback = str(item.get("fallback") or "").strip()
        if fallback and str(item.get("warning") or "").strip():
            bucket["fallback_count"] += 1
            fallbacks = bucket["fallbacks"] if isinstance(bucket.get("fallbacks"), dict) else {}
            fallbacks[fallback] = int(fallbacks.get(fallback, 0) or 0) + 1
            bucket["fallbacks"] = fallbacks
    return summary


def export_canonical_bundle(
    *,
    rows: List[Dict[str, Any]],
    summary: Dict[str, Any],
    meta: Dict[str, Any],
    output_path: str,
    spec: Dict[str, Any],
) -> Dict[str, Any]:
    output_dir = os.path.dirname(os.path.abspath(output_path)) or os.getcwd()
    bundle_dir = resolve_path_within_root(
        output_dir,
        str(spec.get("canonical_bundle_dir") or f"{output_path}.bundle"),
    )
    os.makedirs(bundle_dir, exist_ok=True)
    title = str(spec.get("canonical_title") or "AIWF Canonical Corpus").strip()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    md_lines: List[str] = []
    md_lines.append(f"# {title}")
    md_lines.append("")
    md_lines.append(f"- generated_at: {now}")
    md_lines.append(f"- output_rows: {int(summary.get('output_rows', len(rows)))}")
    md_lines.append(f"- input_format: {meta.get('input_format')}")
    md_lines.append("")
    md_lines.append("## Content")
    md_lines.append("")
    for i, row in enumerate(rows):
        text = _pick_markdown_text(row)
        if not text:
            continue
        md_lines.append(f"### Item {i + 1}")
        md_lines.append("")
        md_lines.append(text)
        md_lines.append("")
    md_path = os.path.join(bundle_dir, f"{_safe_filename(title)}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines).rstrip() + "\n")

    source_counts: Dict[str, int] = {}
    for row in rows:
        src = str(row.get("source_file") or row.get("source_path") or "unknown").strip()
        source_counts[src] = source_counts.get(src, 0) + 1

    metadata = {
        "title": title,
        "generated_at": now,
        "input_format": meta.get("input_format"),
        "file_count": meta.get("file_count"),
        "summary": summary,
        "row_count": len(rows),
        "source_counts": source_counts,
    }
    metadata_path = os.path.join(bundle_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    lineage = {
        "generated_at": now,
        "output_path": output_path,
        "output_format": _detect_output_format(output_path, spec),
        "input_files": spec.get("input_files") if isinstance(spec.get("input_files"), list) else [],
        "skipped_files": meta.get("skipped_files") or [],
        "failed_files": meta.get("failed_files") or [],
        "steps": [
            "ingest",
            "normalize",
            "filter",
            "deduplicate",
            "export_markdown_bundle",
        ],
    }
    lineage_path = os.path.join(bundle_dir, "lineage.json")
    with open(lineage_path, "w", encoding="utf-8") as f:
        json.dump(lineage, f, ensure_ascii=False, indent=2)

    return {
        "bundle_dir": bundle_dir,
        "markdown_path": md_path,
        "metadata_path": metadata_path,
        "lineage_path": lineage_path,
    }


def _normalized_claim_key(value: Any) -> str:
    text = str(value or "").lower()
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\[[^\]]{1,40}\]", " ", text)
    text = re.sub(r"[^0-9a-z\u4e00-\u9fff\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _looks_ocr_noise(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if any(token in text for token in ("锛", "鈥", "銆", "�")):
        return True
    symbol_count = len(re.findall(r"[^0-9A-Za-z\u4e00-\u9fff\s]", text))
    return _safe_ratio(symbol_count, len(text)) > 0.35


def _collect_input_signal_rows(spec: Dict[str, Any], input_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(spec.get("_input_rows"), list):
        return [dict(item) for item in spec.get("_input_rows") if isinstance(item, dict)]
    file_results = input_meta.get("file_results") if isinstance(input_meta.get("file_results"), list) else []
    rows: List[Dict[str, Any]] = []
    for item in file_results:
        if not isinstance(item, dict):
            continue
        sample_rows = item.get("sample_rows") if isinstance(item.get("sample_rows"), list) else []
        rows.extend(dict(sample) for sample in sample_rows if isinstance(sample, dict))
    return rows


def _build_raw_signal_hit_summary(
    raw_signal_summary: Dict[str, Any],
    metrics: Dict[str, Any],
    errors: List[str],
) -> Dict[str, Any]:
    input_row_count = int(raw_signal_summary.get("input_row_count", 0) or 0)
    signal_specs = [
        ("speaker_signal_rows", "speaker_prefix", "speaker labels"),
        ("stance_signal_rows", "stance_marker", "stance markers"),
        ("source_ref_signal_rows", "source_reference", "source references"),
        ("quote_signal_rows", "quote_block", "quote-like text"),
        ("ocr_noise_rows", "ocr_noise", "OCR noise"),
    ]
    hits: List[Dict[str, Any]] = []
    for key, label, description in signal_specs:
        count = int(raw_signal_summary.get(key, 0) or 0)
        if count <= 0:
            continue
        hits.append(
            {
                "label": label,
                "count": count,
                "ratio": round(_safe_ratio(count, input_row_count), 6),
                "description": description,
            }
        )
    if hits:
        reason_parts = [f"{item['description']} {item['count']}/{input_row_count}" for item in hits]
        recommendation_reason = "debate evidence signals found: " + "; ".join(reason_parts)
    else:
        recommendation_reason = "no explicit debate evidence signals found in source rows"
    coverage_notes = [
        f"speaker coverage {float(metrics.get('speaker_coverage') or 0.0):.6f}",
        f"speaker attribution coverage {float(metrics.get('speaker_attribution_coverage') or 0.0):.6f}",
        f"stance coverage {float(metrics.get('stance_coverage') or 0.0):.6f}",
        f"debate topic coverage {float(metrics.get('debate_topic_coverage') or 0.0):.6f}",
        f"language coverage {float(metrics.get('language_coverage') or 0.0):.6f}",
        f"source reference coverage {float(metrics.get('source_ref_coverage') or 0.0):.6f}",
        f"multi-source citation appended rows {int(metrics.get('multi_source_citation_appended_rows') or 0)}",
    ]
    conditional_failures = [
        str(item)
        for item in (metrics.get("conditional_required_failures") or [])
        if str(item).strip()
    ]
    block_reason = "; ".join(str(item) for item in errors if str(item).strip())
    return {
        "input_row_count": input_row_count,
        "detected": bool(hits),
        "hit_labels": [str(item["label"]) for item in hits],
        "hits": hits,
        "recommendation_reason": recommendation_reason,
        "coverage_notes": coverage_notes,
        "conditional_required_failures": conditional_failures,
        "block_reason": block_reason,
    }


def _debate_mode(spec: Dict[str, Any], rows: List[Dict[str, Any]]) -> bool:
    profile = str(spec.get("canonical_profile") or "").strip().lower()
    if profile == "debate_evidence" or bool(spec.get("standardize_evidence", False)):
        return True
    return any("claim_text" in row or "speaker" in row or "stance" in row for row in rows)


def _strict_debate_contract(spec: Dict[str, Any]) -> bool:
    profile = str(spec.get("canonical_profile") or "").strip().lower()
    return profile == "debate_evidence" or bool(spec.get("standardize_evidence", False))


def _requested_backend(spec: Dict[str, Any], key: str, expected: str) -> bool:
    return str(spec.get(key) or "").strip().lower() == expected


def _explicit_backend_failed(engine_trace: List[Dict[str, Any]], engine: str) -> bool:
    for item in engine_trace:
        if not isinstance(item, dict):
            continue
        if str(item.get("engine") or "").strip() != engine:
            continue
        if not bool(item.get("requested")):
            continue
        if str(item.get("warning") or "").strip() and not bool(item.get("ok")):
            return True
    return False


def _build_quality_report(rows: List[Dict[str, Any]], summary: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    input_meta = spec.get("_input_meta") if isinstance(spec.get("_input_meta"), dict) else {}
    row_count = len(rows)
    all_fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                all_fields.append(key)

    non_null_counts: Dict[str, int] = {}
    for field in all_fields:
        count = 0
        for row in rows:
            if _non_empty(row.get(field)):
                count += 1
        non_null_counts[field] = count

    coverage = {
        field: {
            "non_null": non_null_counts[field],
            "ratio": _safe_ratio(non_null_counts[field], row_count),
        }
        for field in all_fields
    }

    source_type_counts: Dict[str, int] = {}
    for row in rows:
        source_type = str(row.get("source_type") or "unknown")
        source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1

    claim_lengths: List[int] = []
    for row in rows:
        value = row.get("claim_text")
        if _non_empty(value):
            claim_lengths.append(len(str(value).strip()))
    claim_stats = {
        "count": len(claim_lengths),
        "min": min(claim_lengths) if claim_lengths else 0,
        "max": max(claim_lengths) if claim_lengths else 0,
        "avg": (sum(claim_lengths) / len(claim_lengths)) if claim_lengths else 0.0,
    }

    debate_mode = _debate_mode(spec, rows)
    strict_debate_contract = _strict_debate_contract(spec)
    required = [str(x) for x in (spec.get("quality_required_fields") or []) if str(x).strip()]
    if strict_debate_contract:
        required = sorted({*required, "claim_text", "source_path"})
    elif not required and bool(spec.get("standardize_evidence", False)):
        required = ["claim_text", "source_path"]

    required_missing: Dict[str, int] = {}
    for field in required:
        required_missing[field] = sum(1 for row in rows if not _non_empty(row.get(field)))

    input_rows = _collect_input_signal_rows(spec, input_meta)
    raw_signal_summary = {
        "input_row_count": len(input_rows),
        "speaker_signal_rows": 0,
        "stance_signal_rows": 0,
        "source_ref_signal_rows": 0,
        "quote_signal_rows": 0,
        "ocr_noise_rows": 0,
    }
    for row in input_rows:
        signals = analyze_debate_row_signals(row)
        for key in (
            "speaker_signal",
            "stance_signal",
            "source_ref_signal",
            "quote_signal",
            "ocr_noise",
        ):
            if bool(signals.get(key)):
                raw_signal_summary[f"{key}_rows"] = raw_signal_summary.get(f"{key}_rows", 0) + 1

    metric_rows = [
        row
        for row in rows
        if str(row.get("argument_role") or "").strip().lower() not in {"metadata", "section", "citation"}
    ]
    metric_row_count = len(metric_rows) or row_count

    speaker_rows = sum(1 for row in metric_rows if _non_empty(row.get("speaker")))
    speaker_role_rows = sum(1 for row in metric_rows if _non_empty(row.get("speaker_role")))
    speaker_attribution_rows = sum(
        1
        for row in metric_rows
        if _non_empty(row.get("speaker")) or _non_empty(row.get("speaker_role"))
    )
    stance_rows = sum(
        1
        for row in metric_rows
        if _non_empty(row.get("stance")) and str(row.get("stance")).strip() != "unknown"
    )
    debate_topic_rows = sum(1 for row in metric_rows if _non_empty(row.get("debate_topic")))
    language_rows = sum(
        1
        for row in metric_rows
        if _non_empty(row.get("language")) and str(row.get("language")).strip().lower() != "unknown"
    )
    source_ref_rows = sum(
        1
        for row in metric_rows
        if _non_empty(row.get("source_url"))
        or _non_empty(row.get("source_title"))
        or _non_empty(row.get("citation_text"))
    )
    resolved_source_ref_rows_all = sum(
        1
        for row in rows
        if str(row.get("argument_role") or "").strip().lower() not in {"metadata", "section"}
        and (
            _non_empty(row.get("source_url"))
            or _non_empty(row.get("source_title"))
            or _non_empty(row.get("citation_text"))
        )
    )
    quote_only_rows = sum(1 for row in metric_rows if str(row.get("argument_role") or "").strip().lower() == "quote")
    ocr_noise_rows = sum(1 for row in metric_rows if _looks_ocr_noise(row.get("claim_text")))
    source_title_rows = sum(1 for row in metric_rows if _non_empty(row.get("source_title")))
    source_url_rows = sum(1 for row in metric_rows if _non_empty(row.get("source_url")))
    citation_text_url_rows = sum(
        1
        for row in metric_rows
        if _non_empty(row.get("citation_text")) and re.search(r"https?://|www\.", str(row.get("citation_text") or ""), flags=re.I)
    )
    claim_like_rows = [
        row
        for row in metric_rows
        if str(row.get("argument_role") or "").strip().lower() not in {"quote", "evidence", "citation", "moderation", "question", "metadata", "section"}
        and _non_empty(row.get("claim_text"))
    ]
    normalized_claims = [
        normalized
        for normalized in (_normalized_claim_key(row.get("claim_text")) for row in claim_like_rows)
        if normalized
    ]
    unique_claims = set(normalized_claims)
    duplicate_claim_ratio = 1.0 - _safe_ratio(len(unique_claims), len(normalized_claims)) if normalized_claims else 0.0

    engine_trace = summary.get("engine_trace") if isinstance(summary.get("engine_trace"), list) else []
    backend_trace_summary = _summarize_backend_trace(engine_trace)
    source_title_resolution_denom = source_ref_rows or raw_signal_summary.get("source_ref_signal_rows", 0)
    citation_candidate_rows = int(summary.get("citation_candidate_rows", 0) or 0)
    url_metadata_candidate_rows = int(summary.get("url_metadata_candidate_rows", 0) or 0)
    encoding_input_rows = int(summary.get("input_rows", row_count) or row_count)

    metrics = {
        "row_count": row_count,
        "speaker_coverage": round(_safe_ratio(speaker_rows, metric_row_count), 6),
        "speaker_role_coverage": round(_safe_ratio(speaker_role_rows, metric_row_count), 6),
        "speaker_attribution_coverage": round(_safe_ratio(speaker_attribution_rows, metric_row_count), 6),
        "stance_coverage": round(_safe_ratio(stance_rows, metric_row_count), 6),
        "debate_topic_coverage": round(_safe_ratio(debate_topic_rows, metric_row_count), 6),
        "language_coverage": round(_safe_ratio(language_rows, metric_row_count), 6),
        "source_ref_coverage": round(_safe_ratio(source_ref_rows, metric_row_count), 6),
        "quote_only_ratio": round(_safe_ratio(quote_only_rows, metric_row_count), 6),
        "ocr_noise_ratio": round(_safe_ratio(ocr_noise_rows, metric_row_count), 6),
        "duplicate_claim_ratio": round(duplicate_claim_ratio, 6),
        "source_title_resolution_rate": round(_safe_ratio(source_title_rows, source_title_resolution_denom), 6),
        "url_metadata_resolution_rate": round(
            _safe_ratio(int(summary.get("url_metadata_enriched_rows", 0) or 0), max(1, url_metadata_candidate_rows or source_url_rows)),
            6,
        ),
        "source_url_normalization_rate": round(
            _safe_ratio(
                int(summary.get("source_url_normalized_rows", 0) or 0)
                + int(summary.get("citation_text_url_normalized_rows", 0) or 0),
                max(1, source_url_rows + citation_text_url_rows),
            ),
            6,
        ),
        "citation_parse_success_rate": round(
            _safe_ratio(int(summary.get("citation_backend_success_rows", 0) or 0), citation_candidate_rows),
            6,
        ),
        "multi_source_citation_appended_rows": int(summary.get("multi_source_citation_appended_rows", 0) or 0),
        "encoding_repair_ratio": round(
            min(1.0, _safe_ratio(int(summary.get("encoding_rows_repaired", 0) or 0), encoding_input_rows)),
            6,
        ),
        "backend_trace_summary": backend_trace_summary,
        "required_field_missing": required_missing,
    }

    conditional_required_failures: List[str] = []
    if strict_debate_contract:
        if raw_signal_summary.get("speaker_signal_rows", 0) > 0 and speaker_rows == 0:
            conditional_required_failures.append("speaker is required when source rows contain speaker signal")
        if raw_signal_summary.get("stance_signal_rows", 0) > 0 and stance_rows == 0:
            conditional_required_failures.append("stance is required when source rows contain stance signal")
        if raw_signal_summary.get("source_ref_signal_rows", 0) > 0 and resolved_source_ref_rows_all == 0:
            conditional_required_failures.append("source_url or source_title is required when source rows contain source reference signal")
    metrics["conditional_required_failures"] = conditional_required_failures

    backend_fallback_failures: List[str] = []
    if _requested_backend(spec, "citation_parse_backend", "grobid") and _explicit_backend_failed(engine_trace, "grobid"):
        regex_summary = backend_trace_summary.get("regex_citation_parser") if isinstance(backend_trace_summary.get("regex_citation_parser"), dict) else {}
        regex_matched_rows = int(regex_summary.get("matched_rows", 0) or 0)
        if regex_matched_rows <= 0 and int(summary.get("citation_backend_success_rows", 0) or 0) <= 0:
            backend_fallback_failures.append("citation backend grobid failed and regex fallback parsed no citation rows")
    if _requested_backend(spec, "document_parse_backend", "azure_docintelligence") and _explicit_backend_failed(engine_trace, "azure_docintelligence"):
        local_fallback_success = (
            resolved_source_ref_rows_all > 0
            or int(summary.get("source_context_backfilled_rows", 0) or 0) > 0
            or int(summary.get("source_title_enriched_rows", 0) or 0) > 0
            or int(summary.get("citation_backend_success_rows", 0) or 0) > 0
        )
        if not local_fallback_success:
            backend_fallback_failures.append("document backend azure_docintelligence failed and local fallback produced no source/citation structure")
    metrics["backend_fallback_failures"] = backend_fallback_failures

    errors: List[str] = []
    if row_count == 0:
        errors.append("preprocess produced no rows")
    for field, missing in required_missing.items():
        if missing > 0:
            errors.append(f"{field} missing in {missing} rows")
    errors.extend(conditional_required_failures)
    errors.extend(backend_fallback_failures)

    warnings: List[str] = []
    if debate_mode and row_count > 0:
        if raw_signal_summary.get("speaker_signal_rows", 0) == 0 and metrics["speaker_coverage"] < 0.25:
            warnings.append("speaker coverage is low for debate evidence rows")
        if raw_signal_summary.get("source_ref_signal_rows", 0) == 0 and metrics["source_ref_coverage"] < 0.25:
            warnings.append("source reference coverage is low for debate evidence rows")
        if metrics["ocr_noise_ratio"] > 0.2:
            warnings.append("ocr noise ratio is high")
        if metrics["duplicate_claim_ratio"] > 0.2:
            warnings.append("duplicate claim ratio is high")
    for item in engine_trace:
        if not isinstance(item, dict):
            continue
        engine = str(item.get("engine") or "").strip()
        warning = str(item.get("warning") or "").strip()
        if not engine or not warning:
            continue
        if engine == "ftfy":
            continue
        if engine == "trafilatura" and warning in {"external enrichment mode is off", "url metadata enrichment disabled"}:
            continue
        warnings.append(f"{engine}: {warning}")
    raw_signal_hit_summary = _build_raw_signal_hit_summary(raw_signal_summary, metrics, errors)

    return {
        "ok": len(errors) == 0,
        "blocked": len(errors) > 0,
        "errors": errors,
        "warnings": warnings,
        "rows": row_count,
        "fields": len(all_fields),
        "summary": summary,
        "metrics": metrics,
        "source_types": source_type_counts,
        "field_coverage": coverage,
        "required_field_missing": required_missing,
        "claim_length": claim_stats,
        "raw_signal_summary": raw_signal_summary,
        "raw_signal_hit_summary": raw_signal_hit_summary,
        "engine_trace": engine_trace,
        "input_quality": {
            "blocked": bool(input_meta.get("quality_blocked")),
            "error": str(input_meta.get("quality_error") or ""),
            "file_results": input_meta.get("file_results") if isinstance(input_meta.get("file_results"), list) else [],
            "blocked_inputs": input_meta.get("blocked_inputs") if isinstance(input_meta.get("blocked_inputs"), list) else [],
        },
    }

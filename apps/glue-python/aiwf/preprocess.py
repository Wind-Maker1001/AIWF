from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from aiwf import ingest


def _normalize_header(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[\s\-\/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "")
    if s.startswith("$"):
        s = s[1:]
    try:
        return float(s)
    except Exception:
        return None


def _normalize_amount(v: Any, digits: int = 2) -> Any:
    f = _to_float(v)
    if f is None:
        return v
    return round(f, digits)


def _normalize_date(v: Any, output_fmt: str, fmts: List[str]) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).strftime(output_fmt)
        except ValueError:
            pass
    return v


def _detect_input_format(path: str, spec: Dict[str, Any]) -> str:
    fmt = str(spec.get("input_format") or "").strip().lower()
    if fmt in {"csv", "json", "jsonl"}:
        return fmt
    ext = os.path.splitext(path)[1].lower()
    if ext in {".json"}:
        return "json"
    if ext in {".jsonl", ".ndjson"}:
        return "jsonl"
    return "csv"


def _detect_output_format(path: str, spec: Dict[str, Any]) -> str:
    fmt = str(spec.get("output_format") or "").strip().lower()
    if fmt in {"csv", "json", "jsonl"}:
        return fmt
    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        return "json"
    if ext in {".jsonl", ".ndjson"}:
        return "jsonl"
    return "csv"


def _read_csv(path: str, delimiter: Optional[str] = None) -> Tuple[List[Dict[str, Any]], str]:
    if delimiter is None:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","
            reader = csv.DictReader(f, delimiter=delimiter)
            return [dict(r) for r in reader], delimiter

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [dict(r) for r in reader], delimiter


def _read_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            return [dict(x) for x in payload["rows"] if isinstance(x, dict)]
        return [payload]
    return []


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _read_rows(path: str, spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    input_files = spec.get("input_files") if isinstance(spec.get("input_files"), list) else []
    if input_files:
        abs_files = [str(x) for x in input_files]
        rows, meta = ingest.load_rows_from_files(
            abs_files,
            text_by_line=bool(spec.get("text_split_by_line", False)),
            ocr_enabled=bool(spec.get("ocr_enabled", True)),
            ocr_lang=str(spec.get("ocr_lang") or "").strip() or None,
            ocr_config=str(spec.get("ocr_config") or "").strip() or None,
            ocr_preprocess=str(spec.get("ocr_preprocess") or "").strip() or None,
            xlsx_all_sheets=bool(spec.get("xlsx_all_sheets", False)),
            max_retries=int(spec.get("max_retries", 0)),
            on_file_error=str(spec.get("on_file_error", "skip")).strip().lower(),
        )
        return rows, meta

    fmt = _detect_input_format(path, spec)
    if fmt == "csv":
        rows, delimiter = _read_csv(path, delimiter=spec.get("delimiter"))
        return rows, {"input_format": "csv", "delimiter": delimiter}
    if fmt == "json":
        rows = _read_json(path)
        return rows, {"input_format": "json"}
    rows = _read_jsonl(path)
    return rows, {"input_format": "jsonl"}


def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if rows:
        fields = list(rows[0].keys())
        seen = set(fields)
        for r in rows[1:]:
            for k in r.keys():
                if k not in seen:
                    fields.append(k)
                    seen.add(k)
    else:
        fields = []

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _write_json(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def _write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _write_rows(path: str, rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> str:
    fmt = _detect_output_format(path, spec)
    if fmt == "json":
        _write_json(path, rows)
        return "json"
    if fmt == "jsonl":
        _write_jsonl(path, rows)
        return "jsonl"
    _write_csv(path, rows)
    return "csv"


def _filter_match(row: Dict[str, Any], f: Dict[str, Any]) -> bool:
    field = str(f.get("field") or "").strip()
    op = str(f.get("op") or "eq").strip().lower()
    target = f.get("value")
    if not field:
        return True
    val = row.get(field)

    if op == "exists":
        return val is not None
    if op == "not_exists":
        return val is None
    if op == "eq":
        return val == target
    if op == "ne":
        return val != target
    if op in {"gt", "gte", "lt", "lte"}:
        a = _to_float(val)
        b = _to_float(target)
        if a is None or b is None:
            return False
        if op == "gt":
            return a > b
        if op == "gte":
            return a >= b
        if op == "lt":
            return a < b
        return a <= b
    if op == "in":
        arr = target if isinstance(target, list) else []
        return val in arr
    if op == "not_in":
        arr = target if isinstance(target, list) else []
        return val not in arr
    if op == "contains":
        return str(target) in str(val)
    if op == "regex":
        try:
            return re.search(str(target), str(val)) is not None
        except re.error:
            return False
    if op == "not_regex":
        try:
            return re.search(str(target), str(val)) is None
        except re.error:
            return False
    return True


def _apply_field_transform(value: Any, op: str, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if op == "trim":
        if isinstance(value, str):
            return value.strip(), True
        return value, False
    if op == "lower":
        if isinstance(value, str):
            return value.lower(), True
        return value, False
    if op == "upper":
        if isinstance(value, str):
            return value.upper(), True
        return value, False
    if op == "collapse_whitespace":
        if isinstance(value, str):
            return re.sub(r"\s+", " ", value).strip(), True
        return value, False
    if op == "remove_urls":
        if isinstance(value, str):
            return re.sub(r"https?://\S+|www\.\S+", "", value).strip(), True
        return value, False
    if op == "remove_emails":
        if isinstance(value, str):
            return re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "", value).strip(), True
        return value, False
    if op == "regex_replace":
        if isinstance(value, str):
            pat = str(cfg.get("pattern") or "")
            rep = str(cfg.get("replace") or "")
            try:
                return re.sub(pat, rep, value), True
            except re.error:
                return value, False
        return value, False
    if op == "parse_number":
        f = _to_float(value)
        return (f if f is not None else value), (f is not None)
    if op == "round_number":
        digits = int(cfg.get("digits", 2))
        f = _to_float(value)
        if f is None:
            return value, False
        return round(f, digits), True
    if op == "parse_date":
        out_fmt = str(cfg.get("output_format") or "%Y-%m-%d")
        in_fmts = cfg.get("input_formats") or [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
        ]
        if isinstance(value, str):
            v = _normalize_date(value, out_fmt, [str(x) for x in in_fmts])
            return v, v != value
        return value, False
    if op == "extract_regex":
        if isinstance(value, str):
            pat = str(cfg.get("pattern") or "")
            group = int(cfg.get("group", 0))
            try:
                m = re.search(pat, value)
            except re.error:
                return value, False
            if not m:
                return value, False
            try:
                return m.group(group), True
            except IndexError:
                return value, False
        return value, False
    return value, False


def _first_non_empty(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


def _to_canonical_evidence_row(row: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    def _aliases(name: str, defaults: List[str]) -> List[str]:
        v = schema.get(name)
        if isinstance(v, str):
            return [v]
        if isinstance(v, list):
            return [str(x) for x in v]
        return defaults

    claim = _first_non_empty(row, _aliases("claim_text", ["claim_text", "text", "content"]))
    speaker = _first_non_empty(row, _aliases("speaker", ["speaker", "author", "name"]))
    source_url = _first_non_empty(row, _aliases("source_url", ["source_url", "url", "link"]))
    source_title = _first_non_empty(row, _aliases("source_title", ["source_title", "title", "source_name"]))
    published_at = _first_non_empty(row, _aliases("published_at", ["published_at", "publish_date", "date"]))
    stance = _first_non_empty(row, _aliases("stance", ["stance", "position"]))
    confidence = _first_non_empty(row, _aliases("confidence", ["confidence", "score"]))

    source_path = row.get("source_path")
    source_file = row.get("source_file")
    source_type = row.get("source_type")
    chunk_index = row.get("chunk_index")
    page = row.get("page")
    sheet_name = row.get("sheet_name")
    row_index = row.get("row_index")

    key_text = "|".join(
        [
            str(source_path or ""),
            str(page or ""),
            str(sheet_name or ""),
            str(row_index or chunk_index or ""),
            str(claim or ""),
        ]
    )
    evidence_id = hashlib.sha1(key_text.encode("utf-8")).hexdigest()[:16]

    return {
        "evidence_id": evidence_id,
        "claim_text": claim,
        "speaker": speaker,
        "source_title": source_title,
        "source_url": source_url,
        "published_at": published_at,
        "stance": stance,
        "confidence": confidence,
        "source_file": source_file,
        "source_path": source_path,
        "source_type": source_type,
        "page": page,
        "sheet_name": sheet_name,
        "row_index": row_index,
        "chunk_index": chunk_index,
    }


def _build_quality_report(rows: List[Dict[str, Any]], summary: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    row_count = len(rows)
    all_fields: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                all_fields.append(k)

    non_null_counts: Dict[str, int] = {}
    for f in all_fields:
        c = 0
        for r in rows:
            v = r.get(f)
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            c += 1
        non_null_counts[f] = c

    coverage = {
        f: {
            "non_null": non_null_counts[f],
            "ratio": (float(non_null_counts[f]) / float(row_count)) if row_count > 0 else 0.0,
        }
        for f in all_fields
    }

    source_type_counts: Dict[str, int] = {}
    for r in rows:
        st = str(r.get("source_type") or "unknown")
        source_type_counts[st] = source_type_counts.get(st, 0) + 1

    claim_lengths: List[int] = []
    for r in rows:
        v = r.get("claim_text")
        if v is None:
            continue
        s = str(v).strip()
        if s:
            claim_lengths.append(len(s))
    claim_stats = {
        "count": len(claim_lengths),
        "min": min(claim_lengths) if claim_lengths else 0,
        "max": max(claim_lengths) if claim_lengths else 0,
        "avg": (sum(claim_lengths) / len(claim_lengths)) if claim_lengths else 0.0,
    }

    required = [str(x) for x in (spec.get("quality_required_fields") or [])]
    if not required and bool(spec.get("standardize_evidence", False)):
        required = ["claim_text", "source_path"]
    required_missing: Dict[str, int] = {}
    for f in required:
        miss = 0
        for r in rows:
            v = r.get(f)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                miss += 1
        required_missing[f] = miss

    return {
        "rows": row_count,
        "fields": len(all_fields),
        "summary": summary,
        "source_types": source_type_counts,
        "field_coverage": coverage,
        "required_field_missing": required_missing,
        "claim_length": claim_stats,
    }


def _chunk_text(text: str, mode: str, max_chars: int) -> List[str]:
    s = (text or "").strip()
    if not s:
        return []
    m = (mode or "none").strip().lower()
    if m in {"none", "off"}:
        return [s]
    if m == "paragraph":
        parts = [p.strip() for p in re.split(r"\n\s*\n", s) if p.strip()]
        return parts if parts else [s]
    if m == "sentence":
        parts = [p.strip() for p in re.split(r"(?<=[\.\!\?。！？])\s+", s) if p.strip()]
        return parts if parts else [s]
    if m == "fixed":
        size = max(1, int(max_chars))
        return [s[i : i + size].strip() for i in range(0, len(s), size) if s[i : i + size].strip()]
    return [s]


def _infer_topic_key(text: Any, ignore_words: Optional[List[str]] = None) -> str:
    s = str(text or "").lower()
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff\s]+", " ", s)
    tokens = [t for t in s.split() if t]
    if not tokens:
        return ""
    stop = {
        "the",
        "a",
        "an",
        "is",
        "are",
        "of",
        "to",
        "and",
        "or",
        "in",
        "on",
        "for",
        "we",
        "should",
        "it",
        "this",
        "that",
        "these",
        "those",
        "they",
        "them",
    }
    if ignore_words:
        stop.update({str(x).lower() for x in ignore_words})
    topic = [t for t in tokens if t not in stop][:8]
    return " ".join(topic)


def _detect_polarity(text: Any, positive_words: List[str], negative_words: List[str]) -> str:
    s = str(text or "").lower()
    pos = any(w in s for w in positive_words)
    neg = any(w in s for w in negative_words)
    if pos and not neg:
        return "pro"
    if neg and not pos:
        return "con"
    return "unknown"


def _apply_conflict_detection(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    if not bool(spec.get("detect_conflicts", False)):
        return rows, 0

    topic_field = str(spec.get("conflict_topic_field") or "topic").strip()
    stance_field = str(spec.get("conflict_stance_field") or "stance").strip()
    text_field = str(spec.get("conflict_text_field") or "claim_text").strip()
    positive_words = [str(x).lower() for x in (spec.get("conflict_positive_words") or ["support", "true", "yes", "approve", "agree"])]
    negative_words = [str(x).lower() for x in (spec.get("conflict_negative_words") or ["oppose", "false", "no", "reject", "disagree"])]

    groups: Dict[str, List[int]] = {}
    row_polarity: List[str] = []
    for idx, r in enumerate(rows):
        topic = str(r.get(topic_field) or "").strip().lower()
        if not topic:
            topic = _infer_topic_key(r.get(text_field), ignore_words=positive_words + negative_words)
        if not topic:
            fallback_src = str(r.get("source_path") or r.get("source_file") or "").strip().lower()
            if fallback_src:
                topic = f"src:{fallback_src}"
        pol = _detect_polarity(r.get(stance_field) if r.get(stance_field) is not None else r.get(text_field), positive_words, negative_words)
        row_polarity.append(pol)
        if topic:
            groups.setdefault(topic, []).append(idx)

    conflict_topics = set()
    for t, idxs in groups.items():
        ps = {row_polarity[i] for i in idxs}
        if "pro" in ps and "con" in ps:
            conflict_topics.add(t)

    marked = 0
    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        topic = str(r.get(topic_field) or "").strip().lower() or _infer_topic_key(
            r.get(text_field), ignore_words=positive_words + negative_words
        )
        if not topic:
            fallback_src = str(r.get("source_path") or r.get("source_file") or "").strip().lower()
            if fallback_src:
                topic = f"src:{fallback_src}"
        rr = dict(r)
        rr["conflict_topic"] = topic
        rr["conflict_polarity"] = row_polarity[idx]
        rr["conflict_flag"] = bool(topic and topic in conflict_topics)
        if rr["conflict_flag"]:
            marked += 1
        out.append(rr)
    return out, marked


def validate_preprocess_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(spec, dict):
        return {"ok": False, "errors": ["preprocess spec must be object"], "warnings": []}

    for key in ["header_map", "default_values"]:
        if key in spec and not isinstance(spec.get(key), dict):
            errors.append(f"{key} must be an object")
    for key in [
        "amount_fields",
        "date_fields",
        "null_values",
        "include_fields",
        "exclude_fields",
        "field_transforms",
        "row_filters",
        "input_files",
        "quality_required_fields",
        "conflict_positive_words",
        "conflict_negative_words",
    ]:
        if key in spec and not isinstance(spec.get(key), list):
            errors.append(f"{key} must be an array")
    if "ocr_enabled" in spec and not isinstance(spec.get("ocr_enabled"), bool):
        errors.append("ocr_enabled must be boolean")
    if "ocr_lang" in spec and not isinstance(spec.get("ocr_lang"), str):
        errors.append("ocr_lang must be string")
    if "ocr_config" in spec and not isinstance(spec.get("ocr_config"), str):
        errors.append("ocr_config must be string")
    if "ocr_preprocess" in spec and not isinstance(spec.get("ocr_preprocess"), str):
        errors.append("ocr_preprocess must be string")
    if "ocr_preprocess" in spec:
        val = str(spec.get("ocr_preprocess") or "").strip().lower()
        if val and val not in {"adaptive", "gray", "none", "off"}:
            errors.append("ocr_preprocess must be adaptive|gray|none|off")
    if "xlsx_all_sheets" in spec and not isinstance(spec.get("xlsx_all_sheets"), bool):
        errors.append("xlsx_all_sheets must be boolean")
    if "standardize_evidence" in spec and not isinstance(spec.get("standardize_evidence"), bool):
        errors.append("standardize_evidence must be boolean")
    if "generate_quality_report" in spec and not isinstance(spec.get("generate_quality_report"), bool):
        errors.append("generate_quality_report must be boolean")
    if "quality_report_path" in spec and not isinstance(spec.get("quality_report_path"), str):
        errors.append("quality_report_path must be string")
    if "evidence_schema" in spec and not isinstance(spec.get("evidence_schema"), dict):
        errors.append("evidence_schema must be object")
    if "detect_conflicts" in spec and not isinstance(spec.get("detect_conflicts"), bool):
        errors.append("detect_conflicts must be boolean")
    if "chunk_mode" in spec and str(spec.get("chunk_mode")).strip().lower() not in {"none", "off", "paragraph", "sentence", "fixed"}:
        errors.append("chunk_mode must be one of none/off/paragraph/sentence/fixed")
    if "chunk_max_chars" in spec:
        try:
            if int(spec.get("chunk_max_chars")) <= 0:
                errors.append("chunk_max_chars must be > 0")
        except Exception:
            errors.append("chunk_max_chars must be integer")
    if "max_retries" in spec:
        try:
            if int(spec.get("max_retries")) < 0:
                errors.append("max_retries must be >= 0")
        except Exception:
            errors.append("max_retries must be integer")
    if "on_file_error" in spec:
        if str(spec.get("on_file_error")).strip().lower() not in {"skip", "raise"}:
            errors.append("on_file_error must be 'skip' or 'raise'")
    if "amount_round_digits" in spec:
        try:
            d = int(spec.get("amount_round_digits"))
            if d < 0 or d > 6:
                errors.append("amount_round_digits must be [0..6]")
        except Exception:
            errors.append("amount_round_digits must be integer")
    if "deduplicate_keep" in spec and str(spec.get("deduplicate_keep")).strip().lower() not in {"first", "last"}:
        errors.append("deduplicate_keep must be 'first' or 'last'")
    if "deduplicate_by" in spec and not isinstance(spec.get("deduplicate_by"), list):
        errors.append("deduplicate_by must be an array")

    allowed_input = {"", "csv", "json", "jsonl"}
    if str(spec.get("input_format") or "").strip().lower() not in allowed_input:
        errors.append("input_format must be one of csv/json/jsonl")
    allowed_output = {"", "csv", "json", "jsonl"}
    if str(spec.get("output_format") or "").strip().lower() not in allowed_output:
        errors.append("output_format must be one of csv/json/jsonl")

    if isinstance(spec.get("field_transforms"), list):
        for i, t in enumerate(spec["field_transforms"]):
            if not isinstance(t, dict):
                errors.append(f"field_transforms[{i}] must be an object")
                continue
            if "field" not in t or "op" not in t:
                errors.append(f"field_transforms[{i}] requires field and op")

    if isinstance(spec.get("row_filters"), list):
        for i, f in enumerate(spec["row_filters"]):
            if not isinstance(f, dict):
                errors.append(f"row_filters[{i}] must be an object")
                continue
            if "op" not in f:
                errors.append(f"row_filters[{i}] requires op")
            if f.get("op") not in {"exists", "not_exists"} and "field" not in f:
                errors.append(f"row_filters[{i}] requires field")

    known = {
        "pipeline",
        "input_format",
        "output_format",
        "input_files",
        "text_split_by_line",
        "ocr_enabled",
        "ocr_lang",
        "ocr_config",
        "ocr_preprocess",
        "xlsx_all_sheets",
        "max_retries",
        "on_file_error",
        "standardize_evidence",
        "evidence_schema",
        "generate_quality_report",
        "quality_report_path",
        "quality_required_fields",
        "chunk_mode",
        "chunk_field",
        "chunk_max_chars",
        "detect_conflicts",
        "conflict_topic_field",
        "conflict_stance_field",
        "conflict_text_field",
        "conflict_positive_words",
        "conflict_negative_words",
        "delimiter",
        "header_map",
        "null_values",
        "amount_fields",
        "date_fields",
        "amount_round_digits",
        "trim_strings",
        "drop_empty_rows",
        "date_output_format",
        "date_input_formats",
        "default_values",
        "include_fields",
        "exclude_fields",
        "field_transforms",
        "row_filters",
        "deduplicate_by",
        "deduplicate_keep",
    }
    unknown = [k for k in spec.keys() if k not in known]
    if unknown:
        warnings.append(f"unknown preprocess keys: {', '.join(sorted(unknown))}")

    pipeline = spec.get("pipeline")
    if pipeline is not None:
        if not isinstance(pipeline, dict):
            errors.append("pipeline must be object")
        else:
            stages = pipeline.get("stages")
            if not isinstance(stages, list) or not stages:
                errors.append("pipeline.stages must be a non-empty array")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def validate_preprocess_pipeline(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(pipeline, dict):
        return {"ok": False, "errors": ["pipeline must be object"], "warnings": []}

    stages = pipeline.get("stages")
    if not isinstance(stages, list) or not stages:
        errors.append("pipeline.stages must be a non-empty array")
        return {"ok": False, "errors": errors, "warnings": warnings}

    allowed = {"extract", "clean", "structure", "audit"}
    for i, stage in enumerate(stages):
        if not isinstance(stage, dict):
            errors.append(f"pipeline.stages[{i}] must be object")
            continue
        name = str(stage.get("name") or "").strip().lower()
        if name not in allowed:
            errors.append(f"pipeline.stages[{i}].name must be one of {sorted(allowed)}")
            continue
        cfg = stage.get("config") if isinstance(stage.get("config"), dict) else {}
        vr = validate_preprocess_spec(cfg)
        if not vr.get("ok"):
            errors.extend([f"pipeline.stages[{i}]: {x}" for x in vr.get("errors", [])])
        warnings.extend([f"pipeline.stages[{i}]: {x}" for x in vr.get("warnings", [])])

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def _stage_output_ext(output_format: str, fallback: str = ".csv") -> str:
    m = str(output_format or "").strip().lower()
    if m == "json":
        return ".json"
    if m == "jsonl":
        return ".jsonl"
    return fallback


def run_preprocess_pipeline(
    *,
    pipeline: Dict[str, Any],
    job_root: str,
    stage_dir: str,
    input_path: str,
    final_output_path: Optional[str] = None,
) -> Dict[str, Any]:
    vr = validate_preprocess_pipeline(pipeline)
    if not vr.get("ok"):
        raise RuntimeError(f"preprocess pipeline invalid: {vr.get('errors')}")

    os.makedirs(stage_dir, exist_ok=True)
    current_input = input_path
    stage_results: List[Dict[str, Any]] = []
    stages = pipeline.get("stages") if isinstance(pipeline.get("stages"), list) else []

    for i, stage in enumerate(stages):
        name = str(stage.get("name") or "").strip().lower()
        cfg = dict(stage.get("config") if isinstance(stage.get("config"), dict) else {})
        if name == "extract":
            cfg.setdefault("output_format", "jsonl")
        elif name == "clean":
            cfg.setdefault("trim_strings", True)
        elif name == "structure":
            cfg.setdefault("standardize_evidence", True)
            cfg.setdefault("output_format", "jsonl")
        elif name == "audit":
            cfg.setdefault("generate_quality_report", True)
            cfg.setdefault("output_format", "jsonl")
            if "quality_report_path" not in cfg:
                cfg["quality_report_path"] = os.path.join(stage_dir, f"pre_stage_{i+1}_audit_quality.json")

        ext = _stage_output_ext(str(cfg.get("output_format") or "csv"))
        stage_output = os.path.join(stage_dir, f"pre_stage_{i+1}_{name}{ext}")
        res = preprocess_file(current_input, stage_output, cfg)
        stage_results.append({"stage": name, "output_path": stage_output, "result": res})
        current_input = stage_output

    final_out = str(final_output_path or os.path.join(stage_dir, "preprocessed_input.csv"))
    if not os.path.isabs(final_out):
        final_out = os.path.join(job_root, final_out)
    final_res = preprocess_file(current_input, final_out, {"output_format": "csv"})

    return {
        "mode": "pipeline",
        "input_path": input_path,
        "output_path": final_out,
        "stages": stage_results,
        "final": final_res,
        "warnings": vr.get("warnings", []),
    }


def preprocess_rows(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    header_map = spec.get("header_map") if isinstance(spec.get("header_map"), dict) else {}
    null_values = [str(x).strip().lower() for x in (spec.get("null_values") or ["null", "none", "na", "n/a"])]
    amount_fields = [str(x) for x in (spec.get("amount_fields") or ["amount"])]
    date_fields = [str(x) for x in (spec.get("date_fields") or [])]
    amount_round_digits = int(spec.get("amount_round_digits", 2))
    trim_strings = bool(spec.get("trim_strings", True))
    drop_empty_rows = bool(spec.get("drop_empty_rows", True))
    date_output_format = str(spec.get("date_output_format", "%Y-%m-%d"))
    date_input_formats = spec.get("date_input_formats") or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
    ]
    defaults = spec.get("default_values") if isinstance(spec.get("default_values"), dict) else {}
    include_fields = [str(x) for x in (spec.get("include_fields") or [])]
    exclude_fields = [str(x) for x in (spec.get("exclude_fields") or [])]
    field_transforms = spec.get("field_transforms") if isinstance(spec.get("field_transforms"), list) else []
    row_filters = spec.get("row_filters") if isinstance(spec.get("row_filters"), list) else []
    deduplicate_by = [str(x) for x in (spec.get("deduplicate_by") or [])]
    deduplicate_keep = str(spec.get("deduplicate_keep") or "first").strip().lower()
    standardize_evidence = bool(spec.get("standardize_evidence", False))
    evidence_schema = spec.get("evidence_schema") if isinstance(spec.get("evidence_schema"), dict) else {}
    chunk_mode = str(spec.get("chunk_mode") or "none").strip().lower()
    chunk_field = str(spec.get("chunk_field") or ("claim_text" if standardize_evidence else "text")).strip()
    chunk_max_chars = int(spec.get("chunk_max_chars", 500))

    out: List[Dict[str, Any]] = []
    dropped_empty = 0
    normalized_amount_cells = 0
    normalized_date_cells = 0
    transformed_cells = 0
    dropped_by_filters = 0
    duplicate_rows_removed = 0
    standardized_rows = 0
    chunked_rows_created = 0

    for raw in rows:
        row: Dict[str, Any] = {}
        for k, v in dict(raw or {}).items():
            nk = header_map.get(k, _normalize_header(k))
            vv = v
            if isinstance(vv, str) and trim_strings:
                vv = vv.strip()
            if isinstance(vv, str) and vv.strip().lower() in null_values:
                vv = None
            row[nk] = vv

        for k, dv in defaults.items():
            if row.get(k) is None:
                row[k] = dv

        for f in amount_fields:
            if f in row and row[f] is not None:
                nv = _normalize_amount(row[f], amount_round_digits)
                if nv != row[f]:
                    normalized_amount_cells += 1
                row[f] = nv

        for f in date_fields:
            if f in row and row[f] is not None:
                nv = _normalize_date(row[f], date_output_format, [str(x) for x in date_input_formats])
                if nv != row[f]:
                    normalized_date_cells += 1
                row[f] = nv

        for t in field_transforms:
            if not isinstance(t, dict):
                continue
            field = str(t.get("field") or "")
            op = str(t.get("op") or "")
            if not field or not op:
                continue
            nv, changed = _apply_field_transform(row.get(field), op, t)
            if changed:
                transformed_cells += 1
            row[field] = nv

        if include_fields:
            row = {k: row.get(k) for k in include_fields}
        for k in exclude_fields:
            row.pop(k, None)

        if row_filters and any(not _filter_match(row, f if isinstance(f, dict) else {}) for f in row_filters):
            dropped_by_filters += 1
            continue

        if drop_empty_rows and all(v is None or str(v).strip() == "" for v in row.values()):
            dropped_empty += 1
            continue

        chunk_targets = _chunk_text(str(row.get(chunk_field) or ""), chunk_mode, chunk_max_chars)
        if not chunk_targets:
            chunk_targets = [None]
        chunked_rows_created += max(0, len(chunk_targets) - 1)
        for ci, chunk_text in enumerate(chunk_targets):
            rr = dict(row)
            if chunk_text is not None:
                rr[chunk_field] = chunk_text
                rr["chunk_seq"] = ci
            if standardize_evidence:
                rr = _to_canonical_evidence_row(rr, evidence_schema)
                standardized_rows += 1
            out.append(rr)

    if deduplicate_by:
        uniq: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        if deduplicate_keep == "last":
            for r in out:
                key = tuple(r.get(k) for k in deduplicate_by)
                uniq[key] = r
            deduped = list(uniq.values())
        else:
            for r in out:
                key = tuple(r.get(k) for k in deduplicate_by)
                if key not in uniq:
                    uniq[key] = r
            deduped = list(uniq.values())
        duplicate_rows_removed = len(out) - len(deduped)
        out = deduped

    out, conflict_rows_marked = _apply_conflict_detection(out, spec)

    summary = {
        "input_rows": len(rows),
        "output_rows": len(out),
        "dropped_empty_rows": dropped_empty,
        "dropped_by_filters": dropped_by_filters,
        "duplicate_rows_removed": duplicate_rows_removed,
        "normalized_amount_cells": normalized_amount_cells,
        "normalized_date_cells": normalized_date_cells,
        "transformed_cells": transformed_cells,
        "standardized_rows": standardized_rows,
        "chunked_rows_created": chunked_rows_created,
        "conflict_rows_marked": conflict_rows_marked,
    }
    return out, summary


def preprocess_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    rows, meta = _read_rows(input_path, spec)
    out_rows, summary = preprocess_rows(rows, spec)
    out_fmt = _write_rows(output_path, out_rows, spec)
    quality_report_path = None
    if bool(spec.get("generate_quality_report", False)):
        quality_report_path = str(spec.get("quality_report_path") or f"{output_path}.quality.json")
        report = _build_quality_report(out_rows, summary, spec)
        _write_json(quality_report_path, report)
    return {
        "input_path": input_path,
        "output_path": output_path,
        "input_format": meta.get("input_format"),
        "output_format": out_fmt,
        "delimiter": meta.get("delimiter"),
        "skipped_files": meta.get("skipped_files"),
        "failed_files": meta.get("failed_files"),
        "quality_report_path": quality_report_path,
        "summary": summary,
    }


def preprocess_csv_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    # Backward-compatible wrapper.
    return preprocess_file(input_path, output_path, spec)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AIWF raw-to-cooked preprocessing")
    p.add_argument("--input", required=True, help="input path (csv/json/jsonl)")
    p.add_argument("--output", required=True, help="output path (csv/json/jsonl)")
    p.add_argument("--config", required=False, help="JSON/YAML config path for preprocess spec")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    spec: Dict[str, Any] = {}
    if args.config:
        with open(args.config, "r", encoding="utf-8-sig") as f:
            text = f.read()
        ext = os.path.splitext(args.config)[1].lower()
        if ext in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except Exception as e:
                print(json.dumps({"ok": False, "errors": [f"yaml support requires pyyaml: {e}"]}, ensure_ascii=False))
                return 2
            loaded = yaml.safe_load(text)
        else:
            loaded = json.loads(text)
        if isinstance(loaded, dict):
            spec = loaded.get("preprocess") if isinstance(loaded.get("preprocess"), dict) else loaded
        else:
            print(json.dumps({"ok": False, "errors": ["config must be an object"]}, ensure_ascii=False))
            return 2

    val = validate_preprocess_spec(spec)
    if not val["ok"]:
        print(json.dumps(val, ensure_ascii=False))
        return 2
    res = preprocess_file(args.input, args.output, spec)
    print(json.dumps({"ok": True, "result": res, "warnings": val.get("warnings", [])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

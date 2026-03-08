from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from aiwf import ingest
from aiwf.preprocess_conflicts import (
    _apply_conflict_detection as _apply_conflict_detection_impl,
    _chunk_text as _chunk_text_impl,
    _detect_polarity as _detect_polarity_impl,
    _infer_topic_key as _infer_topic_key_impl,
)
from aiwf.preprocess_io import (
    _detect_input_format,
    _detect_output_format,
    _read_csv,
    _read_json,
    _read_jsonl,
    _read_rows,
    _write_csv,
    _write_json,
    _write_jsonl,
    _write_rows,
)
from aiwf.preprocess_reporting import (
    _build_quality_report,
    _pick_markdown_text,
    _safe_filename,
    export_canonical_bundle,
)
from aiwf.preprocess_pipeline import (
    _stage_output_ext as _stage_output_ext_impl,
    default_pipeline_stage_executor as _default_pipeline_stage_executor_impl,
    pipeline_stage_output_path as pipeline_stage_output_path_impl,
    run_preprocess_pipeline_impl,
)
from aiwf.preprocess_validation import (
    validate_preprocess_pipeline_impl,
    validate_preprocess_spec_impl,
)
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.registry_utils import infer_caller_module


PreprocessTransformFn = Callable[[Any, Dict[str, Any]], Tuple[Any, bool]]
PreprocessFilterFn = Callable[[Dict[str, Any], Dict[str, Any]], bool]
PipelineStageValidatorFn = Callable[[Dict[str, Any]], Dict[str, Any]]
PipelineStagePrepareFn = Callable[["PipelineStageContext"], Dict[str, Any]]
PipelineStageExecutorFn = Callable[["PipelineStageContext"], Dict[str, Any]]


@dataclass(frozen=True)
class FieldTransformRegistration:
    op: str
    handler: PreprocessTransformFn
    source_module: str


@dataclass(frozen=True)
class RowFilterRegistration:
    op: str
    handler: PreprocessFilterFn
    requires_field: bool
    source_module: str


@dataclass(frozen=True)
class PipelineStageContext:
    stage_index: int
    stage_name: str
    input_path: str
    stage_dir: str
    job_root: str
    config: Dict[str, Any]


@dataclass(frozen=True)
class PipelineStageRegistration:
    name: str
    validator: PipelineStageValidatorFn
    prepare_config: PipelineStagePrepareFn
    executor: Optional[PipelineStageExecutorFn]
    source_module: str


_FIELD_TRANSFORMS: Dict[str, FieldTransformRegistration] = {}
_ROW_FILTERS: Dict[str, RowFilterRegistration] = {}
_PIPELINE_STAGES: Dict[str, PipelineStageRegistration] = {}


def _normalize_preprocess_op(op: str) -> str:
    normalized = str(op or "").strip().lower()
    if not normalized:
        raise ValueError("preprocess op must be non-empty")
    return normalized


def register_field_transform(
    op: str,
    handler: PreprocessTransformFn,
    *,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FieldTransformRegistration:
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("field transform handler must be callable")
    source = str(source_module or infer_caller_module())
    existing = _FIELD_TRANSFORMS.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="field_transform",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"field transform {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="field_transform",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="field_transform",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = FieldTransformRegistration(
        op=normalized,
        handler=handler,
        source_module=source,
    )
    _FIELD_TRANSFORMS[normalized] = registration
    return registration


def unregister_field_transform(op: str) -> Optional[FieldTransformRegistration]:
    normalized = _normalize_preprocess_op(op)
    return _FIELD_TRANSFORMS.pop(normalized, None)


def get_field_transform(op: str) -> FieldTransformRegistration:
    normalized = _normalize_preprocess_op(op)
    registration = _FIELD_TRANSFORMS.get(normalized)
    if registration is None:
        raise KeyError(f"unknown field transform: {normalized}")
    return registration


def list_field_transforms() -> List[str]:
    return sorted(_FIELD_TRANSFORMS.keys())


def list_field_transform_details() -> List[Dict[str, Any]]:
    return [
        {
            "op": registration.op,
            "source_module": registration.source_module,
        }
        for registration in sorted(_FIELD_TRANSFORMS.values(), key=lambda item: item.op)
    ]


def register_row_filter(
    op: str,
    handler: PreprocessFilterFn,
    *,
    requires_field: bool = True,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> RowFilterRegistration:
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("row filter handler must be callable")
    source = str(source_module or infer_caller_module())
    existing = _ROW_FILTERS.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="row_filter",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"row filter {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="row_filter",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="row_filter",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = RowFilterRegistration(
        op=normalized,
        handler=handler,
        requires_field=requires_field,
        source_module=source,
    )
    _ROW_FILTERS[normalized] = registration
    return registration


def unregister_row_filter(op: str) -> Optional[RowFilterRegistration]:
    normalized = _normalize_preprocess_op(op)
    return _ROW_FILTERS.pop(normalized, None)


def get_row_filter(op: str) -> RowFilterRegistration:
    normalized = _normalize_preprocess_op(op)
    registration = _ROW_FILTERS.get(normalized)
    if registration is None:
        raise KeyError(f"unknown row filter: {normalized}")
    return registration


def list_row_filters() -> List[str]:
    return sorted(_ROW_FILTERS.keys())


def list_row_filter_details() -> List[Dict[str, Any]]:
    return [
        {
            "op": registration.op,
            "requires_field": registration.requires_field,
            "source_module": registration.source_module,
        }
        for registration in sorted(_ROW_FILTERS.values(), key=lambda item: item.op)
    ]


def register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> PipelineStageRegistration:
    normalized = _normalize_preprocess_op(name)
    source = str(source_module or infer_caller_module())
    existing = _PIPELINE_STAGES.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="pipeline_stage",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"pipeline stage {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="pipeline_stage",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="pipeline_stage",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = PipelineStageRegistration(
        name=normalized,
        validator=validator or validate_preprocess_spec,
        prepare_config=prepare_config or _default_pipeline_stage_prepare_config,
        executor=executor,
        source_module=source,
    )
    _PIPELINE_STAGES[normalized] = registration
    return registration


def unregister_pipeline_stage(name: str) -> Optional[PipelineStageRegistration]:
    normalized = _normalize_preprocess_op(name)
    return _PIPELINE_STAGES.pop(normalized, None)


def get_pipeline_stage(name: str) -> PipelineStageRegistration:
    normalized = _normalize_preprocess_op(name)
    registration = _PIPELINE_STAGES.get(normalized)
    if registration is None:
        raise KeyError(f"unknown pipeline stage: {normalized}")
    return registration


def list_pipeline_stages() -> List[str]:
    return sorted(_PIPELINE_STAGES.keys())


def list_pipeline_stage_details() -> List[Dict[str, Any]]:
    return [
        {
            "name": registration.name,
            "has_custom_executor": registration.executor is not None,
            "source_module": registration.source_module,
        }
        for registration in sorted(_PIPELINE_STAGES.values(), key=lambda item: item.name)
    ]


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


def _filter_field(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("field") or "").strip()


def _filter_value(row: Dict[str, Any], cfg: Dict[str, Any]) -> Any:
    field = _filter_field(cfg)
    if not field:
        return None
    return row.get(field)


def _transform_trim(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.strip(), True
    return value, False


def _transform_lower(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.lower(), True
    return value, False


def _transform_upper(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.upper(), True
    return value, False


def _transform_collapse_whitespace(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip(), True
    return value, False


def _transform_remove_urls(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"https?://\S+|www\.\S+", "", value).strip(), True
    return value, False


def _transform_remove_emails(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "", value).strip(), True
    return value, False


def _transform_regex_replace(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if not isinstance(value, str):
        return value, False
    pat = str(cfg.get("pattern") or "")
    rep = str(cfg.get("replace") or "")
    try:
        return re.sub(pat, rep, value), True
    except re.error:
        return value, False


def _transform_parse_number(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    parsed = _to_float(value)
    return (parsed if parsed is not None else value), (parsed is not None)


def _transform_round_number(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    digits = int(cfg.get("digits", 2))
    parsed = _to_float(value)
    if parsed is None:
        return value, False
    return round(parsed, digits), True


def _transform_parse_date(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    out_fmt = str(cfg.get("output_format") or "%Y-%m-%d")
    in_fmts = cfg.get("input_formats") or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
    ]
    if isinstance(value, str):
        normalized = _normalize_date(value, out_fmt, [str(x) for x in in_fmts])
        return normalized, normalized != value
    return value, False


def _transform_extract_regex(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if not isinstance(value, str):
        return value, False
    pat = str(cfg.get("pattern") or "")
    group = int(cfg.get("group", 0))
    try:
        match = re.search(pat, value)
    except re.error:
        return value, False
    if not match:
        return value, False
    try:
        return match.group(group), True
    except IndexError:
        return value, False


def _filter_exists(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return row.get(field) is not None


def _filter_not_exists(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return row.get(field) is None


def _filter_eq(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return _filter_value(row, cfg) == cfg.get("value")


def _filter_ne(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return _filter_value(row, cfg) != cfg.get("value")


def _filter_compare_numeric(row: Dict[str, Any], cfg: Dict[str, Any], op: str) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    a = _to_float(_filter_value(row, cfg))
    b = _to_float(cfg.get("value"))
    if a is None or b is None:
        return False
    if op == "gt":
        return a > b
    if op == "gte":
        return a >= b
    if op == "lt":
        return a < b
    return a <= b


def _filter_gt(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "gt")


def _filter_gte(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "gte")


def _filter_lt(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "lt")


def _filter_lte(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "lte")


def _filter_in(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    arr = cfg.get("value") if isinstance(cfg.get("value"), list) else []
    return _filter_value(row, cfg) in arr


def _filter_not_in(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    arr = cfg.get("value") if isinstance(cfg.get("value"), list) else []
    return _filter_value(row, cfg) not in arr


def _filter_contains(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return str(cfg.get("value")) in str(_filter_value(row, cfg))


def _filter_regex(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    try:
        return re.search(str(cfg.get("value")), str(_filter_value(row, cfg))) is not None
    except re.error:
        return False


def _filter_not_regex(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    try:
        return re.search(str(cfg.get("value")), str(_filter_value(row, cfg))) is None
    except re.error:
        return False


def _register_builtin_preprocess_ops() -> None:
    register_field_transform("trim", _transform_trim)
    register_field_transform("lower", _transform_lower)
    register_field_transform("upper", _transform_upper)
    register_field_transform("collapse_whitespace", _transform_collapse_whitespace)
    register_field_transform("remove_urls", _transform_remove_urls)
    register_field_transform("remove_emails", _transform_remove_emails)
    register_field_transform("regex_replace", _transform_regex_replace)
    register_field_transform("parse_number", _transform_parse_number)
    register_field_transform("round_number", _transform_round_number)
    register_field_transform("parse_date", _transform_parse_date)
    register_field_transform("extract_regex", _transform_extract_regex)

    register_row_filter("exists", _filter_exists, requires_field=False)
    register_row_filter("not_exists", _filter_not_exists, requires_field=False)
    register_row_filter("eq", _filter_eq)
    register_row_filter("ne", _filter_ne)
    register_row_filter("gt", _filter_gt)
    register_row_filter("gte", _filter_gte)
    register_row_filter("lt", _filter_lt)
    register_row_filter("lte", _filter_lte)
    register_row_filter("in", _filter_in)
    register_row_filter("not_in", _filter_not_in)
    register_row_filter("contains", _filter_contains)
    register_row_filter("regex", _filter_regex)
    register_row_filter("not_regex", _filter_not_regex)


_register_builtin_preprocess_ops()


def _filter_match(row: Dict[str, Any], f: Dict[str, Any]) -> bool:
    op = str(f.get("op") or "eq").strip().lower()
    try:
        return get_row_filter(op).handler(row, f)
    except KeyError:
        return True


def _apply_field_transform(value: Any, op: str, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    try:
        return get_field_transform(op).handler(value, cfg)
    except KeyError:
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
    return _chunk_text_impl(text, mode, max_chars)


def _infer_topic_key(text: Any, ignore_words: Optional[List[str]] = None) -> str:
    return _infer_topic_key_impl(text, ignore_words=ignore_words)


def _detect_polarity(text: Any, positive_words: List[str], negative_words: List[str]) -> str:
    return _detect_polarity_impl(text, positive_words, negative_words)


def _apply_conflict_detection(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    return _apply_conflict_detection_impl(rows, spec)


def validate_preprocess_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    return validate_preprocess_spec_impl(
        spec,
        field_transform_ops=list_field_transforms(),
        row_filter_specs={registration.op: registration.requires_field for registration in _ROW_FILTERS.values()},
    )


def validate_preprocess_pipeline(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    return validate_preprocess_pipeline_impl(
        pipeline,
        list_pipeline_stages=list_pipeline_stages,
        get_pipeline_registration=get_pipeline_stage,
    )


def _default_pipeline_stage_prepare_config(context: PipelineStageContext) -> Dict[str, Any]:
    return dict(context.config)


def _stage_output_ext(output_format: str, fallback: str = ".csv") -> str:
    return _stage_output_ext_impl(output_format, fallback=fallback)


def pipeline_stage_output_path(context: PipelineStageContext) -> str:
    return pipeline_stage_output_path_impl(context)


def _default_pipeline_stage_executor(
    context: PipelineStageContext,
    *,
    preprocess_file: Optional[Callable[[str, str, Dict[str, Any]], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    runner = preprocess_file or globals()["preprocess_file"]
    return _default_pipeline_stage_executor_impl(context, preprocess_file=runner)


def _prepare_extract_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("output_format", "jsonl")
    return cfg


def _prepare_clean_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("trim_strings", True)
    return cfg


def _prepare_structure_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("standardize_evidence", True)
    cfg.setdefault("output_format", "jsonl")
    return cfg


def _prepare_audit_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("generate_quality_report", True)
    cfg.setdefault("output_format", "jsonl")
    if "quality_report_path" not in cfg:
        cfg["quality_report_path"] = os.path.join(
            context.stage_dir,
            f"pre_stage_{context.stage_index+1}_audit_quality.json",
        )
    return cfg


def _register_builtin_pipeline_stages() -> None:
    register_pipeline_stage("extract", prepare_config=_prepare_extract_stage_config)
    register_pipeline_stage("clean", prepare_config=_prepare_clean_stage_config)
    register_pipeline_stage("structure", prepare_config=_prepare_structure_stage_config)
    register_pipeline_stage("audit", prepare_config=_prepare_audit_stage_config)


_register_builtin_pipeline_stages()


def run_preprocess_pipeline(
    *,
    pipeline: Dict[str, Any],
    job_root: str,
    stage_dir: str,
    input_path: str,
    final_output_path: Optional[str] = None,
) -> Dict[str, Any]:
    return run_preprocess_pipeline_impl(
        pipeline=pipeline,
        job_root=job_root,
        stage_dir=stage_dir,
        input_path=input_path,
        final_output_path=final_output_path,
        validate_pipeline=validate_preprocess_pipeline,
        get_pipeline_stage=get_pipeline_stage,
        pipeline_stage_context_type=PipelineStageContext,
        default_stage_executor=_default_pipeline_stage_executor,
        preprocess_file=preprocess_file,
    )


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
    canonical_bundle = None
    if bool(spec.get("export_canonical_bundle", False)):
        canonical_bundle = export_canonical_bundle(
            rows=out_rows,
            summary=summary,
            meta=meta,
            output_path=output_path,
            spec=spec,
        )
    return {
        "input_path": input_path,
        "output_path": output_path,
        "input_format": meta.get("input_format"),
        "output_format": out_fmt,
        "delimiter": meta.get("delimiter"),
        "skipped_files": meta.get("skipped_files"),
        "failed_files": meta.get("failed_files"),
        "quality_report_path": quality_report_path,
        "canonical_bundle": canonical_bundle,
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

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from aiwf.office_style import (
    office_rows_subset as _office_rows_subset,
    office_theme_settings as _office_theme_settings,
    office_quality_mode as _office_quality_mode,
)
from aiwf.office_outputs import (
    write_profile_illustration_png as _office_write_profile_illustration_png,
    write_fin_xlsx as _office_write_fin_xlsx,
    write_audit_docx as _office_write_audit_docx,
    write_deck_pptx as _office_write_deck_pptx,
)
from aiwf.flows.cleaning_simple_rules import clean_rows_simple as _clean_rows_simple
from aiwf.flows.cleaning_generic_rules import clean_rows_generic as _clean_rows_generic_external


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _write_profile_illustration_png(path: str, profile: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> bool:
    return _office_write_profile_illustration_png(path, profile, params, utc_now_str=_utc_now_str)


def _is_valid_parquet_file(path: str) -> bool:
    # Lightweight parquet validation using magic bytes.
    if not path or not os.path.isfile(path):
        return False
    try:
        size = os.path.getsize(path)
        if size < 8:
            return False
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
        return head == b"PAR1" and tail == b"PAR1"
    except Exception:
        return False


def _headers_from_params(params: Dict[str, Any]) -> Dict[str, str]:
    api_key = params.get("api_key") or os.getenv("AIWF_API_KEY")
    if api_key:
        return {"X-API-Key": str(api_key)}
    return {}


def _post_json(url: str, body: Dict[str, Any], headers: Dict[str, str]) -> None:
    import requests

    r = requests.post(url, json=body, headers=headers, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")


def _try_accel_cleaning(
    params: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
) -> Dict[str, Any]:
    """
    Prefer accel-rust first. If it fails or output is invalid, caller falls back locally.
    """
    import requests

    accel_url = str(params.get("accel_url") or os.getenv("AIWF_ACCEL_URL") or "http://127.0.0.1:18082").rstrip("/")
    timeout = float(params.get("accel_timeout_seconds") or os.getenv("AIWF_ACCEL_TIMEOUT", "10"))

    payload = {
        "job_id": job_id,
        "step_id": step_id,
        "actor": actor,
        "ruleset_version": ruleset_version,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "job_root": params.get("job_root"),
        "params": params,
        # Test-only switch for fallback integration verification.
        "force_bad_parquet": bool(params.get("accel_force_bad_parquet", False)),
    }

    try:
        url = f"{accel_url}/operators/cleaning"
        resp = requests.post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": f"{resp.status_code} {resp.text}",
            }

        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}

        return {
            "attempted": True,
            "ok": True,
            "url": url,
            "response": body,
        }
    except Exception as e:
        return {
            "attempted": True,
            "ok": False,
            "url": f"{accel_url}/operators/cleaning",
            "error": str(e),
        }


def _try_rust_transform_rows_v2(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    import requests

    accel_url = str(params.get("accel_url") or os.getenv("AIWF_ACCEL_URL") or "http://127.0.0.1:18082").rstrip("/")
    timeout = float(params.get("rust_v2_timeout_seconds") or os.getenv("AIWF_RUST_V2_TIMEOUT", "8"))
    rules = _rules_dict(params)
    payload = {
        "run_id": str(params.get("job_id") or ""),
        "rows": raw_rows,
        "rules": rules,
        "quality_gates": {
            "max_invalid_rows": _rule_param(params, "max_invalid_rows"),
            "min_output_rows": _rule_param(params, "min_output_rows"),
            "max_invalid_ratio": _rule_param(params, "max_invalid_ratio"),
            "required_fields": _rule_param(params, "required_fields"),
            "max_required_missing_ratio": _rule_param(params, "max_required_missing_ratio"),
        },
        "schema_hint": {"source": "glue-python.cleaning"},
    }

    try:
        url = f"{accel_url}/operators/transform_rows_v2"
        resp = requests.post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": f"{resp.status_code} {resp.text}",
            }
        body = resp.json()
        rows = body.get("rows") if isinstance(body, dict) else None
        quality = body.get("quality") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not isinstance(quality, dict):
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": "transform_rows_v2 invalid response shape",
                "response": body,
            }
        quality2 = dict(quality)
        quality2["rust_v2_used"] = True
        quality2["rust_v2_trace_id"] = str(body.get("trace_id") or "")
        return {
            "attempted": True,
            "ok": True,
            "url": url,
            "rows": rows,
            "quality": quality2,
            "response": body,
        }
    except Exception as e:
        return {
            "attempted": True,
            "ok": False,
            "url": f"{accel_url}/operators/transform_rows_v2",
            "error": str(e),
        }


def _base_step_start(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
    params: Dict[str, Any],
    headers: Dict[str, str],
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/start?actor={actor}"
    body = {
        "ruleset_version": ruleset_version,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "params": params or {},
    }
    _post_json(url, body, headers)


def _base_step_done(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    output_hash: str,
    headers: Dict[str, str],
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/done?actor={actor}"
    body = {"output_hash": output_hash}
    _post_json(url, body, headers)


def _base_step_fail(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    error: str,
    headers: Dict[str, str],
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/fail?actor={actor}"
    body = {"error": error}
    _post_json(url, body, headers)


def _base_artifact_upsert(
    base_url: str,
    job_id: str,
    actor: str,
    artifact_id: str,
    kind: str,
    path: str,
    sha256: str,
    extra_json: Optional[str],
    headers: Dict[str, str],
) -> None:
    import requests

    candidates = [
        f"{base_url}/api/v1/jobs/{job_id}/artifacts/upsert?actor={actor}",
        f"{base_url}/api/v1/jobs/{job_id}/artifacts?actor={actor}",
        f"{base_url}/api/v1/jobs/{job_id}/artifacts/register?actor={actor}",
    ]
    body = {
        "artifact_id": artifact_id,
        "kind": kind,
        "path": path,
        "sha256": sha256,
        "extra_json": extra_json,
    }

    last_err = None
    for url in candidates:
        try:
            r = requests.post(url, json=body, headers=headers, timeout=30)
            if r.status_code < 400:
                return
            last_err = f"{r.status_code} {r.text}"
        except Exception as e:
            last_err = str(e)

    raise RuntimeError(f"artifact upsert failed, tried {candidates}, last_err={last_err}")


def _normalize_key(k: str) -> str:
    return (k or "").strip().lower()


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if "." in s:
            return int(float(s))
        return int(s)
    except Exception:
        return None


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


def _to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "")
    if s.startswith("$"):
        s = s[1:]
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _quantize_decimal(v: Decimal, digits: int) -> Decimal:
    q = Decimal("1").scaleb(-digits)
    return v.quantize(q, rounding=ROUND_HALF_UP)


def _rules_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    rules = params.get("rules")
    if isinstance(rules, dict):
        return rules
    return {}


def _rule_param(params: Dict[str, Any], key: str, default: Any = None) -> Any:
    rules = _rules_dict(params)
    if key in rules:
        return rules.get(key)
    return params.get(key, default)


def _is_generic_rules_enabled(params: Dict[str, Any]) -> bool:
    rules = _rules_dict(params)
    if str(rules.get("platform_mode", "")).strip().lower() == "generic":
        return True
    generic_keys = {
        "rename_map",
        "casts",
        "filters",
        "required_fields",
        "default_values",
        "include_fields",
        "exclude_fields",
        "deduplicate_by",
        "sort_by",
        "null_values",
        "trim_strings",
        "lowercase_fields",
        "uppercase_fields",
    }
    return any(k in rules for k in generic_keys)


def validate_cleaning_rules(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate declarative cleaning rules and return structured diagnostics.
    Accepts full params payload or a direct rules object.
    """
    if not isinstance(params, dict):
        return {"ok": False, "errors": ["params must be an object"], "warnings": []}

    rules = params.get("rules") if isinstance(params.get("rules"), dict) else params
    if not isinstance(rules, dict):
        return {"ok": False, "errors": ["rules must be an object"], "warnings": []}

    errors: List[str] = []
    warnings: List[str] = []

    allowed_keys = {
        "platform_mode",
        "rename_map",
        "casts",
        "filters",
        "required_fields",
        "default_values",
        "include_fields",
        "exclude_fields",
        "deduplicate_by",
        "deduplicate_keep",
        "sort_by",
        "null_values",
        "trim_strings",
        "lowercase_fields",
        "uppercase_fields",
        "id_field",
        "amount_field",
        "amount_round_digits",
        "drop_negative_amount",
        "min_amount",
        "max_amount",
        "deduplicate_by_id",
        "sort_by_id",
        "allow_empty_output",
        "local_parquet_strict",
        "max_invalid_rows",
        "max_filtered_rows",
        "min_output_rows",
        "max_invalid_ratio",
        "max_required_missing_ratio",
        "force_local_cleaning",
        "use_rust_v2",
        "rust_v2_timeout_seconds",
    }
    unknown = [k for k in rules.keys() if k not in allowed_keys]
    if unknown:
        warnings.append(f"unknown rule keys: {', '.join(sorted(unknown))}")

    if "platform_mode" in rules and str(rules.get("platform_mode", "")).strip().lower() not in {"generic", ""}:
        errors.append("platform_mode must be 'generic' when provided")

    if "rename_map" in rules and not isinstance(rules.get("rename_map"), dict):
        errors.append("rename_map must be an object")
    if "casts" in rules and not isinstance(rules.get("casts"), dict):
        errors.append("casts must be an object")
    if "default_values" in rules and not isinstance(rules.get("default_values"), dict):
        errors.append("default_values must be an object")

    for key in ["required_fields", "include_fields", "exclude_fields", "deduplicate_by", "lowercase_fields", "uppercase_fields", "null_values"]:
        if key in rules and not isinstance(rules.get(key), list):
            errors.append(f"{key} must be an array")

    if "filters" in rules:
        filters = rules.get("filters")
        if not isinstance(filters, list):
            errors.append("filters must be an array")
        else:
            allowed_ops = {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in", "contains", "regex", "exists", "not_exists"}
            for i, f in enumerate(filters):
                if not isinstance(f, dict):
                    errors.append(f"filters[{i}] must be an object")
                    continue
                op = str(f.get("op", "eq")).strip().lower()
                if op not in allowed_ops:
                    errors.append(f"filters[{i}].op must be one of {sorted(allowed_ops)}")
                if op not in {"exists", "not_exists"} and "field" not in f:
                    errors.append(f"filters[{i}].field is required")

    if "deduplicate_keep" in rules:
        keep = str(rules.get("deduplicate_keep", "")).strip().lower()
        if keep not in {"first", "last"}:
            errors.append("deduplicate_keep must be 'first' or 'last'")

    if "sort_by" in rules:
        sort_by = rules.get("sort_by")
        if not isinstance(sort_by, list):
            errors.append("sort_by must be an array")
        else:
            for i, s in enumerate(sort_by):
                if isinstance(s, str):
                    continue
                if not isinstance(s, dict):
                    errors.append(f"sort_by[{i}] must be a string or object")
                    continue
                order = str(s.get("order", "asc")).strip().lower()
                if order not in {"asc", "desc"}:
                    errors.append(f"sort_by[{i}].order must be 'asc' or 'desc'")

    if "amount_round_digits" in rules:
        d = _to_int(rules.get("amount_round_digits"))
        if d is None or d < 0 or d > 6:
            errors.append("amount_round_digits must be integer in range [0,6]")

    if "max_invalid_ratio" in rules:
        r = _to_decimal(rules.get("max_invalid_ratio"))
        if r is None or r < 0 or r > 1:
            errors.append("max_invalid_ratio must be a number in [0,1]")
    if "max_required_missing_ratio" in rules:
        r2 = _to_decimal(rules.get("max_required_missing_ratio"))
        if r2 is None or r2 < 0 or r2 > 1:
            errors.append("max_required_missing_ratio must be a number in [0,1]")

    for key in ["max_invalid_rows", "max_filtered_rows", "min_output_rows"]:
        if key in rules:
            iv = _to_int(rules.get(key))
            if iv is None or iv < 0:
                errors.append(f"{key} must be a non-negative integer")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def _local_parquet_strict_enabled(params: Dict[str, Any]) -> bool:
    if "local_parquet_strict" in params or "local_parquet_strict" in _rules_dict(params):
        return _to_bool(_rule_param(params, "local_parquet_strict", True), default=True)
    return _to_bool(os.getenv("AIWF_GLUE_LOCAL_PARQUET_STRICT", "true"), default=True)


def _require_local_parquet_dependencies(params: Dict[str, Any]) -> None:
    if not _local_parquet_strict_enabled(params):
        return
    try:
        import pandas as _  # type: ignore
    except Exception as e:
        raise RuntimeError(f"local parquet strict mode: missing pandas ({e})")
    try:
        import pyarrow as _  # type: ignore
    except Exception as e:
        raise RuntimeError(f"local parquet strict mode: missing pyarrow ({e})")


def _resolve_csv_source_path(params: Dict[str, Any], job_root: Optional[str]) -> Optional[str]:
    candidate = (
        params.get("input_csv_path")
        or params.get("source_csv_path")
        or params.get("csv_path")
        or params.get("input_uri")
    )
    if not candidate:
        return None
    path = str(candidate).strip()
    if not path:
        return None
    if path.startswith("file://"):
        path = path[len("file://") :]
    if os.path.isabs(path):
        return path
    if job_root:
        return os.path.join(job_root, path)
    return path


def _parse_rows_from_csv_text(csv_text: str) -> List[Dict[str, Any]]:
    if not csv_text or not csv_text.strip():
        return []
    out: List[Dict[str, Any]] = []
    reader = csv.DictReader(csv_text.strip().splitlines())
    for row in reader:
        out.append(dict(row))
    return out


def _read_text_file_with_fallback(path: str, encodings: Optional[List[str]] = None) -> str:
    tried = encodings or ["utf-8-sig", "utf-8", "gb18030", "gbk"]
    last_err: Optional[Exception] = None
    for enc in tried:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return f.read()
        except Exception as e:
            last_err = e
    raise RuntimeError(f"failed to decode text file with fallback encodings: {path}, last_err={last_err}")


def _load_raw_rows(params: Dict[str, Any], job_root: Optional[str]) -> Tuple[List[Dict[str, Any]], str]:
    if isinstance(params.get("rows"), list) and params["rows"]:
        return list(params["rows"]), "params.rows"

    csv_text = params.get("csv_text")
    if isinstance(csv_text, str) and csv_text.strip():
        rows = _parse_rows_from_csv_text(csv_text)
        if rows:
            return rows, "params.csv_text"

    source_path = _resolve_csv_source_path(params, job_root)
    if source_path and os.path.isfile(source_path):
        csv_text_file = _read_text_file_with_fallback(source_path)
        with io.StringIO(csv_text_file) as f:
            reader = csv.DictReader(f)
            rows = [dict(r) for r in reader]
        if rows:
            return rows, source_path

    return [{"id": 1, "amount": 100.0}, {"id": 2, "amount": 200.0}], "default.sample"


def _maybe_preprocess_input(params: Dict[str, Any], job_root: str, stage_dir: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Optional raw-to-cooked preprocessing entry.
    This is intentionally decoupled from cleaning rules and runs only when enabled.
    """
    preprocess_cfg = params.get("preprocess") if isinstance(params.get("preprocess"), dict) else {}
    enabled = _to_bool(preprocess_cfg.get("enabled"), default=False)
    input_csv = preprocess_cfg.get("input_path") or params.get("input_csv_path")
    if not enabled or not input_csv:
        return params, None

    from aiwf.preprocess import (
        preprocess_csv_file,
        run_preprocess_pipeline,
        validate_preprocess_pipeline,
        validate_preprocess_spec,
    )  # local import keeps loose coupling

    input_path = str(input_csv)
    if not os.path.isabs(input_path):
        input_path = os.path.join(job_root, input_path)
    output_path = str(preprocess_cfg.get("output_path") or os.path.join(stage_dir, "preprocessed_input.csv"))
    if not os.path.isabs(output_path):
        output_path = os.path.join(job_root, output_path)

    pipeline_cfg = preprocess_cfg.get("pipeline") if isinstance(preprocess_cfg.get("pipeline"), dict) else None
    if pipeline_cfg and (pipeline_cfg.get("enabled", True)):
        pipeline = dict(pipeline_cfg)
        pipeline.pop("enabled", None)
        vr = validate_preprocess_pipeline(pipeline)
        if not vr.get("ok"):
            raise RuntimeError(f"preprocess pipeline invalid: {vr.get('errors')}")
        res = run_preprocess_pipeline(
            pipeline=pipeline,
            job_root=job_root,
            stage_dir=stage_dir,
            input_path=input_path,
            final_output_path=output_path,
        )
        output_path = res.get("output_path", output_path)
    else:
        spec = dict(preprocess_cfg)
        spec.pop("enabled", None)
        spec.pop("input_path", None)
        spec.pop("output_path", None)
        vr = validate_preprocess_spec(spec)
        if not vr.get("ok"):
            raise RuntimeError(f"preprocess config invalid: {vr.get('errors')}")
        res = preprocess_csv_file(input_path, output_path, spec)

    next_params = dict(params)
    next_params["input_csv_path"] = output_path
    return next_params, res


def _clean_rows(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    rust_v2_enabled = _to_bool(_rule_param(params, "use_rust_v2", os.getenv("AIWF_RUST_V2_ENABLED", "false")), default=False)
    if rust_v2_enabled and not _is_generic_rules_enabled(params):
        rust_v2 = _try_rust_transform_rows_v2(raw_rows, params)
        if rust_v2.get("ok"):
            return {
                "rows": rust_v2["rows"],
                "quality": rust_v2["quality"],
            }

    out = _clean_rows_simple(
        raw_rows,
        params,
        hooks={
            "is_generic_rules_enabled": _is_generic_rules_enabled,
            "clean_rows_generic": _clean_rows_generic,
            "to_int": _to_int,
            "to_bool": _to_bool,
            "rule_param": _rule_param,
            "to_decimal": _to_decimal,
            "normalize_key": _normalize_key,
            "quantize_decimal": _quantize_decimal,
        },
    )
    if rust_v2_enabled:
        q = dict(out.get("quality") or {})
        q["rust_v2_used"] = False
        q["rust_v2_error"] = str((locals().get("rust_v2") or {}).get("error") or "")
        out["quality"] = q
    return out


def _clean_rows_generic(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    return _clean_rows_generic_external(
        raw_rows,
        params,
        hooks={
            "rules_dict": _rules_dict,
            "to_bool": _to_bool,
            "to_int": _to_int,
            "to_float": _to_float,
        },
    )


def _build_profile(rows: List[Dict[str, Any]], quality: Dict[str, Any], source: str) -> Dict[str, Any]:
    field_set = set()
    for r in rows:
        field_set.update(r.keys())

    numeric_stats: Dict[str, Dict[str, float]] = {}
    for field in sorted(field_set):
        vals: List[Decimal] = []
        for r in rows:
            d = _to_decimal(r.get(field))
            if d is not None:
                vals.append(d)
        if vals:
            total_f = sum(vals, Decimal("0"))
            min_f = min(vals)
            max_f = max(vals)
            avg_f = total_f / Decimal(len(vals))
            numeric_stats[field] = {
                "sum": float(_quantize_decimal(total_f, 2)),
                "min": float(_quantize_decimal(min_f, 2)),
                "max": float(_quantize_decimal(max_f, 2)),
                "avg": float(_quantize_decimal(avg_f, 2)),
            }

    amounts = [_to_decimal(r.get("amount")) or Decimal("0") for r in rows]
    total = sum(amounts, Decimal("0")) if amounts else Decimal("0")
    min_amount = min(amounts) if amounts else Decimal("0")
    max_amount = max(amounts) if amounts else Decimal("0")
    avg_amount = (total / Decimal(len(amounts))) if amounts else Decimal("0")
    return {
        "rows": len(rows),
        "cols": 2,
        "sum_amount": float(_quantize_decimal(total, 2)),
        "min_amount": float(_quantize_decimal(min_amount, 2)),
        "max_amount": float(_quantize_decimal(max_amount, 2)),
        "avg_amount": float(_quantize_decimal(avg_amount, 2)),
        "quality": quality,
        "fields": sorted(field_set),
        "numeric_stats": numeric_stats,
        "source": source,
    }


def _apply_quality_gates(quality: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enforce optional quality thresholds. Raises RuntimeError when a gate is violated.
    Rules can be provided via top-level params or params.rules.
    """
    input_rows = int(quality.get("input_rows", 0))
    output_rows = int(quality.get("output_rows", 0))
    invalid_rows = int(quality.get("invalid_rows", 0))
    filtered_rows = int(quality.get("filtered_rows", 0))

    max_invalid_rows = _to_int(_rule_param(params, "max_invalid_rows"))
    max_filtered_rows = _to_int(_rule_param(params, "max_filtered_rows"))
    min_output_rows = _to_int(_rule_param(params, "min_output_rows"))
    max_invalid_ratio = _to_decimal(_rule_param(params, "max_invalid_ratio"))

    if max_invalid_rows is not None and invalid_rows > max_invalid_rows:
        raise RuntimeError(
            f"quality gate failed: invalid_rows={invalid_rows} exceeds max_invalid_rows={max_invalid_rows}"
        )
    if max_filtered_rows is not None and filtered_rows > max_filtered_rows:
        raise RuntimeError(
            f"quality gate failed: filtered_rows={filtered_rows} exceeds max_filtered_rows={max_filtered_rows}"
        )
    if min_output_rows is not None and output_rows < min_output_rows:
        raise RuntimeError(
            f"quality gate failed: output_rows={output_rows} below min_output_rows={min_output_rows}"
        )
    if max_invalid_ratio is not None:
        ratio = (Decimal(invalid_rows) / Decimal(input_rows)) if input_rows > 0 else Decimal("0")
        if ratio > max_invalid_ratio:
            raise RuntimeError(
                f"quality gate failed: invalid_ratio={float(ratio):.6f} exceeds max_invalid_ratio={float(max_invalid_ratio):.6f}"
            )

    return {
        "max_invalid_rows": max_invalid_rows,
        "max_filtered_rows": max_filtered_rows,
        "min_output_rows": min_output_rows,
        "max_invalid_ratio": (float(max_invalid_ratio) if max_invalid_ratio is not None else None),
        "evaluated": True,
    }


def _default_rows(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Backward-compatible wrapper used by older tests/callers.
    rows, _ = _load_raw_rows(params, None)
    return rows


def _write_cleaned_csv(csv_path: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    if rows:
        columns = list(rows[0].keys())
        seen = set(columns)
        for r in rows[1:]:
            for k in r.keys():
                if k not in seen:
                    columns.append(k)
                    seen.add(k)
    else:
        columns = ["id", "amount"]

    with open(csv_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(",".join(columns) + "\n")
        for r in rows:
            vals = ["" if r.get(c) is None else str(r.get(c)) for c in columns]
            f.write(",".join(vals) + "\n")
    return {"rows": len(rows), "cols": len(columns)}


def _write_cleaned_parquet(parquet_path: str, rows: List[Dict[str, Any]]) -> None:
    try:
        import pandas as pd  # type: ignore

        df = pd.DataFrame(rows)
        df.to_parquet(parquet_path, index=False)
    except Exception:
        # Placeholder remains as compatibility fallback if parquet libs are unavailable.
        with open(parquet_path, "wb") as f:
            f.write(b"PARQUET_PLACEHOLDER\n")


def _write_fin_xlsx(
    xlsx_path: str,
    rows: List[Dict[str, Any]],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    _office_write_fin_xlsx(
        xlsx_path,
        rows,
        image_path,
        params,
        to_decimal=_to_decimal,
        build_profile=_build_profile,
        utc_now_str=_utc_now_str,
    )


def _write_audit_docx(
    docx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    _office_write_audit_docx(docx_path, job_id, profile, image_path, params, utc_now_str=_utc_now_str)


def _write_deck_pptx(
    pptx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    _office_write_deck_pptx(pptx_path, job_id, profile, image_path, params, utc_now_str=_utc_now_str)


def _write_profile_json(profile_path: str, profile: Dict[str, Any], params: Dict[str, Any]) -> None:
    payload = {
        "profile": profile,
        "params": params or {},
    }
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)



from aiwf.flows.cleaning_orchestrator import run_cleaning_flow as _run_cleaning_flow


def run_cleaning(
    job_id: str,
    actor: str = "glue",
    ruleset_version: str = "v1",
    params: Optional[Dict[str, Any]] = None,
    s: Optional[Any] = None,
    base: Optional[Any] = None,
) -> Dict[str, Any]:
    return _run_cleaning_flow(
        job_id=job_id,
        actor=actor,
        ruleset_version=ruleset_version,
        params=params or {},
        s=s,
        base=base,
        hooks={
            "_ensure_dirs": _ensure_dirs,
            "_load_raw_rows": _load_raw_rows,
            "_clean_rows": _clean_rows,
            "_rules_dict": _rules_dict,
            "_to_bool": _to_bool,
            "_rule_param": _rule_param,
            "_is_generic_rules_enabled": _is_generic_rules_enabled,
            "_try_accel_cleaning": _try_accel_cleaning,
            "_is_valid_parquet_file": _is_valid_parquet_file,
            "_office_rows_subset": _office_rows_subset,
            "_build_profile": _build_profile,
            "_write_profile_illustration_png": _write_profile_illustration_png,
            "_write_fin_xlsx": _write_fin_xlsx,
            "_write_audit_docx": _write_audit_docx,
            "_write_deck_pptx": _write_deck_pptx,
            "_sha256_file": _sha256_file,
            "_apply_quality_gates": _apply_quality_gates,
            "_require_local_parquet_dependencies": _require_local_parquet_dependencies,
            "_write_cleaned_csv": _write_cleaned_csv,
            "_write_cleaned_parquet": _write_cleaned_parquet,
            "_local_parquet_strict_enabled": _local_parquet_strict_enabled,
            "_write_profile_json": _write_profile_json,
            "_base_step_start": _base_step_start,
            "_headers_from_params": _headers_from_params,
            "_maybe_preprocess_input": _maybe_preprocess_input,
            "_base_artifact_upsert": _base_artifact_upsert,
            "_base_step_done": _base_step_done,
            "_base_step_fail": _base_step_fail,
        },
    )

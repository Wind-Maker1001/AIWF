from __future__ import annotations

import hashlib
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
from aiwf.flows.artifact_selection import validate_artifact_selection_config_with_tokens
from aiwf.flows.cleaning_artifacts import list_cleaning_artifact_tokens
from aiwf.flows.office_artifacts import list_office_artifact_tokens
from aiwf.flows.cleaning_inputs import (
    load_raw_rows_impl,
    local_parquet_strict_enabled_impl,
    maybe_preprocess_input_impl,
    parse_rows_from_csv_text_impl,
    read_text_file_with_fallback_impl,
    require_local_parquet_dependencies_impl,
    resolve_csv_source_path_impl,
)
from aiwf.flows.cleaning_outputs import (
    write_audit_docx_impl,
    write_cleaned_csv_impl,
    write_cleaned_parquet_impl,
    write_deck_pptx_impl,
    write_fin_xlsx_impl,
    write_profile_json_impl,
)
from aiwf.flows.cleaning_profile import build_profile_impl
from aiwf.flows.cleaning_quality import apply_quality_gates_impl
from aiwf.flows.cleaning_simple_rules import clean_rows_simple as _clean_rows_simple
from aiwf.flows.cleaning_generic_rules import clean_rows_generic as _clean_rows_generic_external
from aiwf.paths import resolve_path


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
        "artifact_selection",
        "office_outputs_enabled",
        "enabled_office_artifacts",
        "disabled_office_artifacts",
        "enabled_core_artifacts",
        "disabled_core_artifacts",
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

    artifact_vr = validate_artifact_selection_config_with_tokens(
        params,
        allowed_core_tokens=list_cleaning_artifact_tokens(),
        allowed_office_tokens=list_office_artifact_tokens(),
    )
    errors.extend(artifact_vr.get("errors", []))
    warnings.extend(artifact_vr.get("warnings", []))

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def _local_parquet_strict_enabled(params: Dict[str, Any]) -> bool:
    return local_parquet_strict_enabled_impl(
        params,
        rules_dict=_rules_dict,
        to_bool=_to_bool,
        rule_param=_rule_param,
    )


def _require_local_parquet_dependencies(params: Dict[str, Any]) -> None:
    return require_local_parquet_dependencies_impl(
        params,
        local_parquet_strict_enabled=_local_parquet_strict_enabled,
    )


def _resolve_csv_source_path(params: Dict[str, Any], job_root: Optional[str]) -> Optional[str]:
    return resolve_csv_source_path_impl(
        params,
        job_root,
        resolve_path=lambda root, path, allow_absolute: resolve_path(root, path, allow_absolute=allow_absolute),
    )


def _parse_rows_from_csv_text(csv_text: str) -> List[Dict[str, Any]]:
    return parse_rows_from_csv_text_impl(csv_text)


def _read_text_file_with_fallback(path: str, encodings: Optional[List[str]] = None) -> str:
    return read_text_file_with_fallback_impl(path, encodings)


def _load_raw_rows(params: Dict[str, Any], job_root: Optional[str]) -> Tuple[List[Dict[str, Any]], str]:
    return load_raw_rows_impl(
        params,
        job_root,
        resolve_csv_source_path=_resolve_csv_source_path,
        parse_rows_from_csv_text=_parse_rows_from_csv_text,
        read_text_file_with_fallback=_read_text_file_with_fallback,
    )


def _maybe_preprocess_input(params: Dict[str, Any], job_root: str, stage_dir: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    from aiwf.preprocess import (
        preprocess_csv_file,
        run_preprocess_pipeline,
        validate_preprocess_pipeline,
        validate_preprocess_spec,
    )  # local import keeps loose coupling

    return maybe_preprocess_input_impl(
        params,
        job_root,
        stage_dir,
        to_bool=_to_bool,
        resolve_path=lambda root, path, allow_absolute: resolve_path(root, path, allow_absolute=allow_absolute),
        preprocess_csv_file=preprocess_csv_file,
        run_preprocess_pipeline=run_preprocess_pipeline,
        validate_preprocess_pipeline=validate_preprocess_pipeline,
        validate_preprocess_spec=validate_preprocess_spec,
    )


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
    return build_profile_impl(
        rows,
        quality,
        source,
        to_decimal=_to_decimal,
        quantize_decimal=_quantize_decimal,
    )


def _apply_quality_gates(quality: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return apply_quality_gates_impl(
        quality,
        params,
        to_int=_to_int,
        to_decimal=_to_decimal,
        rule_param=_rule_param,
    )


def _default_rows(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Backward-compatible wrapper used by older tests/callers.
    rows, _ = _load_raw_rows(params, None)
    return rows


def _write_cleaned_csv(csv_path: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    return write_cleaned_csv_impl(csv_path, rows)


def _write_cleaned_parquet(parquet_path: str, rows: List[Dict[str, Any]]) -> None:
    return write_cleaned_parquet_impl(parquet_path, rows)


def _write_fin_xlsx(
    xlsx_path: str,
    rows: List[Dict[str, Any]],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    return write_fin_xlsx_impl(
        xlsx_path,
        rows,
        image_path,
        params,
        office_write_fin_xlsx=_office_write_fin_xlsx,
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
    return write_audit_docx_impl(
        docx_path,
        job_id,
        profile,
        image_path,
        params,
        office_write_audit_docx=_office_write_audit_docx,
        utc_now_str=_utc_now_str,
    )


def _write_deck_pptx(
    pptx_path: str,
    job_id: str,
    profile: Dict[str, Any],
    image_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> None:
    return write_deck_pptx_impl(
        pptx_path,
        job_id,
        profile,
        image_path,
        params,
        office_write_deck_pptx=_office_write_deck_pptx,
        utc_now_str=_utc_now_str,
    )


def _write_profile_json(profile_path: str, profile: Dict[str, Any], params: Dict[str, Any]) -> None:
    return write_profile_json_impl(profile_path, profile, params)



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

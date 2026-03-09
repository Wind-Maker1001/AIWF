from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
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
from aiwf.accel_client import run_cleaning_operator, transform_rows_v2_operator
from aiwf.flows.office_artifacts import list_office_artifact_tokens
from aiwf.flows.cleaning_config import (
    is_generic_rules_enabled as _is_generic_rules_enabled_impl,
    normalize_key as _normalize_key_impl,
    quantize_decimal as _quantize_decimal_impl,
    rule_param as _rule_param_impl,
    rules_dict as _rules_dict_impl,
    to_bool as _to_bool_impl,
    to_decimal as _to_decimal_impl,
    to_float as _to_float_impl,
    to_int as _to_int_impl,
    validate_cleaning_rules_impl,
)
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
from aiwf.flows.cleaning_transport import (
    base_artifact_upsert_impl,
    base_step_done_impl,
    base_step_fail_impl,
    base_step_start_impl,
    headers_from_params_impl,
    post_json_impl,
)
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
    return headers_from_params_impl(params, env_api_key=os.getenv("AIWF_API_KEY"))


def _post_json(url: str, body: Dict[str, Any], headers: Dict[str, str]) -> None:
    return post_json_impl(url, body, headers)


def _try_accel_cleaning(
    params: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
) -> Dict[str, Any]:
    return run_cleaning_operator(
        params=params,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
    )


def _try_rust_transform_rows_v2(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    rules = _rules_dict(params)
    return transform_rows_v2_operator(
        raw_rows=raw_rows,
        params=params,
        rules=rules,
        quality_gates={
            "max_invalid_rows": _rule_param(params, "max_invalid_rows"),
            "min_output_rows": _rule_param(params, "min_output_rows"),
            "max_invalid_ratio": _rule_param(params, "max_invalid_ratio"),
            "required_fields": _rule_param(params, "required_fields"),
            "max_required_missing_ratio": _rule_param(params, "max_required_missing_ratio"),
        },
        schema_hint={"source": "glue-python.cleaning"},
    )


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
    return base_step_start_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        params=params,
        headers=headers,
        post_json=_post_json,
    )


def _base_step_done(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    output_hash: str,
    headers: Dict[str, str],
) -> None:
    return base_step_done_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        output_hash=output_hash,
        headers=headers,
        post_json=_post_json,
    )


def _base_step_fail(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    error: str,
    headers: Dict[str, str],
) -> None:
    return base_step_fail_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        error=error,
        headers=headers,
        post_json=_post_json,
    )


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
    return base_artifact_upsert_impl(
        base_url=base_url,
        job_id=job_id,
        actor=actor,
        artifact_id=artifact_id,
        kind=kind,
        path=path,
        sha256=sha256,
        extra_json=extra_json,
        headers=headers,
    )


def _normalize_key(k: str) -> str:
    return _normalize_key_impl(k)


def _to_int(v: Any) -> Optional[int]:
    return _to_int_impl(v)


def _to_float(v: Any) -> Optional[float]:
    return _to_float_impl(v)


def _to_decimal(v: Any) -> Optional[Decimal]:
    return _to_decimal_impl(v)


def _to_bool(v: Any, default: bool = False) -> bool:
    return _to_bool_impl(v, default=default)


def _quantize_decimal(v: Decimal, digits: int) -> Decimal:
    return _quantize_decimal_impl(v, digits)


def _rules_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    return _rules_dict_impl(params)


def _rule_param(params: Dict[str, Any], key: str, default: Any = None) -> Any:
    return _rule_param_impl(params, key, default)


def _is_generic_rules_enabled(params: Dict[str, Any]) -> bool:
    return _is_generic_rules_enabled_impl(params)


def validate_cleaning_rules(params: Dict[str, Any]) -> Dict[str, Any]:
    return validate_cleaning_rules_impl(
        params,
        validate_artifact_selection_config_with_tokens=validate_artifact_selection_config_with_tokens,
        list_cleaning_artifact_tokens=list_cleaning_artifact_tokens,
        list_office_artifact_tokens=list_office_artifact_tokens,
    )


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

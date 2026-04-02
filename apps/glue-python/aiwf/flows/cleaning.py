from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from aiwf.cleaning_spec_v2 import CLEANING_SPEC_V2_VERSION, compile_cleaning_params_to_spec
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
from aiwf.flows.cleaning_runtime_support import (
    base_artifact_upsert as _base_artifact_upsert_impl_runtime,
    base_step_done as _base_step_done_impl_runtime,
    base_step_fail as _base_step_fail_impl_runtime,
    base_step_start as _base_step_start_impl_runtime,
    ensure_dirs as _ensure_dirs_impl_runtime,
    headers_from_params as _headers_from_params_impl_runtime,
    is_valid_parquet_file as _is_valid_parquet_file_impl_runtime,
    post_json as _post_json_impl_runtime,
    sha256_file as _sha256_file_impl_runtime,
    try_accel_cleaning as _try_accel_cleaning_impl_runtime,
    try_rust_transform_rows_v3 as _try_rust_transform_rows_v3_impl_runtime,
    utc_now_str as _utc_now_str_impl_runtime,
)
from aiwf.flows.cleaning_simple_rules import clean_rows_simple as _clean_rows_simple
from aiwf.flows.cleaning_generic_rules import clean_rows_generic as _clean_rows_generic_external
from aiwf.paths import resolve_path


def _sha256_file(path: str) -> str:
    return _sha256_file_impl_runtime(path)


def _ensure_dirs(*paths: str) -> None:
    return _ensure_dirs_impl_runtime(*paths)


def _utc_now_str() -> str:
    return _utc_now_str_impl_runtime()


def _write_profile_illustration_png(path: str, profile: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> bool:
    return _office_write_profile_illustration_png(path, profile, params, utc_now_str=_utc_now_str)


def _is_valid_parquet_file(path: str) -> bool:
    return _is_valid_parquet_file_impl_runtime(path)


def _headers_from_params(params: Dict[str, Any]) -> Dict[str, str]:
    return _headers_from_params_impl_runtime(params, env_api_key=os.getenv("AIWF_API_KEY"))


def _post_json(url: str, body: Dict[str, Any], headers: Dict[str, str]) -> None:
    return _post_json_impl_runtime(url, body, headers)


def _try_accel_cleaning(
    params: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
) -> Dict[str, Any]:
    return _try_accel_cleaning_impl_runtime(
        params=params,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
    )


def _try_rust_transform_rows_v3(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    return _try_rust_transform_rows_v3_impl_runtime(
        raw_rows,
        params,
        rules_dict=_rules_dict,
        rule_param=_rule_param,
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
    return _base_step_start_impl_runtime(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        params=params,
        headers=headers,
        post_json_fn=_post_json,
    )


def _base_step_done(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    output_hash: str,
    headers: Dict[str, str],
) -> None:
    return _base_step_done_impl_runtime(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        output_hash=output_hash,
        headers=headers,
        post_json_fn=_post_json,
    )


def _base_step_fail(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    error: str,
    headers: Dict[str, str],
) -> None:
    return _base_step_fail_impl_runtime(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        error=error,
        headers=headers,
        post_json_fn=_post_json,
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
    return _base_artifact_upsert_impl_runtime(
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


def _cleaning_rust_v2_strategy(params: Dict[str, Any]) -> Dict[str, str]:
    env_mode = str(os.getenv("AIWF_CLEANING_RUST_V2_MODE", "off") or "off").strip().lower()
    if env_mode not in {"off", "shadow", "default"}:
        env_mode = "off"
    if bool(params.get("local_standalone")):
        env_mode = "default"
    rules = _rules_dict(params)
    if "use_rust_v2" in rules:
        enabled = _to_bool(rules.get("use_rust_v2"), default=False)
        decision = "force_rust" if enabled else "force_python"
        return {
            "decision": decision,
            "mode": "explicit",
            "requested_mode": env_mode,
            "effective_mode": decision,
        }
    if "use_rust_v2" in params:
        enabled = _to_bool(params.get("use_rust_v2"), default=False)
        decision = "force_rust" if enabled else "force_python"
        return {
            "decision": decision,
            "mode": "explicit",
            "requested_mode": env_mode,
            "effective_mode": decision,
        }
    return {
        "decision": env_mode,
        "mode": env_mode,
        "requested_mode": env_mode,
        "effective_mode": env_mode,
    }


def _shadow_compare_result(
    *,
    status: str,
    matched: bool,
    mismatches: List[str],
    skipped_reason: str = "",
) -> Dict[str, Any]:
    return {
        "status": status,
        "matched": matched,
        "mismatch_count": len(mismatches),
        "mismatches": list(mismatches),
        "skipped_reason": skipped_reason,
        "compare_fields": ["rows", "quality", "reason_counts"],
    }


def _stable_rows_for_compare(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        [dict(item or {}) for item in rows],
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )


def _quality_compare_view(quality: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in [
        "input_rows",
        "output_rows",
        "invalid_rows",
        "filtered_rows",
        "duplicate_rows_removed",
        "required_missing_ratio",
    ]:
        if key in quality:
            out[key] = quality.get(key)
    return out


def _python_reason_counts(quality: Dict[str, Any]) -> Dict[str, int]:
    rule_hits = quality.get("rule_hits") if isinstance(quality.get("rule_hits"), dict) else {}
    return {
        "invalid_object": 0,
        "cast_failed": int(rule_hits.get("cast_failed", 0)),
        "required_missing": int(rule_hits.get("required_failed", 0)) + int(rule_hits.get("invalid_id", 0)) + int(rule_hits.get("invalid_amount", 0)),
        "filter_rejected": int(rule_hits.get("filtered_rules", 0)) + int(rule_hits.get("filtered_negative", 0)) + int(rule_hits.get("filtered_min_amount", 0)) + int(rule_hits.get("filtered_max_amount", 0)),
        "duplicate_removed": int(rule_hits.get("deduplicate_removed", quality.get("duplicate_rows_removed", 0) or 0)),
    }


def _reason_counts_from_audit(execution_audit: Dict[str, Any], fallback_quality: Dict[str, Any]) -> Dict[str, int]:
    counts = execution_audit.get("reason_counts") if isinstance(execution_audit.get("reason_counts"), dict) else None
    if isinstance(counts, dict):
        return {str(key): int(value or 0) for key, value in counts.items()}
    return _python_reason_counts(fallback_quality)


def _reason_samples_structurally_valid(execution_audit: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    reason_counts = _reason_counts_from_audit(execution_audit, {})
    samples = execution_audit.get("reason_samples") if isinstance(execution_audit.get("reason_samples"), dict) else {}
    sample_limit = (
        execution_audit.get("limits", {}).get("sample_limit")
        if isinstance(execution_audit.get("limits"), dict)
        else None
    )
    for key in sorted(reason_counts.keys()):
        if key not in samples:
            errors.append(f"reason_samples missing key {key}")
            continue
        items = samples.get(key)
        if not isinstance(items, list):
            errors.append(f"reason_samples[{key}] must be array")
            continue
        if sample_limit is not None and len(items) > int(sample_limit):
            errors.append(f"reason_samples[{key}] exceeds sample_limit")
        if reason_counts.get(key, 0) < len(items):
            errors.append(f"reason_samples[{key}] larger than reason_counts[{key}]")
    return len(errors) == 0, errors


def _compare_python_and_rust_cleaning(
    *,
    python_rows: List[Dict[str, Any]],
    python_quality: Dict[str, Any],
    python_execution_audit: Dict[str, Any],
    rust_rows: List[Dict[str, Any]],
    rust_quality: Dict[str, Any],
    rust_execution_audit: Dict[str, Any],
) -> Dict[str, Any]:
    mismatches: List[str] = []
    if _stable_rows_for_compare(python_rows) != _stable_rows_for_compare(rust_rows):
        mismatches.append("rows mismatch")
    python_quality_view = _quality_compare_view(python_quality)
    rust_quality_view = _quality_compare_view(rust_quality)
    if python_quality_view != rust_quality_view:
        mismatches.append(f"quality mismatch: python={python_quality_view} rust={rust_quality_view}")
    python_reason_counts = _reason_counts_from_audit(python_execution_audit, python_quality)
    rust_reason_counts = _reason_counts_from_audit(rust_execution_audit, rust_quality)
    if python_reason_counts != rust_reason_counts:
        mismatches.append(f"reason_counts mismatch: python={python_reason_counts} rust={rust_reason_counts}")
    valid_samples, sample_errors = _reason_samples_structurally_valid(rust_execution_audit)
    if not valid_samples:
        mismatches.extend(sample_errors)
    if mismatches:
        return _shadow_compare_result(status="mismatched", matched=False, mismatches=mismatches)
    return _shadow_compare_result(status="matched", matched=True, mismatches=[])


def _clean_rows(raw_rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Any]:
    compiled_spec = compile_cleaning_params_to_spec(params)
    strategy = _cleaning_rust_v2_strategy(params)
    verify_on_default = bool(params.get("local_standalone")) or _to_bool(os.getenv("AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT", "false"), default=False)
    quality_rule_set_provenance = params.get("_quality_rule_set_provenance") if isinstance(params.get("_quality_rule_set_provenance"), dict) else {}

    def apply_execution_metadata(out: Dict[str, Any]) -> Dict[str, Any]:
        out["requested_rust_v2_mode"] = str(strategy.get("requested_mode") or "")
        out["effective_rust_v2_mode"] = str(strategy.get("effective_mode") or "")
        out["verify_on_default"] = bool(verify_on_default)
        return out

    def build_python_result(
        *,
        rust_error: str = "",
        shadow_compare: Optional[Dict[str, Any]] = None,
        skipped_reason: str = "mode_off",
    ) -> Dict[str, Any]:
        out = _clean_rows_simple(
            raw_rows,
            params,
            hooks={
                "is_generic_rules_enabled": _is_generic_rules_enabled,
                "clean_rows_generic": _clean_rows_generic,
                "to_int": _to_int,
                "to_bool": _to_bool,
                "rule_param": _rule_param,
                "rules_dict": _rules_dict,
                "to_decimal": _to_decimal,
                "normalize_key": _normalize_key,
                "quantize_decimal": _quantize_decimal,
            },
        )
        q = dict(out.get("quality") or {})
        q["cleaning_spec_version"] = CLEANING_SPEC_V2_VERSION
        q["rust_v2_used"] = False
        if quality_rule_set_provenance:
            q["quality_rule_set_id"] = str(quality_rule_set_provenance.get("resolved_id") or params.get("quality_rule_set_id") or "")
        if rust_error:
            q["rust_v2_error"] = rust_error
        out["quality"] = q
        reason_samples = out.get("reason_samples") if isinstance(out.get("reason_samples"), dict) else {}
        out["cleaning_spec"] = compiled_spec
        out["execution_mode"] = "python_legacy"
        out["execution_audit"] = {
            "schema": "python_cleaning.audit.v1",
            "operator": "python_clean_rows",
            "rule_hits": dict(q.get("rule_hits") or {}),
            "reason_counts": _python_reason_counts(q),
            "reason_samples": reason_samples or {
                "invalid_object": [],
                "cast_failed": [],
                "required_missing": [],
                "filter_rejected": [],
                "duplicate_removed": [],
            },
            "rust_v2_error": rust_error,
        }
        out["row_transform_engine"] = "python"
        out["postprocess_engine"] = "none"
        out["quality_gate_engine"] = "python"
        out["materialization_engine"] = "python"
        out["legacy_cleaning_operator_used"] = False
        out["stage_provenance"] = [
            {"stage": "row_transform", "engine": "python"},
            {"stage": "quality_gate", "engine": "python"},
            {"stage": "materialize", "engine": "python"},
        ]
        if quality_rule_set_provenance:
            out["execution_audit"]["quality_rule_set_provenance"] = dict(quality_rule_set_provenance)
        if strategy["decision"] == "force_python":
            out["eligibility_reason"] = "forced_python"
        elif rust_error:
            out["eligibility_reason"] = "rust_v2_error"
        elif strategy["decision"] == "off":
            out["eligibility_reason"] = "mode_off"
        else:
            out["eligibility_reason"] = "eligible"
        out["shadow_compare"] = shadow_compare or _shadow_compare_result(
            status="skipped",
            matched=False,
            mismatches=[],
            skipped_reason=skipped_reason,
        )
        return apply_execution_metadata(out)

    def build_rust_result(
        rust_v2: Dict[str, Any],
        *,
        shadow_compare: Optional[Dict[str, Any]] = None,
        skipped_reason: str = "default_without_verify",
    ) -> Dict[str, Any]:
        quality = dict(rust_v2["quality"])
        quality["cleaning_spec_version"] = CLEANING_SPEC_V2_VERSION
        if quality_rule_set_provenance:
            quality["quality_rule_set_id"] = str(quality_rule_set_provenance.get("resolved_id") or params.get("quality_rule_set_id") or "")
        execution_audit = (
            rust_v2.get("audit")
            if isinstance(rust_v2.get("audit"), dict)
            else dict(quality.get("rust_v2_audit") or {})
        )
        execution_audit["operator"] = str((rust_v2.get("response") or {}).get("operator") or "transform_rows_v3")
        if quality_rule_set_provenance:
            execution_audit["quality_rule_set_provenance"] = dict(quality_rule_set_provenance)
        return apply_execution_metadata({
            "rows": rust_v2["rows"],
            "quality": quality,
            "cleaning_spec": compiled_spec,
            "execution_mode": "rust_v2",
            "execution_audit": execution_audit,
            "eligibility_reason": "eligible",
            "row_transform_engine": str((rust_v2.get("response") or {}).get("operator") or "transform_rows_v3"),
            "postprocess_engine": "none",
            "quality_gate_engine": str((rust_v2.get("response") or {}).get("operator") or "transform_rows_v3"),
            "materialization_engine": "python",
            "legacy_cleaning_operator_used": False,
            "stage_provenance": [
                {"stage": "row_transform", "engine": str((rust_v2.get("response") or {}).get("operator") or "transform_rows_v3")},
                {"stage": "quality_gate", "engine": str((rust_v2.get("response") or {}).get("operator") or "transform_rows_v3")},
                {"stage": "materialize", "engine": "python"},
            ],
            "shadow_compare": shadow_compare or _shadow_compare_result(
                status="skipped",
                matched=False,
                mismatches=[],
                skipped_reason=skipped_reason,
            ),
        })

    if strategy["decision"] == "force_python":
        return build_python_result(skipped_reason="forced_python")

    if strategy["decision"] == "off":
        return build_python_result(skipped_reason="mode_off")

    rust_v2 = _try_rust_transform_rows_v3(raw_rows, params)

    if strategy["decision"] == "force_rust":
        if rust_v2.get("ok"):
            return build_rust_result(rust_v2, skipped_reason="explicit_force_rust")
        return build_python_result(
            rust_error=str(rust_v2.get("error") or ""),
            shadow_compare=_shadow_compare_result(
                status="rust_error",
                matched=False,
                mismatches=[str(rust_v2.get("error") or "rust_v2_error")],
            ),
        )

    if strategy["decision"] == "shadow":
        python_result = build_python_result(skipped_reason="shadow_not_attempted")
        if rust_v2.get("ok"):
            python_result["shadow_compare"] = _compare_python_and_rust_cleaning(
                python_rows=python_result["rows"],
                python_quality=python_result["quality"],
                python_execution_audit=python_result["execution_audit"],
                rust_rows=rust_v2["rows"],
                rust_quality=dict(rust_v2.get("quality") or {}),
                rust_execution_audit=rust_v2.get("audit") if isinstance(rust_v2.get("audit"), dict) else {},
            )
            return python_result
        python_result["shadow_compare"] = _shadow_compare_result(
            status="rust_error",
            matched=False,
            mismatches=[str(rust_v2.get("error") or "rust_v2_error")],
        )
        python_result["eligibility_reason"] = "rust_v2_error"
        return python_result

    if strategy["decision"] == "default":
        if rust_v2.get("ok"):
            if verify_on_default:
                python_result = build_python_result(skipped_reason="default_verify_baseline")
                shadow_compare = _compare_python_and_rust_cleaning(
                    python_rows=python_result["rows"],
                    python_quality=python_result["quality"],
                    python_execution_audit=python_result["execution_audit"],
                    rust_rows=rust_v2["rows"],
                    rust_quality=dict(rust_v2.get("quality") or {}),
                    rust_execution_audit=rust_v2.get("audit") if isinstance(rust_v2.get("audit"), dict) else {},
                )
                if shadow_compare.get("status") == "matched":
                    return build_rust_result(rust_v2, shadow_compare=shadow_compare, skipped_reason="")
                python_result["shadow_compare"] = shadow_compare
                python_result["eligibility_reason"] = "shadow_compare_mismatch"
                return python_result
            return build_rust_result(rust_v2, skipped_reason="default_without_verify")
        return build_python_result(
            rust_error=str(rust_v2.get("error") or ""),
            shadow_compare=_shadow_compare_result(
                status="rust_error",
                matched=False,
                mismatches=[str(rust_v2.get("error") or "rust_v2_error")],
            ),
        )

    return build_python_result(skipped_reason="mode_off")


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

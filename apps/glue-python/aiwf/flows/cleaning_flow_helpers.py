from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional

from aiwf.flow_context import normalize_job_context
from aiwf.flows.office_artifacts import collect_accel_office_artifact_issues
from aiwf.flows.cleaning_flow_materialization import (
    materialize_accel_outputs,
    materialize_local_outputs,
    materialize_office_outputs,
)
from aiwf.paths import resolve_bus_root, resolve_job_root


def resolve_base_url(s: Optional[Any], base: Optional[Any]) -> str:
    if s is not None and getattr(s, "base_url", None):
        return str(getattr(s, "base_url"))
    if base is not None and getattr(base, "base_url", None):
        return str(getattr(base, "base_url"))
    return "http://127.0.0.1:18080"


def prepare_job_layout(
    job_id: str,
    params: Dict[str, Any],
    *,
    ensure_dirs: Callable[..., Any],
) -> Dict[str, str]:
    bus_root = resolve_bus_root()
    job_context = normalize_job_context(
        job_id,
        params=params,
        job_context=params.get("job_context") if isinstance(params.get("job_context"), dict) else None,
    )
    job_root = job_context["job_root"]
    stage_dir = job_context["stage_dir"]
    artifacts_dir = job_context["artifacts_dir"]
    evidence_dir = job_context["evidence_dir"]
    ensure_dirs(stage_dir, artifacts_dir, evidence_dir)
    job_root_uri = os.path.join(job_root, "")
    input_uri = params.get("input_uri") or job_root_uri
    output_uri = params.get("output_uri") or job_root_uri
    return {
        "bus_root": bus_root,
        "job_root": job_root,
        "stage_dir": stage_dir,
        "artifacts_dir": artifacts_dir,
        "evidence_dir": evidence_dir,
        "input_uri": input_uri,
        "output_uri": output_uri,
    }


def prepare_local_clean_cache(
    params_effective: Dict[str, Any],
    job_root: str,
    *,
    load_raw_rows: Callable[..., Any],
    clean_rows: Callable[..., Any],
    rules_dict: Callable[..., Any],
) -> Dict[str, Any]:
    raw_rows, source = load_raw_rows(params_effective, job_root)
    cleaned_local = clean_rows(raw_rows, params_effective)
    local_rows = cleaned_local["rows"]
    local_quality = cleaned_local["quality"]
    params_for_accel = dict(params_effective)
    params_for_accel["rows"] = local_rows
    params_for_accel["rules"] = rules_dict(params_effective)
    return {
        "raw_rows": raw_rows,
        "source": source,
        "local_rows": local_rows,
        "local_quality": local_quality,
        "local_execution": {
            "execution_mode": str(cleaned_local.get("execution_mode") or ""),
            "execution_audit": dict(cleaned_local.get("execution_audit") or {}),
            "eligibility_reason": str(cleaned_local.get("eligibility_reason") or ""),
            "execution_plan": "rust_row_transform" if str(cleaned_local.get("row_transform_engine") or "").startswith("transform_rows_v3") else "python_row_transform",
            "shadow_compare": dict(cleaned_local.get("shadow_compare") or {}),
            "requested_rust_v2_mode": str(cleaned_local.get("requested_rust_v2_mode") or ""),
            "effective_rust_v2_mode": str(cleaned_local.get("effective_rust_v2_mode") or ""),
            "verify_on_default": bool(cleaned_local.get("verify_on_default", False)),
            "row_transform_engine": str(cleaned_local.get("row_transform_engine") or ""),
            "postprocess_engine": str(cleaned_local.get("postprocess_engine") or "none"),
            "quality_gate_engine": str(cleaned_local.get("quality_gate_engine") or ""),
            "materialization_engine": str(cleaned_local.get("materialization_engine") or "python"),
            "legacy_cleaning_operator_used": bool(cleaned_local.get("legacy_cleaning_operator_used", False)),
            "stage_provenance": list(cleaned_local.get("stage_provenance") or []),
        },
        "params_for_accel": params_for_accel,
    }


def prepare_accel_result(
    *,
    params_effective: Dict[str, Any],
    params_for_accel: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: str,
    output_uri: str,
    to_bool: Callable[..., bool],
    rule_param: Callable[..., Any],
    is_generic_rules_enabled: Callable[..., bool],
    try_accel_cleaning: Callable[..., Any],
    is_valid_parquet_file: Callable[..., bool],
) -> Dict[str, Any]:
    if to_bool(rule_param(params_effective, "force_local_cleaning", False), default=False) or is_generic_rules_enabled(params_effective):
        accel = {
            "attempted": False,
            "ok": False,
            "error": "legacy accel cleaning skipped for generic/local-only cleaning mode",
        }
    else:
        accel = try_accel_cleaning(
            params=params_for_accel,
            job_id=job_id,
            step_id=step_id,
            actor=actor,
            ruleset_version=ruleset_version,
            input_uri=input_uri,
            output_uri=output_uri,
        )

    accel_resp = accel.get("response") if isinstance(accel, dict) else {}
    accel_outputs = accel_resp.get("outputs") if isinstance(accel_resp, dict) else {}
    accel_profile = accel_resp.get("profile") if isinstance(accel_resp, dict) else {}

    accel_parquet_path = ""
    if isinstance(accel_outputs, dict):
        parquet_obj = accel_outputs.get("cleaned_parquet") or {}
        if isinstance(parquet_obj, dict):
            accel_parquet_path = str(parquet_obj.get("path", ""))
    accel_parquet_valid = is_valid_parquet_file(accel_parquet_path)

    accel_validation_error = None
    if accel.get("ok") and isinstance(accel_outputs, dict) and isinstance(accel_outputs.get("cleaned_parquet"), dict):
        if not accel_parquet_valid:
            accel_validation_error = f"invalid parquet from accel output: {accel_parquet_path or '<empty path>'}"
    office_output_issues: List[str] = []
    if accel.get("ok") and isinstance(accel_outputs, dict):
        office_output_issues = collect_accel_office_artifact_issues(
            accel_outputs,
            params_effective=params_effective,
        )
        if office_output_issues:
            office_error = "; ".join(office_output_issues)
            if accel_validation_error:
                accel_validation_error = f"{accel_validation_error}; {office_error}"
            else:
                accel_validation_error = office_error

    use_accel_outputs = (
        accel.get("ok")
        and isinstance(accel_outputs, dict)
        and isinstance(accel_outputs.get("cleaned_parquet"), dict)
        and accel_parquet_valid
        and not office_output_issues
    )

    return {
        "accel": accel,
        "accel_resp": accel_resp,
        "accel_outputs": accel_outputs,
        "accel_profile": accel_profile,
        "accel_validation_error": accel_validation_error,
        "use_accel_outputs": use_accel_outputs,
    }

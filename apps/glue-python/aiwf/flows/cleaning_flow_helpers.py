from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional

from aiwf.flow_context import normalize_job_context
from aiwf.flows.cleaning_artifacts import (
    CleaningArtifactContext,
    materialize_accel_cleaning_artifacts,
    materialize_local_cleaning_artifacts,
)
from aiwf.flows.office_artifacts import (
    OfficeArtifactContext,
    select_office_artifact_registrations,
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
            "error": "accel skipped for generic/local-only cleaning mode",
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

    use_accel_outputs = (
        accel.get("ok")
        and isinstance(accel_outputs, dict)
        and isinstance(accel_outputs.get("cleaned_parquet"), dict)
        and accel_parquet_valid
    )

    return {
        "accel": accel,
        "accel_resp": accel_resp,
        "accel_outputs": accel_outputs,
        "accel_profile": accel_profile,
        "accel_validation_error": accel_validation_error,
        "use_accel_outputs": use_accel_outputs,
    }


def materialize_office_outputs(
    *,
    job_id: str,
    artifacts_dir: str,
    params_effective: Dict[str, Any],
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    profile_source: str,
    office_rows_subset: Callable[..., Any],
    build_profile: Callable[..., Any],
    write_profile_illustration_png: Callable[..., Any],
    write_fin_xlsx: Callable[..., Any],
    write_audit_docx: Callable[..., Any],
    write_deck_pptx: Callable[..., Any],
    sha256_file: Callable[..., str],
) -> Dict[str, Any]:
    registrations = select_office_artifact_registrations(params_effective)
    if not registrations:
        return {
            "office_profile": None,
            "office_artifacts": [],
        }

    illustration_path = os.path.join(artifacts_dir, "summary_visual.png")

    office_rows, office_truncated = office_rows_subset(rows, params_effective)
    office_profile = build_profile(office_rows, quality, profile_source)
    office_profile["office_rows_truncated"] = office_truncated
    office_profile["office_rows_used"] = len(office_rows)

    write_profile_illustration_png(illustration_path, office_profile, params_effective)
    context = OfficeArtifactContext(
        job_id=job_id,
        artifacts_dir=artifacts_dir,
        params_effective=params_effective,
        rows=rows,
        quality=quality,
        profile_source=profile_source,
        office_rows=office_rows,
        office_profile=office_profile,
        illustration_path=illustration_path,
        write_fin_xlsx=write_fin_xlsx,
        write_audit_docx=write_audit_docx,
        write_deck_pptx=write_deck_pptx,
        sha256_file=sha256_file,
    )

    out: Dict[str, Any] = {
        "office_profile": office_profile,
        "office_artifacts": [],
    }
    for registration in registrations:
        output_path = os.path.join(artifacts_dir, registration.filename)
        registration.writer(context, output_path)
        sha = sha256_file(output_path)
        out[registration.path_key] = output_path
        out[registration.sha_key] = sha
        out["office_artifacts"].append(
            {
                "artifact_id": registration.artifact_id,
                "kind": registration.kind,
                "path": output_path,
                "sha256": sha,
            }
        )

    return out


def materialize_accel_outputs(
    *,
    job_id: str,
    artifacts_dir: str,
    params_effective: Dict[str, Any],
    local_rows: List[Dict[str, Any]],
    local_quality: Dict[str, Any],
    accel_outputs: Dict[str, Any],
    accel_profile: Dict[str, Any],
    sha256_file: Callable[..., str],
    materialize_office_outputs_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    out = {
        "profile": {
            "rows": int((accel_profile or {}).get("rows", 0)),
            "cols": int((accel_profile or {}).get("cols", 2)),
            "source": "accel",
        },
    }
    out.update(
        materialize_accel_cleaning_artifacts(
            accel_outputs,
            params_effective=params_effective,
            sha256_file=sha256_file,
        )
    )
    out.update(
        materialize_office_outputs_fn(
            job_id=job_id,
            artifacts_dir=artifacts_dir,
            params_effective=params_effective,
            rows=local_rows,
            quality=local_quality,
            profile_source="accel+local_office",
        )
    )
    return out


def materialize_local_outputs(
    *,
    job_id: str,
    stage_dir: str,
    artifacts_dir: str,
    evidence_dir: str,
    params_effective: Dict[str, Any],
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    source: str,
    preprocess_result: Optional[Dict[str, Any]],
    apply_quality_gates: Callable[..., Any],
    to_bool: Callable[..., bool],
    rule_param: Callable[..., Any],
    require_local_parquet_dependencies: Callable[..., Any],
    write_cleaned_csv: Callable[..., Any],
    write_cleaned_parquet: Callable[..., Any],
    is_valid_parquet_file: Callable[..., bool],
    local_parquet_strict_enabled: Callable[..., bool],
    build_profile: Callable[..., Any],
    write_profile_json: Callable[..., Any],
    sha256_file: Callable[..., str],
    materialize_office_outputs_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    quality_gate = apply_quality_gates(quality, params_effective)
    if not rows and not to_bool(rule_param(params_effective, "allow_empty_output", True), default=True):
        raise RuntimeError("cleaning produced empty result")

    require_local_parquet_dependencies(params_effective)

    profile = build_profile(rows, quality, source)
    profile["quality_gate"] = quality_gate
    profile["preprocess"] = preprocess_result
    out = {"profile": profile}
    out.update(
        materialize_local_cleaning_artifacts(
            CleaningArtifactContext(
                stage_dir=stage_dir,
                evidence_dir=evidence_dir,
                rows=rows,
                profile=profile,
                params_effective=params_effective,
                write_cleaned_csv=write_cleaned_csv,
                write_cleaned_parquet=write_cleaned_parquet,
                write_profile_json=write_profile_json,
                sha256_file=sha256_file,
            )
        )
    )
    parquet_valid_local = is_valid_parquet_file(str(out.get("cleaned_parquet") or ""))
    if not parquet_valid_local and local_parquet_strict_enabled(params_effective):
        raise RuntimeError(
            "local parquet generation invalid; strict mode enabled (set local_parquet_strict=false to bypass)"
        )
    out.update(
        materialize_office_outputs_fn(
            job_id=job_id,
            artifacts_dir=artifacts_dir,
            params_effective=params_effective,
            rows=rows,
            quality=quality,
            profile_source=source,
        )
    )
    return out

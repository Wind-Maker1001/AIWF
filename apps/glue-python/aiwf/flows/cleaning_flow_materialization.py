from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional

from aiwf.flows.cleaning_artifacts import (
    CleaningArtifactContext,
    materialize_accel_cleaning_artifacts,
    materialize_local_cleaning_artifacts,
)
from aiwf.flows.office_artifacts import (
    OfficeArtifactContext,
    materialize_accel_office_artifacts,
    select_office_artifact_registrations,
)
from aiwf.flows.cleaning_errors import CleaningGuardrailError, guardrail_template_expected_profile, guardrail_template_id
from aiwf.flows.cleaning_advanced_quality import evaluate_advanced_quality
from aiwf.flows.cleaning_reporting import build_quality_summary, flatten_rejection_records


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
    params_effective: Dict[str, Any],
    accel_outputs: Dict[str, Any],
    accel_profile: Dict[str, Any],
    sha256_file: Callable[..., str],
    local_rows: Optional[List[Dict[str, Any]]] = None,
    local_profile: Optional[Dict[str, Any]] = None,
    local_execution: Optional[Dict[str, Any]] = None,
    preprocess_result: Optional[Dict[str, Any]] = None,
    input_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    transform_execution = (
        dict(local_execution)
        if isinstance(local_execution, dict)
        else {}
    )
    execution = {
        "execution_mode": "accel_operator",
        "execution_audit": {},
        "eligibility_reason": "accel_outputs",
        "execution_plan": "legacy_cleaning_operator",
        "requested_rust_v2_mode": "",
        "effective_rust_v2_mode": "",
        "verify_on_default": False,
        "row_transform_engine": str(transform_execution.get("row_transform_engine") or "unknown"),
        "postprocess_engine": str(transform_execution.get("postprocess_engine") or "none"),
        "quality_gate_engine": str(transform_execution.get("quality_gate_engine") or "unknown"),
        "materialization_engine": "legacy_accel_cleaning",
        "legacy_cleaning_operator_used": True,
        "stage_provenance": list(transform_execution.get("stage_provenance") or []),
        "shadow_compare": {
            "status": "skipped",
            "matched": False,
            "mismatch_count": 0,
            "mismatches": [],
            "skipped_reason": "accel_outputs",
            "compare_fields": ["rows", "quality", "reason_counts"],
        },
    }
    profile = (
        dict(local_profile)
        if isinstance(local_profile, dict)
        else {
            "rows": int((accel_profile or {}).get("rows", 0)),
            "cols": int((accel_profile or {}).get("cols", 2)),
            "source": "accel",
        }
    )
    transform_quality = profile.get("quality") if isinstance(profile.get("quality"), dict) else {}
    quality_gate = profile.get("quality_gate") if isinstance(profile.get("quality_gate"), dict) else {}
    advanced_quality = evaluate_advanced_quality(
        rows=list(local_rows or []),
        params_effective=params_effective,
        semantic_rows=list(input_rows or local_rows or []),
    )
    quality_gate["advanced_quality"] = advanced_quality
    if advanced_quality.get("blocked"):
        report = dict(advanced_quality.get("report") or {})
        violations = report.get("violations") if isinstance(report.get("violations"), list) else []
        message = "; ".join(str(item) for item in violations if str(item).strip()) or "advanced quality blocked"
        raise CleaningGuardrailError(
            error_code="advanced_quality_blocked",
            message=message,
            reason_codes=["advanced_quality_blocked"],
            template_id=guardrail_template_id(params_effective),
            template_expected_profile=guardrail_template_expected_profile(params_effective),
            blank_output_expected=bool(params_effective.get("blank_output_expected", False)),
            zero_output_unexpected=False,
            blocking_reason_codes=["advanced_quality_blocked"],
            details={"advanced_quality": advanced_quality},
        )
    summary_execution = {
        **transform_execution,
        **execution,
        "execution_audit": dict(transform_execution.get("execution_audit") or {}),
        "shadow_compare": dict(transform_execution.get("shadow_compare") or execution.get("shadow_compare") or {}),
        "stage_provenance": list(transform_execution.get("stage_provenance") or execution.get("stage_provenance") or []),
        "advanced_quality": advanced_quality,
        "row_samples": {
            "before": list(input_rows or [])[:5],
            "after": list(local_rows or [])[:5],
        },
    }
    quality_summary = build_quality_summary(
        params_effective=params_effective,
        transform_quality=transform_quality,
        quality_gate=quality_gate,
        execution_report=summary_execution,
        preprocess_result=preprocess_result,
    )
    rejections = flatten_rejection_records(
        (transform_execution or execution).get("execution_audit", {}).get("reason_samples")
        if isinstance((transform_execution or execution).get("execution_audit"), dict)
        else {}
    )
    profile["quality_summary"] = quality_summary
    if preprocess_result is not None:
        profile["preprocess"] = preprocess_result
    if transform_execution:
        profile["execution"] = summary_execution
    else:
        profile["execution"] = summary_execution
    out = {
        "profile": profile,
        "execution": execution,
        "quality_summary": quality_summary,
        "rejections": rejections,
    }
    local_context = None
    job_context = params_effective.get("job_context") if isinstance(params_effective.get("job_context"), dict) else {}
    if job_context:
        local_context = CleaningArtifactContext(
            stage_dir=str(job_context.get("stage_dir") or ""),
            evidence_dir=str(job_context.get("evidence_dir") or ""),
            rows=list(local_rows or []),
            profile=profile,
            quality_summary=quality_summary,
            rejections=rejections,
            params_effective=params_effective,
            write_cleaned_csv=lambda *_args, **_kwargs: None,
            write_cleaned_parquet=lambda *_args, **_kwargs: None,
            write_profile_json=lambda *_args, **_kwargs: None,
            sha256_file=sha256_file,
        )
    out.update(
        materialize_accel_cleaning_artifacts(
            accel_outputs,
            params_effective=params_effective,
            sha256_file=sha256_file,
            local_context=local_context,
        )
    )
    out.update(
        materialize_accel_office_artifacts(
            accel_outputs,
            params_effective=params_effective,
            sha256_file=sha256_file,
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
    input_rows: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    execution_report: Dict[str, Any],
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
    advanced_quality = evaluate_advanced_quality(
        rows=rows,
        params_effective=params_effective,
        semantic_rows=list(input_rows or rows or []),
    )
    quality_gate["advanced_quality"] = advanced_quality
    allow_empty_output_default = params_effective.get("blank_output_expected", True)
    if not rows and not to_bool(rule_param(params_effective, "allow_empty_output", allow_empty_output_default), default=True):
        execution_profile_analysis = (
            dict(execution_report.get("profile_analysis") or {})
            if isinstance(execution_report, dict)
            else {}
        )
        raise CleaningGuardrailError(
            error_code="zero_output_unexpected",
            message="cleaning blocked: output_rows=0 while blank output is not expected",
            reason_codes=["zero_output_unexpected"],
            requested_profile=str(execution_profile_analysis.get("requested_profile") or ""),
            recommended_profile=str(execution_profile_analysis.get("recommended_profile") or ""),
            profile_confidence=float(execution_profile_analysis.get("profile_confidence") or 0.0),
            required_field_coverage=float(execution_profile_analysis.get("required_field_coverage") or 0.0),
            template_id=guardrail_template_id(params_effective),
            template_expected_profile=guardrail_template_expected_profile(params_effective),
            blank_output_expected=bool(params_effective.get("blank_output_expected", False)),
            zero_output_unexpected=True,
            blocking_reason_codes=list(execution_profile_analysis.get("blocking_reason_codes") or []) + ["zero_output_unexpected"],
            details={"quality": dict(quality or {})},
        )

    require_local_parquet_dependencies(params_effective)
    execution_effective = dict(execution_report or {})
    row_transform_engine = str(execution_effective.get("row_transform_engine") or "")
    if row_transform_engine.startswith("transform_rows_v3"):
        execution_effective["quality_gate_engine"] = "transform_rows_v3+python_verify"
    elif not str(execution_effective.get("quality_gate_engine") or "").strip():
        execution_effective["quality_gate_engine"] = "python"
    execution_effective["materialization_engine"] = "python"
    execution_effective["legacy_cleaning_operator_used"] = False
    execution_effective["advanced_quality"] = advanced_quality
    execution_effective["row_samples"] = {
        "before": list(input_rows or [])[:5],
        "after": list(rows or [])[:5],
    }

    if advanced_quality.get("blocked"):
        report = dict(advanced_quality.get("report") or {})
        violations = report.get("violations") if isinstance(report.get("violations"), list) else []
        message = "; ".join(str(item) for item in violations if str(item).strip()) or "advanced quality blocked"
        raise CleaningGuardrailError(
            error_code="advanced_quality_blocked",
            message=message,
            reason_codes=["advanced_quality_blocked"],
            template_id=guardrail_template_id(params_effective),
            template_expected_profile=guardrail_template_expected_profile(params_effective),
            blank_output_expected=bool(params_effective.get("blank_output_expected", False)),
            zero_output_unexpected=False,
            blocking_reason_codes=["advanced_quality_blocked"],
            details={"advanced_quality": advanced_quality},
        )

    profile = build_profile(rows, quality, source)
    profile["quality_gate"] = quality_gate
    profile["preprocess"] = preprocess_result
    profile["execution"] = execution_effective
    quality_summary = build_quality_summary(
        params_effective=params_effective,
        transform_quality=quality,
        quality_gate=quality_gate,
        execution_report=execution_effective,
        preprocess_result=preprocess_result,
    )
    rejections = flatten_rejection_records(
        execution_report.get("execution_audit", {}).get("reason_samples")
        if isinstance(execution_report.get("execution_audit"), dict)
        else {}
    )
    profile["quality_summary"] = quality_summary
    out = {
        "profile": profile,
        "execution": execution_effective,
        "quality_summary": quality_summary,
        "rejections": rejections,
    }
    out.update(
        materialize_local_cleaning_artifacts(
            CleaningArtifactContext(
                stage_dir=stage_dir,
                evidence_dir=evidence_dir,
                rows=rows,
                profile=profile,
                quality_summary=quality_summary,
                rejections=rejections,
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

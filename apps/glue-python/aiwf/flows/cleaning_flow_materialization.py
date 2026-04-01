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
) -> Dict[str, Any]:
    execution = {
        "execution_mode": "accel_operator",
        "execution_audit": {},
        "eligibility_reason": "accel_outputs",
        "requested_rust_v2_mode": "",
        "effective_rust_v2_mode": "",
        "verify_on_default": False,
        "shadow_compare": {
            "status": "skipped",
            "matched": False,
            "mismatch_count": 0,
            "mismatches": [],
            "skipped_reason": "accel_outputs",
            "compare_fields": ["rows", "quality", "reason_counts"],
        },
    }
    out = {
        "profile": {
            "rows": int((accel_profile or {}).get("rows", 0)),
            "cols": int((accel_profile or {}).get("cols", 2)),
            "source": "accel",
            "execution": execution,
        },
        "execution": execution,
    }
    out.update(
        materialize_accel_cleaning_artifacts(
            accel_outputs,
            params_effective=params_effective,
            sha256_file=sha256_file,
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
    if not rows and not to_bool(rule_param(params_effective, "allow_empty_output", True), default=True):
        raise RuntimeError("cleaning produced empty result")

    require_local_parquet_dependencies(params_effective)

    profile = build_profile(rows, quality, source)
    profile["quality_gate"] = quality_gate
    profile["preprocess"] = preprocess_result
    profile["execution"] = execution_report
    out = {"profile": profile, "execution": execution_report}
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

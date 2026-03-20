from __future__ import annotations

import time
from typing import Any, Callable, Dict, List

from aiwf.flows.cleaning_flow_helpers import materialize_office_outputs


def build_office_outputs_fn(
    *,
    office_rows_subset: Callable[..., Any],
    build_profile: Callable[..., Any],
    write_profile_illustration_png: Callable[..., Any],
    write_fin_xlsx: Callable[..., Any],
    write_audit_docx: Callable[..., Any],
    write_deck_pptx: Callable[..., Any],
    sha256_file: Callable[..., Any],
) -> Callable[..., Dict[str, Any]]:
    def office_outputs_fn(**kwargs: Any) -> Dict[str, Any]:
        return materialize_office_outputs(
            **kwargs,
            office_rows_subset=office_rows_subset,
            build_profile=build_profile,
            write_profile_illustration_png=write_profile_illustration_png,
            write_fin_xlsx=write_fin_xlsx,
            write_audit_docx=write_audit_docx,
            write_deck_pptx=write_deck_pptx,
            sha256_file=sha256_file,
        )

    return office_outputs_fn


def collect_materialized_artifacts(materialized: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = list(materialized.get("core_artifacts") or [])
    artifacts.extend(list(materialized.get("office_artifacts") or []))
    return artifacts


def register_artifacts(
    *,
    base_artifact_upsert: Callable[..., Any],
    base_url: str,
    job_id: str,
    actor: str,
    artifacts: List[Dict[str, Any]],
    headers: Dict[str, str],
) -> None:
    for artifact in artifacts:
        base_artifact_upsert(
            base_url=base_url,
            job_id=job_id,
            actor=actor,
            artifact_id=artifact["artifact_id"],
            kind=artifact["kind"],
            path=artifact["path"],
            sha256=artifact["sha256"],
            extra_json=None,
            headers=headers,
        )


def build_success_result(
    *,
    job_id: str,
    materialized: Dict[str, Any],
    artifacts: List[Dict[str, Any]],
    accel_result: Dict[str, Any],
    started_at: float,
) -> Dict[str, Any]:
    return {
        "ok": True,
        "job_id": job_id,
        "flow": "cleaning",
        "output_hash": materialized["sha_parquet"],
        "seconds": round(time.time() - started_at, 3),
        "artifacts": artifacts,
        "profile": materialized["profile"],
        "accel": {
            "attempted": accel_result["accel"].get("attempted", False),
            "ok": accel_result["accel"].get("ok", False),
            "used_fallback": not accel_result["use_accel_outputs"],
            "validation_error": accel_result["accel_validation_error"],
            "office_generation_mode": (accel_result["accel_resp"] or {}).get("office_generation_mode"),
            "office_generation_warning": (accel_result["accel_resp"] or {}).get("office_generation_warning"),
            "detail": accel_result["accel"],
        },
    }

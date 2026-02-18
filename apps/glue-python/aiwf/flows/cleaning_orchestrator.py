from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

from aiwf.flows.cleaning_flow_helpers import (
    materialize_accel_outputs,
    materialize_local_outputs,
    materialize_office_outputs,
    prepare_accel_result,
    prepare_job_layout,
    prepare_local_clean_cache,
    resolve_base_url,
)


def run_cleaning_flow(
    *,
    job_id: str,
    actor: str,
    ruleset_version: str,
    params: Dict[str, Any],
    s: Optional[Any],
    base: Optional[Any],
    hooks: Dict[str, Callable[..., Any]],
) -> Dict[str, Any]:
    ensure_dirs = hooks["_ensure_dirs"]
    load_raw_rows = hooks["_load_raw_rows"]
    clean_rows = hooks["_clean_rows"]
    rules_dict = hooks["_rules_dict"]
    to_bool = hooks["_to_bool"]
    rule_param = hooks["_rule_param"]
    is_generic_rules_enabled = hooks["_is_generic_rules_enabled"]
    try_accel_cleaning = hooks["_try_accel_cleaning"]
    is_valid_parquet_file = hooks["_is_valid_parquet_file"]
    office_rows_subset = hooks["_office_rows_subset"]
    build_profile = hooks["_build_profile"]
    write_profile_illustration_png = hooks["_write_profile_illustration_png"]
    write_fin_xlsx = hooks["_write_fin_xlsx"]
    write_audit_docx = hooks["_write_audit_docx"]
    write_deck_pptx = hooks["_write_deck_pptx"]
    sha256_file = hooks["_sha256_file"]
    apply_quality_gates = hooks["_apply_quality_gates"]
    require_local_parquet_dependencies = hooks["_require_local_parquet_dependencies"]
    write_cleaned_csv = hooks["_write_cleaned_csv"]
    write_cleaned_parquet = hooks["_write_cleaned_parquet"]
    local_parquet_strict_enabled = hooks["_local_parquet_strict_enabled"]
    write_profile_json = hooks["_write_profile_json"]
    base_step_start = hooks["_base_step_start"]
    headers_from_params = hooks["_headers_from_params"]
    maybe_preprocess_input = hooks["_maybe_preprocess_input"]
    base_artifact_upsert = hooks["_base_artifact_upsert"]
    base_step_done = hooks["_base_step_done"]
    base_step_fail = hooks["_base_step_fail"]

    t0 = time.time()
    params = params or {}
    base_url = resolve_base_url(s, base)
    headers = headers_from_params(params)
    layout = prepare_job_layout(job_id, params, ensure_dirs=ensure_dirs)

    step_id = "cleaning"
    try:
        base_step_start(
            base_url=base_url,
            job_id=job_id,
            step_id=step_id,
            actor=actor,
            ruleset_version=ruleset_version,
            input_uri=layout["input_uri"],
            output_uri=layout["output_uri"],
            params=params,
            headers=headers,
        )

        params_effective, preprocess_result = maybe_preprocess_input(params, layout["job_root"], layout["stage_dir"])
        local_cache = prepare_local_clean_cache(
            params_effective,
            layout["job_root"],
            load_raw_rows=load_raw_rows,
            clean_rows=clean_rows,
            rules_dict=rules_dict,
        )

        accel_result = prepare_accel_result(
            params_effective=params_effective,
            params_for_accel=local_cache["params_for_accel"],
            job_id=job_id,
            step_id=step_id,
            actor=actor,
            ruleset_version=ruleset_version,
            input_uri=layout["input_uri"],
            output_uri=layout["output_uri"],
            to_bool=to_bool,
            rule_param=rule_param,
            is_generic_rules_enabled=is_generic_rules_enabled,
            try_accel_cleaning=try_accel_cleaning,
            is_valid_parquet_file=is_valid_parquet_file,
        )

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

        if accel_result["use_accel_outputs"]:
            materialized = materialize_accel_outputs(
                job_id=job_id,
                artifacts_dir=layout["artifacts_dir"],
                params_effective=params_effective,
                local_rows=local_cache["local_rows"],
                local_quality=local_cache["local_quality"],
                accel_outputs=accel_result["accel_outputs"],
                accel_profile=accel_result["accel_profile"],
                sha256_file=sha256_file,
                materialize_office_outputs_fn=office_outputs_fn,
            )
        else:
            materialized = materialize_local_outputs(
                job_id=job_id,
                stage_dir=layout["stage_dir"],
                artifacts_dir=layout["artifacts_dir"],
                evidence_dir=layout["evidence_dir"],
                params_effective=params_effective,
                rows=local_cache["local_rows"],
                quality=local_cache["local_quality"],
                source=local_cache["source"],
                preprocess_result=preprocess_result,
                apply_quality_gates=apply_quality_gates,
                to_bool=to_bool,
                rule_param=rule_param,
                require_local_parquet_dependencies=require_local_parquet_dependencies,
                write_cleaned_csv=write_cleaned_csv,
                write_cleaned_parquet=write_cleaned_parquet,
                is_valid_parquet_file=is_valid_parquet_file,
                local_parquet_strict_enabled=local_parquet_strict_enabled,
                build_profile=build_profile,
                write_profile_json=write_profile_json,
                sha256_file=sha256_file,
                materialize_office_outputs_fn=office_outputs_fn,
            )

        artifacts = [
            {"artifact_id": "csv_cleaned_001", "kind": "csv", "path": materialized["cleaned_csv"], "sha256": materialized["sha_csv"]},
            {"artifact_id": "parquet_cleaned_001", "kind": "parquet", "path": materialized["cleaned_parquet"], "sha256": materialized["sha_parquet"]},
            {"artifact_id": "xlsx_fin_001", "kind": "xlsx", "path": materialized["xlsx_path"], "sha256": materialized["sha_xlsx"]},
            {"artifact_id": "docx_audit_001", "kind": "docx", "path": materialized["docx_path"], "sha256": materialized["sha_docx"]},
            {"artifact_id": "pptx_deck_001", "kind": "pptx", "path": materialized["pptx_path"], "sha256": materialized["sha_pptx"]},
            {"artifact_id": "profile_json_001", "kind": "json", "path": materialized["profile_json"], "sha256": materialized["sha_profile"]},
        ]

        for a in artifacts:
            base_artifact_upsert(
                base_url=base_url,
                job_id=job_id,
                actor=actor,
                artifact_id=a["artifact_id"],
                kind=a["kind"],
                path=a["path"],
                sha256=a["sha256"],
                extra_json=None,
                headers=headers,
            )

        base_step_done(
            base_url=base_url,
            job_id=job_id,
            step_id=step_id,
            actor=actor,
            output_hash=materialized["sha_parquet"],
            headers=headers,
        )

        return {
            "ok": True,
            "job_id": job_id,
            "flow": "cleaning",
            "output_hash": materialized["sha_parquet"],
            "seconds": round(time.time() - t0, 3),
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
    except Exception as e:
        try:
            base_step_fail(
                base_url=base_url,
                job_id=job_id,
                step_id=step_id,
                actor=actor,
                error=str(e),
                headers=headers,
            )
        except Exception:
            pass
        raise

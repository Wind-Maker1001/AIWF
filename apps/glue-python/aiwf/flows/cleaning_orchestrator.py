from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

from aiwf.flows.cleaning_flow_helpers import (
    materialize_accel_outputs,
    materialize_local_outputs,
    prepare_accel_result,
    prepare_job_layout,
    prepare_local_clean_cache,
    resolve_base_url,
)
from aiwf.flows.cleaning_errors import CleaningGuardrailError, guardrail_template_expected_profile, guardrail_template_id
from aiwf.flows.cleaning_orchestrator_support import (
    build_office_outputs_fn,
    build_success_result,
    collect_materialized_artifacts,
    register_artifacts,
)
from aiwf.governance_manual_reviews import enqueue_manual_reviews
from aiwf.governance_quality_rule_sets import apply_quality_rule_set_to_params


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
    prepare_cleaning_params = hooks["_prepare_cleaning_params"]
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
    params = apply_quality_rule_set_to_params(params or {})
    params = prepare_cleaning_params(params)
    public_params = {
        key: value
        for key, value in params.items()
        if not str(key).startswith("_")
    }
    base_url = resolve_base_url(s, base)
    headers = headers_from_params(params)
    layout = prepare_job_layout(job_id, params, ensure_dirs=ensure_dirs)
    local_standalone = bool(params.get("local_standalone"))

    def _template_driven_run(params_obj: Dict[str, Any]) -> bool:
        template_meta = params_obj.get("_resolved_cleaning_template") if isinstance(params_obj.get("_resolved_cleaning_template"), dict) else {}
        return bool(template_meta) or str(params_obj.get("cleaning_template") or "").strip().lower() not in {"", "default"}

    def _manual_review_queue_payload(review_analysis: Dict[str, Any]) -> list[Dict[str, Any]]:
        payload: list[Dict[str, Any]] = []
        for index, item in enumerate(review_analysis.get("review_items") or [], start=1):
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind") or "review").strip().lower() or "review"
            message = str(item.get("message") or f"{kind} requires manual review").strip()
            payload.append(
                {
                    "run_id": job_id,
                    "workflow_id": "cleaning",
                    "node_id": "cleaning/manual_review",
                    "review_key": f"cleaning::{kind}::{index}",
                    "comment": message,
                }
            )
        return payload

    step_id = "cleaning"
    try:
        if not local_standalone:
            base_step_start(
                base_url=base_url,
                job_id=job_id,
                step_id=step_id,
                actor=actor,
                ruleset_version=ruleset_version,
                input_uri=layout["input_uri"],
                output_uri=layout["output_uri"],
                params=public_params,
                headers=headers,
            )

        params_effective, preprocess_result = maybe_preprocess_input(params, layout["job_root"], layout["stage_dir"])
        params_effective = prepare_cleaning_params(params_effective)
        local_cache = prepare_local_clean_cache(
            params_effective,
            layout["job_root"],
            load_raw_rows=load_raw_rows,
            clean_rows=clean_rows,
            rules_dict=rules_dict,
        )
        review_analysis = (
            dict(local_cache["local_execution"].get("review_analysis") or {})
            if isinstance(local_cache.get("local_execution"), dict)
            else {}
        )
        manual_review_queue = {
            "review_required": bool(review_analysis.get("review_required", False)),
            "auto_enqueued": False,
            "enqueued_count": 0,
            "pending_total": 0,
            "items": [],
        }
        if manual_review_queue["review_required"] and (local_standalone or _template_driven_run(params_effective)):
            queue_items = _manual_review_queue_payload(review_analysis)
            if queue_items:
                queued = enqueue_manual_reviews(queue_items)
                manual_review_queue = {
                    "review_required": True,
                    "auto_enqueued": True,
                    "enqueued_count": len(queue_items),
                    "pending_total": len(queued),
                    "items": [
                        {
                            "review_key": str(item.get("review_key") or ""),
                            "comment": str(item.get("comment") or ""),
                        }
                        for item in queue_items
                    ],
                }
        local_cache["local_execution"]["manual_review_queue"] = manual_review_queue
        profile_analysis = (
            dict(local_cache["local_execution"].get("profile_analysis") or {})
            if isinstance(local_cache.get("local_execution"), dict)
            else {}
        )
        allow_empty_output = to_bool(
            rule_param(
                params_effective,
                "allow_empty_output",
                params_effective.get("blank_output_expected", True),
            ),
            default=bool(params_effective.get("blank_output_expected", True)),
        )
        if int(local_cache["local_quality"].get("output_rows", 0) or 0) <= 0 and not allow_empty_output:
            raise CleaningGuardrailError(
                error_code="zero_output_unexpected",
                message="cleaning blocked: output_rows=0 while blank output is not expected",
                reason_codes=["zero_output_unexpected"],
                requested_profile=str(profile_analysis.get("requested_profile") or ""),
                recommended_profile=str(profile_analysis.get("recommended_profile") or ""),
                profile_confidence=float(profile_analysis.get("profile_confidence") or 0.0),
                required_field_coverage=float(profile_analysis.get("required_field_coverage") or 0.0),
                template_id=guardrail_template_id(params_effective),
                template_expected_profile=guardrail_template_expected_profile(params_effective),
                blank_output_expected=bool(params_effective.get("blank_output_expected", False)),
                zero_output_unexpected=True,
                blocking_reason_codes=list(profile_analysis.get("blocking_reason_codes") or []) + ["zero_output_unexpected"],
                details={
                    "output_rows": int(local_cache["local_quality"].get("output_rows", 0) or 0),
                    "input_rows": int(local_cache["local_quality"].get("input_rows", 0) or 0),
                    "quality": dict(local_cache["local_quality"] or {}),
                },
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

        office_outputs_fn = build_office_outputs_fn(
            office_rows_subset=office_rows_subset,
            build_profile=build_profile,
            write_profile_illustration_png=write_profile_illustration_png,
            write_fin_xlsx=write_fin_xlsx,
            write_audit_docx=write_audit_docx,
            write_deck_pptx=write_deck_pptx,
            sha256_file=sha256_file,
        )

        if accel_result["use_accel_outputs"]:
            accel_quality_gate = apply_quality_gates(local_cache["local_quality"], params_effective)
            local_profile = build_profile(
                local_cache["local_rows"],
                local_cache["local_quality"],
                local_cache["source"],
            )
            local_profile["quality_gate"] = accel_quality_gate
            local_profile["preprocess"] = preprocess_result
            local_profile["execution"] = local_cache["local_execution"]
            materialized = materialize_accel_outputs(
                params_effective=params_effective,
                accel_outputs=accel_result["accel_outputs"],
                accel_profile=accel_result["accel_profile"],
                sha256_file=sha256_file,
                local_rows=local_cache["local_rows"],
                local_profile=local_profile,
                local_execution=local_cache["local_execution"],
                preprocess_result=preprocess_result,
                input_rows=local_cache["raw_rows"],
            )
        else:
            materialized = materialize_local_outputs(
                job_id=job_id,
                stage_dir=layout["stage_dir"],
                artifacts_dir=layout["artifacts_dir"],
                evidence_dir=layout["evidence_dir"],
                params_effective=params_effective,
                input_rows=local_cache["raw_rows"],
                rows=local_cache["local_rows"],
                quality=local_cache["local_quality"],
                execution_report=local_cache["local_execution"],
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

        artifacts = collect_materialized_artifacts(materialized)

        if not local_standalone:
            register_artifacts(
                base_artifact_upsert=base_artifact_upsert,
                base_url=base_url,
                job_id=job_id,
                actor=actor,
                artifacts=artifacts,
                headers=headers,
            )

        if not local_standalone:
            base_step_done(
                base_url=base_url,
                job_id=job_id,
                step_id=step_id,
                actor=actor,
                output_hash=materialized["sha_parquet"],
                headers=headers,
            )

        return build_success_result(
            job_id=job_id,
            materialized=materialized,
            artifacts=artifacts,
            accel_result=accel_result,
            started_at=t0,
        )
    except Exception as e:
        if not local_standalone:
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

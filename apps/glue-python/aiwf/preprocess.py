from __future__ import annotations

import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from aiwf import ingest
from aiwf.accel_client import (
    postprocess_rows_v1_operator,
    quality_check_v2_operator,
    transform_rows_v3_operator,
)
from aiwf.cleaning_spec_v2 import (
    CLEANING_SPEC_V2_VERSION,
    cleaning_spec_to_transform_components,
    compile_preprocess_spec_to_spec,
)
from aiwf.preprocess_cli import parse_args as _parse_args_impl, run_cli as _run_cli_impl
from aiwf.preprocess_conflicts import (
    _apply_conflict_detection,
    _chunk_text,
)
from aiwf.preprocess_evidence import _to_canonical_evidence_row
from aiwf.preprocess_io import (
    _detect_input_format,
    _detect_output_format,
    _read_csv,
    _read_json,
    _read_jsonl,
    _read_rows,
    _write_csv,
    _write_json,
    _write_jsonl,
    _write_rows,
)
from aiwf.preprocess_reporting import (
    _build_quality_report,
    export_canonical_bundle,
)
from aiwf.preprocess_service import preprocess_file_impl
from aiwf.preprocess_pipeline import (
    default_pipeline_stage_executor as _default_pipeline_stage_executor_impl,
    run_preprocess_pipeline_impl,
)
from aiwf.preprocess_ops import (
    _normalize_amount,
    _normalize_date,
    _normalize_header,
)
from aiwf.preprocess_runtime import preprocess_rows_impl
from aiwf.preprocess_stages import (
    default_pipeline_stage_prepare_config as _default_pipeline_stage_prepare_config_impl,
)
from aiwf.preprocess_validation import (
    validate_preprocess_pipeline_impl,
    validate_preprocess_spec_impl,
)


from aiwf.preprocess_registry import (
    FieldTransformRegistration,
    PipelineStageContext,
    PipelineStageExecutorFn,
    PipelineStagePrepareFn,
    PipelineStageRegistration,
    PipelineStageValidatorFn,
    PreprocessFilterFn,
    PreprocessTransformFn,
    get_field_transform,
    get_pipeline_stage,
    get_row_filter,
    list_field_transform_details,
    list_field_transform_domains,
    list_field_transforms,
    list_pipeline_stage_details,
    list_pipeline_stage_domains,
    list_pipeline_stages,
    list_row_filter_details,
    list_row_filter_domains,
    list_row_filters,
    register_field_transform,
    register_pipeline_stage as _register_pipeline_stage_impl,
    register_row_filter,
    unregister_field_transform,
    unregister_pipeline_stage,
    unregister_row_filter,
)


_RUST_V2_PREPROCESS_FILTER_OPS = {
    "exists",
    "not_exists",
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "contains",
    "regex",
    "not_regex",
}


def _preprocess_postprocess_config(compiled_spec: Dict[str, Any]) -> Dict[str, Any]:
    transform = compiled_spec.get("transform") if isinstance(compiled_spec.get("transform"), dict) else {}
    postprocess = transform.get("postprocess") if isinstance(transform.get("postprocess"), dict) else {}
    return dict(postprocess)


def _preprocess_postprocess_stage_names(compiled_spec: Dict[str, Any]) -> List[str]:
    postprocess = _preprocess_postprocess_config(compiled_spec)
    stages: List[str] = []
    if bool(postprocess.get("standardize_evidence")):
        stages.append("standardize_evidence")
    if str(postprocess.get("chunk_mode") or "none").strip().lower() not in {"", "none", "off"}:
        stages.append("chunk_text")
    if bool(postprocess.get("detect_conflicts")):
        stages.append("detect_conflicts")
    return stages


def _preprocess_rust_v2_capability_report(spec: Dict[str, Any], compiled_spec: Dict[str, Any]) -> Dict[str, Any]:
    enabled = str(spec.get("use_rust_v2") or os.getenv("AIWF_PREPROCESS_RUST_V2_ENABLED") or "").strip().lower()
    postprocess = _preprocess_postprocess_config(compiled_spec)
    row_filters = spec.get("row_filters") if isinstance(spec.get("row_filters"), list) else []
    warnings = compiled_spec.get("audit", {}).get("warnings") if isinstance(compiled_spec.get("audit", {}).get("warnings"), list) else []
    postprocess_stages = _preprocess_postprocess_stage_names(compiled_spec)
    enabled_flag = enabled in {"1", "true", "yes", "on"}

    checks = [
        {
            "name": "flag_enabled",
            "ok": enabled_flag,
            "reason": "flag_disabled",
        },
        {
            "name": "builtin_field_transforms_only",
            "ok": not any("unsupported field transform" in str(item).lower() for item in warnings),
            "reason": "unsupported_field_transform",
        },
        {
            "name": "builtin_row_filters_only",
            "ok": all(
                isinstance(item, dict)
                and str(item.get("op") or "").strip().lower() in _RUST_V2_PREPROCESS_FILTER_OPS
                for item in row_filters
            ),
            "reason": "unsupported_row_filter",
        },
        {
            "name": "postprocess_rows_v1_available",
            "ok": True,
            "reason": "postprocess_rows_v1_unavailable",
        },
    ]
    transform_checks = [item for item in checks if item["name"] != "postprocess_rows_v1_available"]
    failing_transform = next((item for item in transform_checks if not item["ok"]), None)
    row_transform_eligible = enabled_flag and failing_transform is None
    postprocess_required = len(postprocess_stages) > 0
    if not enabled_flag:
        execution_plan = "python_only"
        eligibility_reason = "flag_disabled"
    elif failing_transform is not None:
        execution_plan = "python_only"
        eligibility_reason = str(failing_transform["reason"])
    elif postprocess_required:
        execution_plan = "rust_transform_postprocess_quality"
        eligibility_reason = "mixed_rust_postprocess"
    else:
        execution_plan = "rust_transform_only"
        eligibility_reason = "eligible"
    return {
        "eligible": row_transform_eligible,
        "row_transform_eligible": row_transform_eligible,
        "postprocess_required": postprocess_required,
        "postprocess_stages": postprocess_stages,
        "execution_plan": execution_plan,
        "eligibility_reason": eligibility_reason,
        "checks": checks,
    }


def register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    domain: Optional[str] = None,
    domain_metadata: Optional[Dict[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> PipelineStageRegistration:
    return _register_pipeline_stage_impl(
        name,
        validator=validator,
        prepare_config=prepare_config,
        executor=executor,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=source_module,
        on_conflict=on_conflict,
        default_validator=validate_preprocess_spec,
        default_prepare_config=_default_pipeline_stage_prepare_config,
    )

def _filter_match(row: Dict[str, Any], f: Dict[str, Any]) -> bool:
    op = str(f.get("op") or "eq").strip().lower()
    try:
        return get_row_filter(op).handler(row, f)
    except KeyError:
        return True


def _apply_field_transform(value: Any, op: str, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    try:
        return get_field_transform(op).handler(value, cfg)
    except KeyError:
        return value, False


def validate_preprocess_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    return validate_preprocess_spec_impl(
        spec,
        field_transform_ops=list_field_transforms(),
        row_filter_specs={
            item["op"]: item["requires_field"]
            for item in list_row_filter_details()
        },
    )


def validate_preprocess_pipeline(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    return validate_preprocess_pipeline_impl(
        pipeline,
        list_pipeline_stages=list_pipeline_stages,
        get_pipeline_registration=get_pipeline_stage,
    )


def _default_pipeline_stage_prepare_config(context: PipelineStageContext) -> Dict[str, Any]:
    return _default_pipeline_stage_prepare_config_impl(context)


def _preprocess_execution_report(
    *,
    execution_mode: str,
    execution_audit: Dict[str, Any],
    eligibility_reason: str,
    capability_report: Dict[str, Any],
    row_transform_engine: str = "",
    postprocess_engine: str = "",
    quality_gate_engine: str = "",
    materialization_engine: str = "python",
    stage_provenance: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "execution_mode": execution_mode,
        "execution_audit": execution_audit,
        "eligibility_reason": eligibility_reason,
        "capability_matrix": capability_report.get("checks") or [],
        "execution_plan": str(capability_report.get("execution_plan") or ""),
        "row_transform_eligible": bool(capability_report.get("row_transform_eligible", False)),
        "postprocess_required": bool(capability_report.get("postprocess_required", False)),
        "row_transform_engine": row_transform_engine,
        "postprocess_engine": postprocess_engine or "none",
        "quality_gate_engine": quality_gate_engine or "none",
        "materialization_engine": materialization_engine,
        "stage_provenance": list(stage_provenance or []),
    }


def _preprocess_summary_from_quality(
    quality: Dict[str, Any],
    _compiled_spec: Dict[str, Any],
    *,
    execution_mode: str,
    execution_audit: Dict[str, Any],
    eligibility_reason: str,
    capability_report: Dict[str, Any],
    postprocess_quality: Optional[Dict[str, Any]] = None,
    row_transform_engine: str = "",
    postprocess_engine: str = "",
    quality_gate_engine: str = "",
    stage_provenance: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    post = dict(postprocess_quality or {})
    return {
        "input_rows": int(quality.get("input_rows", 0)),
        "output_rows": int(post.get("output_rows", quality.get("output_rows", 0))),
        "dropped_empty_rows": 0,
        "dropped_by_filters": int(quality.get("filtered_rows", 0)),
        "duplicate_rows_removed": int(quality.get("duplicate_rows_removed", 0)),
        "normalized_amount_cells": int(quality.get("numeric_cells_parsed", 0)),
        "normalized_date_cells": int(quality.get("date_cells_parsed", 0)),
        "transformed_cells": 0,
        "standardized_rows": int(post.get("standardized_rows", 0)),
        "chunked_rows_created": int(post.get("chunked_rows_created", 0)),
        "conflict_rows_marked": int(post.get("conflict_rows_marked", 0)),
        "rust_v2_used": bool(quality.get("rust_v2_used", False) or quality.get("rust_v3_used", False) or quality.get("rust_transform_used", False)),
        "rust_transform_operator": str(quality.get("rust_transform_operator") or execution_audit.get("operator") or ""),
        "cleaning_spec_version": CLEANING_SPEC_V2_VERSION,
        **_preprocess_execution_report(
            execution_mode=execution_mode,
            execution_audit=execution_audit,
            eligibility_reason=eligibility_reason,
            capability_report=capability_report,
            row_transform_engine=row_transform_engine,
            postprocess_engine=postprocess_engine,
            quality_gate_engine=quality_gate_engine,
            stage_provenance=stage_provenance,
        ),
    }


def _preprocess_stage_provenance(
    *,
    row_transform_engine: str,
    postprocess_engine: str,
    quality_gate_engine: str,
    postprocess_stages: List[str],
) -> List[Dict[str, Any]]:
    stages = [
        {"stage": "row_transform", "engine": row_transform_engine or "none"},
    ]
    for stage in postprocess_stages:
        stages.append({"stage": stage, "engine": postprocess_engine or "none"})
    stages.append({"stage": "quality_check", "engine": quality_gate_engine or "none"})
    stages.append({"stage": "materialize", "engine": "python"})
    return stages


def _preprocess_stage_plan(
    *,
    row_transform_engine: str,
    postprocess_engine: str,
    quality_gate_engine: str,
    postprocess_stages: List[str],
) -> Dict[str, Any]:
    requested = {"standardize_evidence", "chunk_text", "detect_conflicts"}
    enabled = set(postprocess_stages)
    stages = [
        {
            "name": "row_transform",
            "operator": row_transform_engine or "none",
            "engine": row_transform_engine or "none",
            "enabled": bool(row_transform_engine and row_transform_engine != "none"),
        }
    ]
    for stage in ["standardize_evidence", "chunk_text", "detect_conflicts"]:
        stages.append(
            {
                "name": stage,
                "operator": postprocess_engine if stage in enabled else "none",
                "engine": postprocess_engine if stage in enabled else "none",
                "enabled": stage in requested and stage in enabled,
            }
        )
    stages.append(
        {
            "name": "quality_check",
            "operator": quality_gate_engine or "none",
            "engine": quality_gate_engine or "none",
            "enabled": bool(quality_gate_engine and quality_gate_engine != "none"),
        }
    )
    stages.append(
        {
            "name": "materialize",
            "operator": "python_write_rows",
            "engine": "python",
            "enabled": True,
        }
    )
    return {
        "schema_version": "preprocess_stage_plan.v1",
        "stages": stages,
    }


def _preprocess_quality_check_rules(compiled_spec: Dict[str, Any]) -> Dict[str, Any]:
    quality = compiled_spec.get("quality") if isinstance(compiled_spec.get("quality"), dict) else {}
    schema = compiled_spec.get("schema") if isinstance(compiled_spec.get("schema"), dict) else {}
    transform = compiled_spec.get("transform") if isinstance(compiled_spec.get("transform"), dict) else {}
    gates = dict(quality.get("gates") or {}) if isinstance(quality.get("gates"), dict) else {}
    required_fields = [str(item) for item in (quality.get("required_fields") or []) if str(item).strip()]
    unique_keys = [str(item) for item in (schema.get("unique_keys") or []) if str(item).strip()]
    deduplicate_by = [str(item) for item in (transform.get("deduplicate_by") or []) if str(item).strip()]
    rules = dict(gates)
    if required_fields:
        rules["required_fields"] = required_fields
    if unique_keys:
        rules["unique_keys"] = unique_keys
    elif deduplicate_by:
        rules["deduplicate_by"] = deduplicate_by
    return rules

def run_preprocess_pipeline(
    *,
    pipeline: Dict[str, Any],
    job_root: str,
    stage_dir: str,
    input_path: str,
    final_output_path: Optional[str] = None,
) -> Dict[str, Any]:
    return run_preprocess_pipeline_impl(
        pipeline=pipeline,
        job_root=job_root,
        stage_dir=stage_dir,
        input_path=input_path,
        final_output_path=final_output_path,
        validate_pipeline=validate_preprocess_pipeline,
        get_pipeline_stage=get_pipeline_stage,
        pipeline_stage_context_type=PipelineStageContext,
        default_stage_executor=_default_pipeline_stage_executor_impl,
        preprocess_file=preprocess_file,
    )


def preprocess_rows(rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    compiled_spec = compile_preprocess_spec_to_spec(spec)
    capability_report = _preprocess_rust_v2_capability_report(spec, compiled_spec)
    rust_v2_error = ""
    if capability_report["eligible"]:
        rules, quality_gates, schema_hint = cleaning_spec_to_transform_components(
            compiled_spec,
            input_rows=rows,
        )
        postprocess_stages = list(capability_report.get("postprocess_stages") or [])
        rust_v3 = transform_rows_v3_operator(
            raw_rows=rows,
            params=spec,
            rules=rules,
            quality_gates=quality_gates,
            schema_hint={**schema_hint, "source": "glue-python.preprocess"},
        )
        if rust_v3.get("ok"):
            transform_rows = list(rust_v3.get("rows") or [])
            transform_quality = dict(rust_v3.get("quality") or {})
            execution_audit = (
                rust_v3.get("audit")
                if isinstance(rust_v3.get("audit"), dict)
                else dict(transform_quality.get("rust_v3_audit") or transform_quality.get("rust_v2_audit") or {})
            )
            execution_audit["operator"] = str((rust_v3.get("response") or {}).get("operator") or "transform_rows_v3")
            row_transform_engine = "transform_rows_v3"
            postprocess_engine = ""
            quality_gate_engine = ""
            postprocess_quality: Dict[str, Any] = {}
            final_rows = transform_rows

            if postprocess_stages:
                postprocess_engine = "postprocess_rows_v1"
                postprocess_payload = {
                    "standardize_evidence": bool(spec.get("standardize_evidence", False)),
                    "evidence_schema": dict(spec.get("evidence_schema") or {}) if isinstance(spec.get("evidence_schema"), dict) else {},
                    "chunk_mode": str(spec.get("chunk_mode") or "none"),
                    "chunk_field": str(spec.get("chunk_field") or ""),
                    "chunk_max_chars": int(spec.get("chunk_max_chars", 500) or 500),
                    "detect_conflicts": bool(spec.get("detect_conflicts", False)),
                    "conflict_topic_field": str(spec.get("conflict_topic_field") or ""),
                    "conflict_stance_field": str(spec.get("conflict_stance_field") or ""),
                    "conflict_text_field": str(spec.get("conflict_text_field") or ""),
                    "conflict_positive_words": [str(item) for item in (spec.get("conflict_positive_words") or [])],
                    "conflict_negative_words": [str(item) for item in (spec.get("conflict_negative_words") or [])],
                    "schema_hint": {"schema_version": CLEANING_SPEC_V2_VERSION, "source": "glue-python.preprocess.postprocess"},
                }
                postprocess_result = postprocess_rows_v1_operator(
                    rows=transform_rows,
                    params=spec,
                    payload=postprocess_payload,
                )
                if not postprocess_result.get("ok"):
                    rust_v2_error = str(postprocess_result.get("error") or "postprocess_rows_v1_not_ok")
                    capability_report = {
                        **capability_report,
                        "eligible": False,
                        "execution_plan": "python_only",
                        "eligibility_reason": "postprocess_rows_v1_error",
                    }
                else:
                    final_rows = list(postprocess_result.get("rows") or [])
                    postprocess_quality = dict(postprocess_result.get("quality") or {})
                    post_audit = postprocess_result.get("audit") if isinstance(postprocess_result.get("audit"), dict) else {}
                    execution_audit["postprocess"] = post_audit

            if not rust_v2_error:
                quality_check_engine = "quality_check_v2"
                quality_check_rules = _preprocess_quality_check_rules(compiled_spec)
                quality_metrics = {
                    "required_missing_ratio": transform_quality.get("required_missing_ratio"),
                    "numeric_parse_rate": transform_quality.get("numeric_parse_rate"),
                    "date_parse_rate": transform_quality.get("date_parse_rate"),
                    "duplicate_key_ratio": transform_quality.get("duplicate_key_ratio"),
                    "blank_row_ratio": transform_quality.get("blank_row_ratio"),
                }
                quality_check = quality_check_v2_operator(
                    rows=final_rows,
                    params=spec,
                    rules=quality_check_rules,
                    metrics=quality_metrics,
                )
                if not quality_check.get("ok"):
                    rust_v2_error = str(quality_check.get("error") or "quality_check_v2_not_ok")
                    capability_report = {
                        **capability_report,
                        "eligible": False,
                        "execution_plan": "python_only",
                        "eligibility_reason": "quality_check_v2_error",
                    }
                else:
                    quality_gate_engine = quality_check_engine
                    execution_audit["quality_check"] = {
                        "operator": "quality_check_v2",
                        "passed": bool(quality_check.get("passed", True)),
                        "report": dict(quality_check.get("report") or {}),
                    }
                    if not bool(quality_check.get("passed", True)):
                        report = dict(quality_check.get("report") or {})
                        violations = report.get("violations") if isinstance(report.get("violations"), list) else []
                        message = "; ".join(str(item) for item in violations if str(item).strip()) or "quality_check_v2 failed"
                        raise RuntimeError(message)

            if not rust_v2_error:
                stage_provenance = _preprocess_stage_provenance(
                    row_transform_engine=row_transform_engine,
                    postprocess_engine=postprocess_engine,
                    quality_gate_engine=quality_gate_engine,
                    postprocess_stages=postprocess_stages,
                )
                execution_audit["stage_provenance"] = stage_provenance
                execution_audit["stage_plan"] = _preprocess_stage_plan(
                    row_transform_engine=row_transform_engine,
                    postprocess_engine=postprocess_engine,
                    quality_gate_engine=quality_gate_engine,
                    postprocess_stages=postprocess_stages,
                )
                summary = _preprocess_summary_from_quality(
                    transform_quality,
                    compiled_spec,
                    execution_mode="rust_v3" if not postprocess_stages else "rust_v3_postprocess_v1",
                    execution_audit=execution_audit,
                    eligibility_reason=str(capability_report["eligibility_reason"]),
                    capability_report=capability_report,
                    postprocess_quality=postprocess_quality,
                    row_transform_engine=row_transform_engine,
                    postprocess_engine=postprocess_engine,
                    quality_gate_engine=quality_gate_engine,
                    stage_provenance=stage_provenance,
                )
                return final_rows, summary

        if not rust_v2_error:
            rust_v2_error = str(rust_v3.get("error") or "rust_v3_not_ok")
        capability_report = {
            **capability_report,
            "eligible": False,
            "execution_plan": "python_only",
            "eligibility_reason": "rust_v3_error",
        }

    rows_out, summary = preprocess_rows_impl(
        rows,
        spec,
        normalize_header=_normalize_header,
        normalize_amount=_normalize_amount,
        normalize_date=_normalize_date,
        apply_field_transform=_apply_field_transform,
        filter_match=_filter_match,
        chunk_text=_chunk_text,
        to_canonical_evidence_row=_to_canonical_evidence_row,
        apply_conflict_detection=_apply_conflict_detection,
    )
    summary.update(
        _preprocess_execution_report(
            execution_mode="python_legacy",
            execution_audit={
                "schema": "python_preprocess.audit.v1",
                "rule_hits": {
                    "filtered_by_rule": int(summary.get("dropped_by_filters", 0)),
                    "duplicate_removed": int(summary.get("duplicate_rows_removed", 0)),
                    "transformed_cells": int(summary.get("transformed_cells", 0)),
                },
                "rust_v2_error": rust_v2_error,
            },
            eligibility_reason=str(capability_report["eligibility_reason"]),
            capability_report=capability_report,
            row_transform_engine="python",
            postprocess_engine="python" if capability_report.get("postprocess_required") else "none",
            quality_gate_engine="none",
            stage_provenance=_preprocess_stage_provenance(
                row_transform_engine="python",
                postprocess_engine="python" if capability_report.get("postprocess_required") else "none",
                quality_gate_engine="none",
                postprocess_stages=list(capability_report.get("postprocess_stages") or []),
            ),
        )
    )
    summary["execution_audit"]["stage_plan"] = _preprocess_stage_plan(
        row_transform_engine="python",
        postprocess_engine="python" if capability_report.get("postprocess_required") else "none",
        quality_gate_engine="none",
        postprocess_stages=list(capability_report.get("postprocess_stages") or []),
    )
    return rows_out, summary


def preprocess_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    compiled_spec = compile_preprocess_spec_to_spec(spec)
    result = preprocess_file_impl(
        input_path,
        output_path,
        spec,
        read_rows=_read_rows,
        preprocess_rows=preprocess_rows,
        write_rows=_write_rows,
        build_quality_report=_build_quality_report,
        write_json=_write_json,
        export_canonical_bundle=export_canonical_bundle,
    )
    result["cleaning_spec_version"] = CLEANING_SPEC_V2_VERSION
    result["cleaning_spec"] = compiled_spec
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    result["execution_mode"] = str(summary.get("execution_mode") or "")
    result["execution_audit"] = dict(summary.get("execution_audit") or {})
    result["eligibility_reason"] = str(summary.get("eligibility_reason") or "")
    return result


def preprocess_csv_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    # Backward-compatible wrapper.
    return preprocess_file(input_path, output_path, spec)


def _parse_args():
    return _parse_args_impl()


def main() -> int:
    return _run_cli_impl(
        _parse_args(),
        validate_preprocess_spec=validate_preprocess_spec,
        preprocess_file=preprocess_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())

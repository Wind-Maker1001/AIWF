from __future__ import annotations

import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from aiwf import ingest
from aiwf.accel_client import transform_rows_v2_operator
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


def _preprocess_rust_v2_capability_report(spec: Dict[str, Any], compiled_spec: Dict[str, Any]) -> Dict[str, Any]:
    enabled = str(spec.get("use_rust_v2") or os.getenv("AIWF_PREPROCESS_RUST_V2_ENABLED") or "").strip().lower()
    postprocess = compiled_spec.get("transform", {}).get("postprocess", {})
    row_filters = spec.get("row_filters") if isinstance(spec.get("row_filters"), list) else []
    warnings = compiled_spec.get("audit", {}).get("warnings") if isinstance(compiled_spec.get("audit", {}).get("warnings"), list) else []

    checks = [
        {
            "name": "flag_enabled",
            "ok": enabled in {"1", "true", "yes", "on"},
            "reason": "flag_disabled",
        },
        {
            "name": "standardize_evidence_disabled",
            "ok": not bool(postprocess.get("standardize_evidence")),
            "reason": "standardize_evidence_enabled",
        },
        {
            "name": "detect_conflicts_disabled",
            "ok": not bool(postprocess.get("detect_conflicts")),
            "reason": "detect_conflicts_enabled",
        },
        {
            "name": "chunking_disabled",
            "ok": str(postprocess.get("chunk_mode") or "none").strip().lower() in {"", "none", "off"},
            "reason": "chunk_mode_enabled",
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
    ]
    failing = next((item for item in checks if not item["ok"]), None)
    return {
        "eligible": failing is None,
        "eligibility_reason": "eligible" if failing is None else str(failing["reason"]),
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
) -> Dict[str, Any]:
    return {
        "execution_mode": execution_mode,
        "execution_audit": execution_audit,
        "eligibility_reason": eligibility_reason,
        "capability_matrix": capability_report.get("checks") or [],
    }


def _preprocess_summary_from_quality(
    quality: Dict[str, Any],
    _compiled_spec: Dict[str, Any],
    *,
    execution_mode: str,
    execution_audit: Dict[str, Any],
    eligibility_reason: str,
    capability_report: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "input_rows": int(quality.get("input_rows", 0)),
        "output_rows": int(quality.get("output_rows", 0)),
        "dropped_empty_rows": 0,
        "dropped_by_filters": int(quality.get("filtered_rows", 0)),
        "duplicate_rows_removed": int(quality.get("duplicate_rows_removed", 0)),
        "normalized_amount_cells": int(quality.get("numeric_cells_parsed", 0)),
        "normalized_date_cells": int(quality.get("date_cells_parsed", 0)),
        "transformed_cells": 0,
        "standardized_rows": 0,
        "chunked_rows_created": 0,
        "conflict_rows_marked": 0,
        "rust_v2_used": bool(quality.get("rust_v2_used", False)),
        "cleaning_spec_version": CLEANING_SPEC_V2_VERSION,
        **_preprocess_execution_report(
            execution_mode=execution_mode,
            execution_audit=execution_audit,
            eligibility_reason=eligibility_reason,
            capability_report=capability_report,
        ),
    }

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
        rust_v2 = transform_rows_v2_operator(
            raw_rows=rows,
            params=spec,
            rules=rules,
            quality_gates=quality_gates,
            schema_hint={**schema_hint, "source": "glue-python.preprocess"},
        )
        if rust_v2.get("ok"):
            quality = dict(rust_v2.get("quality") or {})
            execution_audit = (
                rust_v2.get("audit")
                if isinstance(rust_v2.get("audit"), dict)
                else dict(quality.get("rust_v2_audit") or {})
            )
            summary = _preprocess_summary_from_quality(
                quality,
                compiled_spec,
                execution_mode="rust_v2",
                execution_audit=execution_audit,
                eligibility_reason=str(capability_report["eligibility_reason"]),
                capability_report=capability_report,
            )
            return rust_v2["rows"], summary
        rust_v2_error = str(rust_v2.get("error") or "rust_v2_not_ok")
        capability_report = {
            **capability_report,
            "eligible": False,
            "eligibility_reason": "rust_v2_error",
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
        )
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

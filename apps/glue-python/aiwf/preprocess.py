from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from aiwf import ingest
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
    register_builtin_preprocess_ops as _register_builtin_preprocess_ops_impl,
)
from aiwf.preprocess_runtime import preprocess_rows_impl
from aiwf.preprocess_stages import (
    default_pipeline_stage_prepare_config as _default_pipeline_stage_prepare_config_impl,
    register_builtin_pipeline_stages as _register_builtin_pipeline_stages_impl,
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
    list_field_transforms,
    list_pipeline_stage_details,
    list_pipeline_stages,
    list_row_filter_details,
    list_row_filters,
    register_field_transform,
    register_pipeline_stage as _register_pipeline_stage_impl,
    register_row_filter,
    unregister_field_transform,
    unregister_pipeline_stage,
    unregister_row_filter,
)


def register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> PipelineStageRegistration:
    return _register_pipeline_stage_impl(
        name,
        validator=validator,
        prepare_config=prepare_config,
        executor=executor,
        source_module=source_module,
        on_conflict=on_conflict,
        default_validator=validate_preprocess_spec,
        default_prepare_config=_default_pipeline_stage_prepare_config,
    )


_register_builtin_preprocess_ops_impl(register_field_transform, register_row_filter)


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


_register_builtin_pipeline_stages_impl(register_pipeline_stage)


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
    return preprocess_rows_impl(
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


def preprocess_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    return preprocess_file_impl(
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

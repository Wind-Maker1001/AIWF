from __future__ import annotations

import argparse
import json
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from aiwf import ingest
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
    return dict(context.config)


def _prepare_extract_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("output_format", "jsonl")
    return cfg


def _prepare_clean_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("trim_strings", True)
    return cfg


def _prepare_structure_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("standardize_evidence", True)
    cfg.setdefault("output_format", "jsonl")
    return cfg


def _prepare_audit_stage_config(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("generate_quality_report", True)
    cfg.setdefault("output_format", "jsonl")
    if "quality_report_path" not in cfg:
        cfg["quality_report_path"] = os.path.join(
            context.stage_dir,
            f"pre_stage_{context.stage_index+1}_audit_quality.json",
        )
    return cfg


def _register_builtin_pipeline_stages() -> None:
    register_pipeline_stage("extract", prepare_config=_prepare_extract_stage_config)
    register_pipeline_stage("clean", prepare_config=_prepare_clean_stage_config)
    register_pipeline_stage("structure", prepare_config=_prepare_structure_stage_config)
    register_pipeline_stage("audit", prepare_config=_prepare_audit_stage_config)


_register_builtin_pipeline_stages()


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
    rows, meta = _read_rows(input_path, spec)
    out_rows, summary = preprocess_rows(rows, spec)
    out_fmt = _write_rows(output_path, out_rows, spec)
    quality_report_path = None
    if bool(spec.get("generate_quality_report", False)):
        quality_report_path = str(spec.get("quality_report_path") or f"{output_path}.quality.json")
        report = _build_quality_report(out_rows, summary, spec)
        _write_json(quality_report_path, report)
    canonical_bundle = None
    if bool(spec.get("export_canonical_bundle", False)):
        canonical_bundle = export_canonical_bundle(
            rows=out_rows,
            summary=summary,
            meta=meta,
            output_path=output_path,
            spec=spec,
        )
    return {
        "input_path": input_path,
        "output_path": output_path,
        "input_format": meta.get("input_format"),
        "output_format": out_fmt,
        "delimiter": meta.get("delimiter"),
        "skipped_files": meta.get("skipped_files"),
        "failed_files": meta.get("failed_files"),
        "quality_report_path": quality_report_path,
        "canonical_bundle": canonical_bundle,
        "summary": summary,
    }


def preprocess_csv_file(input_path: str, output_path: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    # Backward-compatible wrapper.
    return preprocess_file(input_path, output_path, spec)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AIWF raw-to-cooked preprocessing")
    p.add_argument("--input", required=True, help="input path (csv/json/jsonl)")
    p.add_argument("--output", required=True, help="output path (csv/json/jsonl)")
    p.add_argument("--config", required=False, help="JSON/YAML config path for preprocess spec")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    spec: Dict[str, Any] = {}
    if args.config:
        with open(args.config, "r", encoding="utf-8-sig") as f:
            text = f.read()
        ext = os.path.splitext(args.config)[1].lower()
        if ext in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except Exception as e:
                print(json.dumps({"ok": False, "errors": [f"yaml support requires pyyaml: {e}"]}, ensure_ascii=False))
                return 2
            loaded = yaml.safe_load(text)
        else:
            loaded = json.loads(text)
        if isinstance(loaded, dict):
            spec = loaded.get("preprocess") if isinstance(loaded.get("preprocess"), dict) else loaded
        else:
            print(json.dumps({"ok": False, "errors": ["config must be an object"]}, ensure_ascii=False))
            return 2

    val = validate_preprocess_spec(spec)
    if not val["ok"]:
        print(json.dumps(val, ensure_ascii=False))
        return 2
    res = preprocess_file(args.input, args.output, spec)
    print(json.dumps({"ok": True, "result": res, "warnings": val.get("warnings", [])}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

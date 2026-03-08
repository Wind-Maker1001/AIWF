from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

from aiwf.paths import resolve_path_within_root


def _stage_output_ext(output_format: str, fallback: str = ".csv") -> str:
    m = str(output_format or "").strip().lower()
    if m == "json":
        return ".json"
    if m == "jsonl":
        return ".jsonl"
    return fallback


def pipeline_stage_output_path(context: Any) -> str:
    ext = _stage_output_ext(str(context.config.get("output_format") or "csv"))
    return os.path.join(context.stage_dir, f"pre_stage_{context.stage_index+1}_{context.stage_name}{ext}")


def default_pipeline_stage_executor(context: Any, *, preprocess_file: Callable[..., Dict[str, Any]]) -> Dict[str, Any]:
    output_path = pipeline_stage_output_path(context)
    result = preprocess_file(context.input_path, output_path, context.config)
    return {
        "stage": context.stage_name,
        "output_path": output_path,
        "result": result,
    }


def run_preprocess_pipeline_impl(
    *,
    pipeline: Dict[str, Any],
    job_root: str,
    stage_dir: str,
    input_path: str,
    final_output_path: Optional[str],
    validate_pipeline: Callable[[Dict[str, Any]], Dict[str, Any]],
    get_pipeline_stage: Callable[[str], Any],
    pipeline_stage_context_type: type,
    default_stage_executor: Callable[..., Dict[str, Any]],
    preprocess_file: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    vr = validate_pipeline(pipeline)
    if not vr.get("ok"):
        raise RuntimeError(f"preprocess pipeline invalid: {vr.get('errors')}")

    os.makedirs(stage_dir, exist_ok=True)
    current_input = input_path
    stage_results = []
    stages = pipeline.get("stages") if isinstance(pipeline.get("stages"), list) else []

    for i, stage in enumerate(stages):
        name = str(stage.get("name") or "").strip().lower()
        registration = get_pipeline_stage(name)
        cfg = dict(stage.get("config") if isinstance(stage.get("config"), dict) else {})
        context = pipeline_stage_context_type(
            stage_index=i,
            stage_name=name,
            input_path=current_input,
            stage_dir=stage_dir,
            job_root=job_root,
            config=cfg,
        )
        prepared_context = pipeline_stage_context_type(
            stage_index=context.stage_index,
            stage_name=context.stage_name,
            input_path=context.input_path,
            stage_dir=context.stage_dir,
            job_root=context.job_root,
            config=registration.prepare_config(context),
        )
        stage_run = (
            registration.executor(prepared_context)
            if registration.executor is not None
            else default_stage_executor(prepared_context, preprocess_file=preprocess_file)
        )
        stage_output = str(stage_run.get("output_path") or "")
        if not stage_output:
            raise RuntimeError(f"pipeline stage {name} did not return output_path")
        stage_results.append(
            {
                "stage": name,
                "output_path": stage_output,
                "result": stage_run.get("result"),
            }
        )
        current_input = stage_output

    final_out = str(final_output_path or os.path.join(stage_dir, "preprocessed_input.csv"))
    final_out = resolve_path_within_root(job_root, final_out)
    final_res = preprocess_file(current_input, final_out, {})

    return {
        "mode": "pipeline",
        "input_path": input_path,
        "output_path": final_out,
        "stages": stage_results,
        "final": final_res,
        "warnings": vr.get("warnings", []),
    }

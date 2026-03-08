from __future__ import annotations

import os
from typing import Any, Dict, Tuple

from aiwf.flows.registry import register_flow
from aiwf.ingest import register_input_reader
from aiwf.preprocess import (
    PipelineStageContext,
    register_field_transform,
    register_pipeline_stage,
    register_row_filter,
)


def run_echo(job_id: str, actor: str = "glue", ruleset_version: str = "v1", params: Dict[str, Any] | None = None, **_: Any) -> Dict[str, Any]:
    payload = dict(params or {})
    return {
        "ok": True,
        "job_id": job_id,
        "actor": actor,
        "ruleset_version": ruleset_version,
        "echo": payload,
    }


def _load_demo_input(path: str, options: Dict[str, Any]) -> Tuple[list[Dict[str, Any]], Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        rows = []
        for index, line in enumerate(f):
            text = line.strip()
            if not text:
                continue
            rows.append(
                {
                    "text": text,
                    "source_path": path,
                    "source_file": os.path.basename(path),
                    "source_type": "demo",
                    "chunk_index": index,
                }
            )
    return rows, {"input_format": "demo"}


def _transform_prefix_demo(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if value is None:
        return value, False
    return f"{str(cfg.get('prefix') or 'demo:')}{value}", True


def _filter_has_token(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = str(cfg.get("field") or "").strip()
    if not field:
        return True
    token = str(cfg.get("value") or "").strip()
    return token in str(row.get(field) or "")


def _prepare_demo_stage(context: PipelineStageContext) -> Dict[str, Any]:
    cfg = dict(context.config)
    cfg.setdefault("output_format", "jsonl")
    transforms = list(cfg.get("field_transforms") or [])
    transforms.append({"field": "text", "op": "prefix_demo", "prefix": "[demo] "})
    cfg["field_transforms"] = transforms
    return cfg


register_flow("echo", runner=run_echo)
register_input_reader("demo", [".demo"], _load_demo_input)
register_field_transform("prefix_demo", _transform_prefix_demo)
register_row_filter("has_token", _filter_has_token)
register_pipeline_stage("demo_stage", prepare_config=_prepare_demo_stage)

from __future__ import annotations

import os
from typing import Any, Callable, Dict


def default_pipeline_stage_prepare_config(context: Any) -> Dict[str, Any]:
    return dict(context.config)


def prepare_extract_stage_config(context: Any) -> Dict[str, Any]:
    config = dict(context.config)
    config.setdefault("output_format", "jsonl")
    return config


def prepare_clean_stage_config(context: Any) -> Dict[str, Any]:
    config = dict(context.config)
    config.setdefault("trim_strings", True)
    return config


def prepare_structure_stage_config(context: Any) -> Dict[str, Any]:
    config = dict(context.config)
    config.setdefault("standardize_evidence", True)
    config.setdefault("output_format", "jsonl")
    return config


def prepare_audit_stage_config(context: Any) -> Dict[str, Any]:
    config = dict(context.config)
    config.setdefault("generate_quality_report", True)
    config.setdefault("output_format", "jsonl")
    if "quality_report_path" not in config:
        config["quality_report_path"] = os.path.join(
            context.stage_dir,
            f"pre_stage_{context.stage_index+1}_audit_quality.json",
        )
    return config


def register_builtin_pipeline_stages(register_pipeline_stage: Callable[..., Any]) -> None:
    domain = {
        "name": "preprocess",
        "label": "Preprocess",
        "backend": "python",
        "builtin": True,
    }
    register_pipeline_stage("extract", prepare_config=prepare_extract_stage_config, domain="preprocess", domain_metadata=domain)
    register_pipeline_stage("clean", prepare_config=prepare_clean_stage_config, domain="preprocess", domain_metadata=domain)
    register_pipeline_stage("structure", prepare_config=prepare_structure_stage_config, domain="preprocess", domain_metadata=domain)
    register_pipeline_stage("audit", prepare_config=prepare_audit_stage_config, domain="preprocess", domain_metadata=domain)

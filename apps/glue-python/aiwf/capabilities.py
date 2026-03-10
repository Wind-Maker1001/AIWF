from __future__ import annotations

from typing import Any, Dict

from aiwf.extensions import extension_status, load_extension_modules
from aiwf.registry_events import list_registry_events
from aiwf.registry_policy import default_conflict_policy
from aiwf.flows.artifact_selection import normalize_artifact_selection
from aiwf.flows.cleaning_artifacts import (
    list_cleaning_artifact_details,
    list_cleaning_artifact_tokens,
    list_cleaning_artifacts,
)
from aiwf.flows.office_artifacts import (
    list_office_artifact_details,
    list_office_artifact_tokens,
    list_office_artifacts,
)
from aiwf.flows.registry import list_flow_details, list_flows
from aiwf.ingest import list_input_formats, list_input_reader_details
from aiwf.preprocess import (
    list_field_transform_details,
    list_field_transforms,
    list_pipeline_stage_details,
    list_pipeline_stages,
    list_row_filter_details,
    list_row_filters,
)


def collect_capabilities() -> Dict[str, Any]:
    load_extension_modules()
    return {
        "flows": list_flows(),
        "flow_details": list_flow_details(),
        "input_formats": list_input_formats(),
        "input_format_details": list_input_reader_details(),
        "preprocess": {
            "field_transforms": list_field_transforms(),
            "field_transform_details": list_field_transform_details(),
            "row_filters": list_row_filters(),
            "row_filter_details": list_row_filter_details(),
            "pipeline_stages": list_pipeline_stages(),
            "pipeline_stage_details": list_pipeline_stage_details(),
        },
        "artifacts": {
            "core": list_cleaning_artifacts(),
            "core_details": list_cleaning_artifact_details(),
            "office": list_office_artifacts(),
            "office_details": list_office_artifact_details(),
            "selection_schema": normalize_artifact_selection({}),
            "selection_tokens": {
                "core": list_cleaning_artifact_tokens(),
                "office": list_office_artifact_tokens(),
            },
        },
        "extensions": extension_status(),
        "registry": {
            "default_conflict_policy": default_conflict_policy(),
            "events": list_registry_events(),
        },
    }

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Dict

from aiwf.extensions import extension_status, load_extension_modules
from aiwf.flows.artifact_selection import normalize_artifact_selection
from aiwf.flows.cleaning_artifacts import (
    list_cleaning_artifact_details,
    list_cleaning_artifact_domains,
    list_cleaning_artifact_tokens,
    list_cleaning_artifacts,
)
from aiwf.flows.office_artifacts import (
    list_office_artifact_details,
    list_office_artifact_domains,
    list_office_artifact_tokens,
    list_office_artifacts,
)
from aiwf.flows.registry import (
    get_flow_runner,
    list_flow_details,
    list_flow_domains,
    list_flows,
)
from aiwf.ingest import list_input_formats, list_input_reader_details, list_input_reader_domains
from aiwf.preprocess import (
    list_field_transform_details,
    list_field_transform_domains,
    list_field_transforms,
    list_pipeline_stage_details,
    list_pipeline_stage_domains,
    list_pipeline_stages,
    list_row_filter_details,
    list_row_filter_domains,
    list_row_filters,
)
from aiwf.registry_events import list_registry_events
from aiwf.registry_policy import default_conflict_policy
from aiwf.runtime_state import create_runtime_state, use_runtime_state


class GlueRuntimeCatalog:
    def __init__(self) -> None:
        self._state = create_runtime_state()

    def activate(self) -> AbstractContextManager:
        return use_runtime_state(self._state)

    def ensure_ready(self, *, force_extensions: bool = False) -> "GlueRuntimeCatalog":
        with use_runtime_state(self._state):
            load_extension_modules(force=force_extensions)
        return self

    def get_flow_runner(self, name: str):
        with use_runtime_state(self._state):
            self.ensure_ready()
            return get_flow_runner(name)

    def list_flows(self) -> list[str]:
        with use_runtime_state(self._state):
            self.ensure_ready()
            return list_flows()

    def capabilities(self) -> Dict[str, Any]:
        with use_runtime_state(self._state):
            self.ensure_ready()
            return {
                "flows": list_flows(),
                "flow_details": list_flow_details(),
                "flow_domains": list_flow_domains(),
                "input_formats": list_input_formats(),
                "input_format_details": list_input_reader_details(),
                "input_domains": list_input_reader_domains(),
                "preprocess": {
                    "field_transforms": list_field_transforms(),
                    "field_transform_details": list_field_transform_details(),
                    "field_transform_domains": list_field_transform_domains(),
                    "row_filters": list_row_filters(),
                    "row_filter_details": list_row_filter_details(),
                    "row_filter_domains": list_row_filter_domains(),
                    "pipeline_stages": list_pipeline_stages(),
                    "pipeline_stage_details": list_pipeline_stage_details(),
                    "pipeline_stage_domains": list_pipeline_stage_domains(),
                },
                "artifacts": {
                    "core": list_cleaning_artifacts(),
                    "core_details": list_cleaning_artifact_details(),
                    "core_domains": list_cleaning_artifact_domains(),
                    "office": list_office_artifacts(),
                    "office_details": list_office_artifact_details(),
                    "office_domains": list_office_artifact_domains(),
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


_DEFAULT_RUNTIME_CATALOG = GlueRuntimeCatalog()


def get_runtime_catalog() -> GlueRuntimeCatalog:
    return _DEFAULT_RUNTIME_CATALOG

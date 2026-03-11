from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List


@dataclass
class GlueRuntimeState:
    flows: Dict[str, Any] = field(default_factory=dict)
    flow_aliases: Dict[str, str] = field(default_factory=dict)
    builtins_flows_registered: bool = False
    flow_bootstrap_in_progress: bool = False

    input_readers_by_format: Dict[str, Any] = field(default_factory=dict)
    input_readers_by_extension: Dict[str, Any] = field(default_factory=dict)
    builtins_inputs_registered: bool = False
    input_bootstrap_in_progress: bool = False

    field_transforms: Dict[str, Any] = field(default_factory=dict)
    row_filters: Dict[str, Any] = field(default_factory=dict)
    pipeline_stages: Dict[str, Any] = field(default_factory=dict)
    builtins_preprocess_registered: bool = False
    preprocess_bootstrap_in_progress: bool = False

    cleaning_artifacts: Dict[str, Any] = field(default_factory=dict)
    builtins_cleaning_artifacts_registered: bool = False
    cleaning_artifact_bootstrap_in_progress: bool = False

    office_artifacts: Dict[str, Any] = field(default_factory=dict)
    builtins_office_artifacts_registered: bool = False
    office_artifact_bootstrap_in_progress: bool = False

    loaded_modules: List[str] = field(default_factory=list)
    failed_modules: Dict[str, str] = field(default_factory=dict)
    load_attempted: bool = False
    loading: bool = False

    registry_events: List[Dict[str, Any]] = field(default_factory=list)


def create_runtime_state() -> GlueRuntimeState:
    return GlueRuntimeState()


_DEFAULT_RUNTIME_STATE = create_runtime_state()
_ACTIVE_RUNTIME_STATE: ContextVar[GlueRuntimeState] = ContextVar(
    "aiwf_glue_runtime_state",
    default=_DEFAULT_RUNTIME_STATE,
)


def get_runtime_state() -> GlueRuntimeState:
    return _ACTIVE_RUNTIME_STATE.get()


@contextmanager
def use_runtime_state(state: GlueRuntimeState) -> Iterator[GlueRuntimeState]:
    token = _ACTIVE_RUNTIME_STATE.set(state)
    try:
        yield state
    finally:
        _ACTIVE_RUNTIME_STATE.reset(token)

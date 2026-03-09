from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.registry_utils import infer_caller_module


PreprocessTransformFn = Callable[[Any, Dict[str, Any]], Tuple[Any, bool]]
PreprocessFilterFn = Callable[[Dict[str, Any], Dict[str, Any]], bool]
PipelineStageValidatorFn = Callable[[Dict[str, Any]], Dict[str, Any]]
PipelineStagePrepareFn = Callable[["PipelineStageContext"], Dict[str, Any]]
PipelineStageExecutorFn = Callable[["PipelineStageContext"], Dict[str, Any]]


@dataclass(frozen=True)
class FieldTransformRegistration:
    op: str
    handler: PreprocessTransformFn
    source_module: str


@dataclass(frozen=True)
class RowFilterRegistration:
    op: str
    handler: PreprocessFilterFn
    requires_field: bool
    source_module: str


@dataclass(frozen=True)
class PipelineStageContext:
    stage_index: int
    stage_name: str
    input_path: str
    stage_dir: str
    job_root: str
    config: Dict[str, Any]


@dataclass(frozen=True)
class PipelineStageRegistration:
    name: str
    validator: PipelineStageValidatorFn
    prepare_config: PipelineStagePrepareFn
    executor: Optional[PipelineStageExecutorFn]
    source_module: str


_FIELD_TRANSFORMS: Dict[str, FieldTransformRegistration] = {}
_ROW_FILTERS: Dict[str, RowFilterRegistration] = {}
_PIPELINE_STAGES: Dict[str, PipelineStageRegistration] = {}


def _normalize_preprocess_op(op: str) -> str:
    normalized = str(op or "").strip().lower()
    if not normalized:
        raise ValueError("preprocess op must be non-empty")
    return normalized


def register_field_transform(
    op: str,
    handler: PreprocessTransformFn,
    *,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FieldTransformRegistration:
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("field transform handler must be callable")
    source = str(source_module or infer_caller_module())
    existing = _FIELD_TRANSFORMS.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="field_transform",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"field transform {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="field_transform",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="field_transform",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = FieldTransformRegistration(
        op=normalized,
        handler=handler,
        source_module=source,
    )
    _FIELD_TRANSFORMS[normalized] = registration
    return registration


def unregister_field_transform(op: str) -> Optional[FieldTransformRegistration]:
    normalized = _normalize_preprocess_op(op)
    return _FIELD_TRANSFORMS.pop(normalized, None)


def get_field_transform(op: str) -> FieldTransformRegistration:
    normalized = _normalize_preprocess_op(op)
    registration = _FIELD_TRANSFORMS.get(normalized)
    if registration is None:
        raise KeyError(f"unknown field transform: {normalized}")
    return registration


def list_field_transforms() -> List[str]:
    return sorted(_FIELD_TRANSFORMS.keys())


def list_field_transform_details() -> List[Dict[str, Any]]:
    return [
        {
            "op": registration.op,
            "source_module": registration.source_module,
        }
        for registration in sorted(_FIELD_TRANSFORMS.values(), key=lambda item: item.op)
    ]


def register_row_filter(
    op: str,
    handler: PreprocessFilterFn,
    *,
    requires_field: bool = True,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> RowFilterRegistration:
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("row filter handler must be callable")
    source = str(source_module or infer_caller_module())
    existing = _ROW_FILTERS.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="row_filter",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"row filter {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="row_filter",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="row_filter",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = RowFilterRegistration(
        op=normalized,
        handler=handler,
        requires_field=requires_field,
        source_module=source,
    )
    _ROW_FILTERS[normalized] = registration
    return registration


def unregister_row_filter(op: str) -> Optional[RowFilterRegistration]:
    normalized = _normalize_preprocess_op(op)
    return _ROW_FILTERS.pop(normalized, None)


def get_row_filter(op: str) -> RowFilterRegistration:
    normalized = _normalize_preprocess_op(op)
    registration = _ROW_FILTERS.get(normalized)
    if registration is None:
        raise KeyError(f"unknown row filter: {normalized}")
    return registration


def list_row_filters() -> List[str]:
    return sorted(_ROW_FILTERS.keys())


def list_row_filter_details() -> List[Dict[str, Any]]:
    return [
        {
            "op": registration.op,
            "requires_field": registration.requires_field,
            "source_module": registration.source_module,
        }
        for registration in sorted(_ROW_FILTERS.values(), key=lambda item: item.op)
    ]


def register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
    default_validator: Optional[PipelineStageValidatorFn] = None,
    default_prepare_config: Optional[PipelineStagePrepareFn] = None,
) -> PipelineStageRegistration:
    normalized = _normalize_preprocess_op(name)
    source = str(source_module or infer_caller_module())
    existing = _PIPELINE_STAGES.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="pipeline_stage",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"pipeline stage {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="pipeline_stage",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="pipeline_stage",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )

    effective_validator = validator or default_validator
    effective_prepare = prepare_config or default_prepare_config
    if effective_validator is None:
        raise ValueError("pipeline stage validator is required")
    if effective_prepare is None:
        raise ValueError("pipeline stage prepare_config is required")

    registration = PipelineStageRegistration(
        name=normalized,
        validator=effective_validator,
        prepare_config=effective_prepare,
        executor=executor,
        source_module=source,
    )
    _PIPELINE_STAGES[normalized] = registration
    return registration


def unregister_pipeline_stage(name: str) -> Optional[PipelineStageRegistration]:
    normalized = _normalize_preprocess_op(name)
    return _PIPELINE_STAGES.pop(normalized, None)


def get_pipeline_stage(name: str) -> PipelineStageRegistration:
    normalized = _normalize_preprocess_op(name)
    registration = _PIPELINE_STAGES.get(normalized)
    if registration is None:
        raise KeyError(f"unknown pipeline stage: {normalized}")
    return registration


def list_pipeline_stages() -> List[str]:
    return sorted(_PIPELINE_STAGES.keys())


def list_pipeline_stage_details() -> List[Dict[str, Any]]:
    return [
        {
            "name": registration.name,
            "has_custom_executor": registration.executor is not None,
            "source_module": registration.source_module,
        }
        for registration in sorted(_PIPELINE_STAGES.values(), key=lambda item: item.name)
    ]

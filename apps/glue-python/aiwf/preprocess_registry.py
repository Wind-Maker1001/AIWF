from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from aiwf.registry_domains import normalize_registry_domain, summarize_registry_domains
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.runtime_state import get_runtime_state
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
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


@dataclass(frozen=True)
class RowFilterRegistration:
    op: str
    handler: PreprocessFilterFn
    requires_field: bool
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
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
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


def _normalize_preprocess_op(op: str) -> str:
    normalized = str(op or "").strip().lower()
    if not normalized:
        raise ValueError("preprocess op must be non-empty")
    return normalized


def _ensure_builtin_preprocess_registry() -> None:
    state = get_runtime_state()
    if state.builtins_preprocess_registered or state.preprocess_bootstrap_in_progress:
        return
    state.preprocess_bootstrap_in_progress = True
    try:
        from aiwf.preprocess_ops import register_builtin_preprocess_ops
        from aiwf.preprocess_stages import (
            default_pipeline_stage_prepare_config,
            register_builtin_pipeline_stages,
        )
        from aiwf.preprocess_validation import validate_preprocess_spec_impl

        register_builtin_preprocess_ops(_register_field_transform, _register_row_filter)
        def _builtin_stage_validator(spec: Dict[str, Any]) -> Dict[str, Any]:
            return validate_preprocess_spec_impl(
                spec,
                field_transform_ops=sorted(state.field_transforms.keys()),
                row_filter_specs={
                    item.op: item.requires_field
                    for item in state.row_filters.values()
                },
            )

        def _builtin_stage_registrar(name: str, **kwargs: Any) -> PipelineStageRegistration:
            return _register_pipeline_stage(
                name,
                default_validator=_builtin_stage_validator,
                default_prepare_config=default_pipeline_stage_prepare_config,
                **kwargs,
            )

        register_builtin_pipeline_stages(_builtin_stage_registrar)
        state.builtins_preprocess_registered = True
    finally:
        state.preprocess_bootstrap_in_progress = False


def _register_field_transform(
    op: str,
    handler: PreprocessTransformFn,
    *,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FieldTransformRegistration:
    state = get_runtime_state()
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("field transform handler must be callable")
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    existing = state.field_transforms.get(normalized)
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
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.field_transforms[normalized] = registration
    return registration


def register_field_transform(
    op: str,
    handler: PreprocessTransformFn,
    *,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FieldTransformRegistration:
    _ensure_builtin_preprocess_registry()
    effective_source = source_module or infer_caller_module()
    return _register_field_transform(
        op,
        handler,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_field_transform(op: str) -> Optional[FieldTransformRegistration]:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(op)
    return get_runtime_state().field_transforms.pop(normalized, None)


def get_field_transform(op: str) -> FieldTransformRegistration:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(op)
    registration = get_runtime_state().field_transforms.get(normalized)
    if registration is None:
        raise KeyError(f"unknown field transform: {normalized}")
    return registration


def list_field_transforms() -> List[str]:
    _ensure_builtin_preprocess_registry()
    return sorted(get_runtime_state().field_transforms.keys())


def list_field_transform_details() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return [
        {
            "op": registration.op,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in sorted(state.field_transforms.values(), key=lambda item: item.op)
    ]


def list_field_transform_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return summarize_registry_domains(
        sorted(state.field_transforms.values(), key=lambda item: item.op),
        item_name_attr="op",
        list_key="field_transforms",
    )


def _register_row_filter(
    op: str,
    handler: PreprocessFilterFn,
    *,
    requires_field: bool = True,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> RowFilterRegistration:
    state = get_runtime_state()
    normalized = _normalize_preprocess_op(op)
    if not callable(handler):
        raise TypeError("row filter handler must be callable")
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    existing = state.row_filters.get(normalized)
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
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.row_filters[normalized] = registration
    return registration


def register_row_filter(
    op: str,
    handler: PreprocessFilterFn,
    *,
    requires_field: bool = True,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> RowFilterRegistration:
    _ensure_builtin_preprocess_registry()
    effective_source = source_module or infer_caller_module()
    return _register_row_filter(
        op,
        handler,
        requires_field=requires_field,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_row_filter(op: str) -> Optional[RowFilterRegistration]:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(op)
    return get_runtime_state().row_filters.pop(normalized, None)


def get_row_filter(op: str) -> RowFilterRegistration:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(op)
    registration = get_runtime_state().row_filters.get(normalized)
    if registration is None:
        raise KeyError(f"unknown row filter: {normalized}")
    return registration


def list_row_filters() -> List[str]:
    _ensure_builtin_preprocess_registry()
    return sorted(get_runtime_state().row_filters.keys())


def list_row_filter_details() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return [
        {
            "op": registration.op,
            "requires_field": registration.requires_field,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in sorted(state.row_filters.values(), key=lambda item: item.op)
    ]


def list_row_filter_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return summarize_registry_domains(
        sorted(state.row_filters.values(), key=lambda item: item.op),
        item_name_attr="op",
        list_key="row_filters",
    )


def _register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
    default_validator: Optional[PipelineStageValidatorFn] = None,
    default_prepare_config: Optional[PipelineStagePrepareFn] = None,
) -> PipelineStageRegistration:
    state = get_runtime_state()
    normalized = _normalize_preprocess_op(name)
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    existing = state.pipeline_stages.get(normalized)
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
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.pipeline_stages[normalized] = registration
    return registration


def register_pipeline_stage(
    name: str,
    *,
    validator: Optional[PipelineStageValidatorFn] = None,
    prepare_config: Optional[PipelineStagePrepareFn] = None,
    executor: Optional[PipelineStageExecutorFn] = None,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
    default_validator: Optional[PipelineStageValidatorFn] = None,
    default_prepare_config: Optional[PipelineStagePrepareFn] = None,
) -> PipelineStageRegistration:
    _ensure_builtin_preprocess_registry()
    effective_source = source_module or infer_caller_module()
    return _register_pipeline_stage(
        name,
        validator=validator,
        prepare_config=prepare_config,
        executor=executor,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
        default_validator=default_validator,
        default_prepare_config=default_prepare_config,
    )


def unregister_pipeline_stage(name: str) -> Optional[PipelineStageRegistration]:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(name)
    return get_runtime_state().pipeline_stages.pop(normalized, None)


def get_pipeline_stage(name: str) -> PipelineStageRegistration:
    _ensure_builtin_preprocess_registry()
    normalized = _normalize_preprocess_op(name)
    registration = get_runtime_state().pipeline_stages.get(normalized)
    if registration is None:
        raise KeyError(f"unknown pipeline stage: {normalized}")
    return registration


def list_pipeline_stages() -> List[str]:
    _ensure_builtin_preprocess_registry()
    return sorted(get_runtime_state().pipeline_stages.keys())


def list_pipeline_stage_details() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return [
        {
            "name": registration.name,
            "has_custom_executor": registration.executor is not None,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in sorted(state.pipeline_stages.values(), key=lambda item: item.name)
    ]


def list_pipeline_stage_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_preprocess_registry()
    state = get_runtime_state()
    return summarize_registry_domains(
        sorted(state.pipeline_stages.values(), key=lambda item: item.name),
        item_name_attr="name",
        list_key="pipeline_stages",
    )

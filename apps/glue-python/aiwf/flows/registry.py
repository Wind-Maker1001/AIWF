from __future__ import annotations

from dataclasses import dataclass, replace
from importlib import import_module
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.registry_utils import infer_caller_module


FlowRunner = Callable[..., Any]


@dataclass(frozen=True)
class FlowRegistration:
    name: str
    aliases: Tuple[str, ...]
    runner: Optional[FlowRunner]
    module_path: Optional[str]
    attr_name: Optional[str]
    source_module: str


_FLOWS: Dict[str, FlowRegistration] = {}
_FLOW_ALIASES: Dict[str, str] = {}


def _normalize_flow_name(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("flow name must be non-empty")
    return normalized


def _alias_conflicts(normalized: str, alias_names: Tuple[str, ...]) -> Dict[str, FlowRegistration]:
    conflicts: Dict[str, FlowRegistration] = {}
    for alias in alias_names:
        canonical = _FLOW_ALIASES.get(alias)
        if canonical is None or canonical == normalized:
            continue
        registration = _FLOWS.get(canonical)
        if registration is None:
            continue
        conflicts[alias] = registration
    return conflicts


def register_flow(
    name: str,
    *,
    runner: Optional[FlowRunner] = None,
    module_path: Optional[str] = None,
    attr_name: Optional[str] = None,
    aliases: Iterable[str] = (),
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FlowRegistration:
    normalized = _normalize_flow_name(name)
    alias_names = tuple(
        alias
        for alias in dict.fromkeys(_normalize_flow_name(alias) for alias in aliases)
        if alias != normalized
    )
    if runner is None and not module_path:
        raise ValueError("register_flow requires either runner or module_path")
    if runner is not None and not callable(runner):
        raise TypeError("flow runner must be callable")
    source = str(source_module or infer_caller_module())
    policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
    existing = _FLOWS.get(normalized)
    if existing is not None:
        if policy == "error":
            record_registry_event(
                registry="flow",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"flow {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="flow",
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
            registry="flow",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
        unregister_flow(normalized)

    alias_conflicts = _alias_conflicts(normalized, alias_names)
    if alias_conflicts:
        first_conflict = next(iter(alias_conflicts.values()))
        canonical_conflicts = {alias: registration for alias, registration in alias_conflicts.items() if alias == registration.name}
        if canonical_conflicts:
            for alias, registration in canonical_conflicts.items():
                record_registry_event(
                    registry="flow_alias",
                    name=alias,
                    action="error",
                    policy=policy,
                    existing_source_module=registration.source_module,
                    new_source_module=source,
                    detail=f"alias {alias} conflicts with existing canonical flow name",
                )
            raise RuntimeError(
                "flow aliases conflict with canonical flow names: "
                + ", ".join(sorted(canonical_conflicts.keys()))
            )
        if policy == "error":
            for alias, registration in alias_conflicts.items():
                record_registry_event(
                    registry="flow_alias",
                    name=alias,
                    action="error",
                    policy=policy,
                    existing_source_module=registration.source_module,
                    new_source_module=source,
                    detail=f"alias {alias} already registered to {registration.name}",
                )
            raise RuntimeError(
                "flow aliases already registered: "
                + ", ".join(f"{alias}->{registration.name}" for alias, registration in sorted(alias_conflicts.items()))
            )
        if policy == "keep":
            for alias, registration in alias_conflicts.items():
                record_registry_event(
                    registry="flow_alias",
                    name=alias,
                    action="keep",
                    policy=policy,
                    existing_source_module=registration.source_module,
                    new_source_module=source,
                    detail=f"kept alias {alias} on {registration.name}",
                )
            return first_conflict

        action = "replace_with_warning" if policy == "warn" else "replace"
        for alias, registration in alias_conflicts.items():
            updated_aliases = tuple(value for value in registration.aliases if value != alias)
            _FLOWS[registration.name] = replace(registration, aliases=updated_aliases)
            record_registry_event(
                registry="flow_alias",
                name=alias,
                action=action,
                policy=policy,
                existing_source_module=registration.source_module,
                new_source_module=source,
                detail=f"alias {alias} moved from {registration.name} to {normalized}",
            )
            _FLOW_ALIASES.pop(alias, None)

    registration = FlowRegistration(
        name=normalized,
        aliases=alias_names,
        runner=runner,
        module_path=module_path,
        attr_name=attr_name,
        source_module=source,
    )
    _FLOWS[normalized] = registration
    _FLOW_ALIASES[normalized] = normalized
    for alias in alias_names:
        _FLOW_ALIASES[alias] = normalized
    return registration


def unregister_flow(name: str) -> Optional[FlowRegistration]:
    normalized = _normalize_flow_name(name)
    canonical = _FLOW_ALIASES.get(normalized, normalized)
    registration = _FLOWS.pop(canonical, None)
    if registration is None:
        return None
    for alias, target in list(_FLOW_ALIASES.items()):
        if target == canonical:
            del _FLOW_ALIASES[alias]
    return registration


def get_flow_registration(name: str) -> FlowRegistration:
    normalized = _normalize_flow_name(name)
    canonical = _FLOW_ALIASES.get(normalized)
    if canonical is None or canonical not in _FLOWS:
        raise KeyError(f"unknown flow: {normalized}")
    return _FLOWS[canonical]


def get_flow_runner(name: str) -> FlowRunner:
    registration = get_flow_registration(name)
    if registration.runner is not None:
        return registration.runner

    if not registration.module_path:
        raise RuntimeError(f"flow {registration.name} has no module path")

    module = import_module(registration.module_path)
    attr_name = registration.attr_name or f"run_{registration.name}"
    runner = getattr(module, attr_name, None)
    if runner is None or not callable(runner):
        raise RuntimeError(
            f"flow {registration.name} missing callable {attr_name} in {registration.module_path}"
        )
    return runner


def list_flows() -> list[str]:
    return sorted(_FLOWS.keys())


def list_flow_details() -> list[dict[str, Any]]:
    return [
        {
            "name": registration.name,
            "aliases": list(registration.aliases),
            "module_path": registration.module_path,
            "attr_name": registration.attr_name,
            "source_module": registration.source_module,
        }
        for registration in sorted(_FLOWS.values(), key=lambda item: item.name)
    ]


register_flow(
    "cleaning",
    module_path="aiwf.flows.cleaning",
    attr_name="run_cleaning",
)

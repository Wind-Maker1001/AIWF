from __future__ import annotations

from dataclasses import dataclass, replace
from importlib import import_module
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.runtime_state import get_runtime_state
from aiwf.registry_utils import infer_caller_module


FlowRunner = Callable[..., Any]


@dataclass(frozen=True)
class FlowRegistration:
    name: str
    aliases: Tuple[str, ...]
    runner: Optional[FlowRunner]
    module_path: Optional[str]
    attr_name: Optional[str]
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


def _normalize_flow_name(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("flow name must be non-empty")
    return normalized


def _normalize_flow_domain_name(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("flow domain name must be non-empty")
    return normalized


def _normalize_flow_domain(
    domain: Optional[str],
    domain_metadata: Optional[Mapping[str, Any]],
) -> tuple[Optional[str], Mapping[str, Any]]:
    metadata = dict(domain_metadata or {})
    normalized_domain = _normalize_flow_domain_name(domain) if domain is not None else None
    metadata_name = metadata.get("name")
    if metadata_name is not None:
        normalized_metadata_name = _normalize_flow_domain_name(metadata_name)
        if normalized_domain is not None and normalized_metadata_name != normalized_domain:
            raise ValueError("flow domain and domain metadata name must match")
        normalized_domain = normalized_metadata_name
    if normalized_domain is None:
        if metadata:
            raise ValueError("flow domain metadata requires a domain name")
        return None, MappingProxyType({})
    metadata["name"] = normalized_domain
    return normalized_domain, MappingProxyType(metadata)


def _alias_conflicts(normalized: str, alias_names: Tuple[str, ...]) -> Dict[str, FlowRegistration]:
    state = get_runtime_state()
    conflicts: Dict[str, FlowRegistration] = {}
    for alias in alias_names:
        canonical = state.flow_aliases.get(alias)
        if canonical is None or canonical == normalized:
            continue
        registration = state.flows.get(canonical)
        if registration is None:
            continue
        conflicts[alias] = registration
    return conflicts


def _ensure_builtin_flow_domains() -> None:
    state = get_runtime_state()
    if state.builtins_flows_registered or state.flow_bootstrap_in_progress:
        return
    state.flow_bootstrap_in_progress = True
    try:
        from aiwf.flows.domains import register_builtin_flow_domains

        register_builtin_flow_domains(_register_flow)
        state.builtins_flows_registered = True
    finally:
        state.flow_bootstrap_in_progress = False


def _register_flow(
    name: str,
    *,
    runner: Optional[FlowRunner] = None,
    module_path: Optional[str] = None,
    attr_name: Optional[str] = None,
    aliases: Iterable[str] = (),
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FlowRegistration:
    state = get_runtime_state()
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
    normalized_domain, normalized_domain_metadata = _normalize_flow_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
    existing = state.flows.get(normalized)
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
            state.flows[registration.name] = replace(registration, aliases=updated_aliases)
            record_registry_event(
                registry="flow_alias",
                name=alias,
                action=action,
                policy=policy,
                existing_source_module=registration.source_module,
                new_source_module=source,
                detail=f"alias {alias} moved from {registration.name} to {normalized}",
            )
            state.flow_aliases.pop(alias, None)

    registration = FlowRegistration(
        name=normalized,
        aliases=alias_names,
        runner=runner,
        module_path=module_path,
        attr_name=attr_name,
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.flows[normalized] = registration
    state.flow_aliases[normalized] = normalized
    for alias in alias_names:
        state.flow_aliases[alias] = normalized
    return registration


def register_flow(
    name: str,
    *,
    runner: Optional[FlowRunner] = None,
    module_path: Optional[str] = None,
    attr_name: Optional[str] = None,
    aliases: Iterable[str] = (),
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> FlowRegistration:
    _ensure_builtin_flow_domains()
    effective_source = source_module or infer_caller_module()
    return _register_flow(
        name,
        runner=runner,
        module_path=module_path,
        attr_name=attr_name,
        aliases=aliases,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_flow(name: str) -> Optional[FlowRegistration]:
    _ensure_builtin_flow_domains()
    state = get_runtime_state()
    normalized = _normalize_flow_name(name)
    canonical = state.flow_aliases.get(normalized, normalized)
    registration = state.flows.pop(canonical, None)
    if registration is None:
        return None
    for alias, target in list(state.flow_aliases.items()):
        if target == canonical:
            del state.flow_aliases[alias]
    return registration


def get_flow_registration(name: str) -> FlowRegistration:
    _ensure_builtin_flow_domains()
    state = get_runtime_state()
    normalized = _normalize_flow_name(name)
    canonical = state.flow_aliases.get(normalized)
    if canonical is None or canonical not in state.flows:
        raise KeyError(f"unknown flow: {normalized}")
    return state.flows[canonical]


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
    _ensure_builtin_flow_domains()
    return sorted(get_runtime_state().flows.keys())


def list_flow_details() -> list[dict[str, Any]]:
    _ensure_builtin_flow_domains()
    state = get_runtime_state()
    return [
        {
            "name": registration.name,
            "aliases": list(registration.aliases),
            "module_path": registration.module_path,
            "attr_name": registration.attr_name,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in sorted(state.flows.values(), key=lambda item: item.name)
    ]


def list_flow_domains() -> list[dict[str, Any]]:
    _ensure_builtin_flow_domains()
    state = get_runtime_state()
    domains: Dict[str, Dict[str, Any]] = {}
    for registration in sorted(state.flows.values(), key=lambda item: item.name):
        if registration.domain is None:
            continue
        entry = domains.get(registration.domain)
        if entry is None:
            entry = dict(registration.domain_metadata)
            entry["name"] = registration.domain
            entry["flow_names"] = []
            domains[registration.domain] = entry
        else:
            for key, value in registration.domain_metadata.items():
                entry.setdefault(key, value)
        entry["flow_names"].append(registration.name)
    return [domains[name] for name in sorted(domains.keys())]

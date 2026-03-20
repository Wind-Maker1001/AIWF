from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, TypeVar

from aiwf.registry_domains import summarize_registry_domains
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy

RegistrationT = TypeVar("RegistrationT")


def normalize_preprocess_op(op: str) -> str:
    normalized = str(op or "").strip().lower()
    if not normalized:
        raise ValueError("preprocess op must be non-empty")
    return normalized


def resolve_registration_conflict(
    *,
    registry: str,
    item_label: str,
    normalized: str,
    existing: Optional[Any],
    source: str,
    on_conflict: Optional[str],
) -> Optional[Any]:
    if existing is None:
        return None
    policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
    if policy == "error":
        record_registry_event(
            registry=registry,
            name=normalized,
            action="error",
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="registration already exists",
        )
        raise RuntimeError(f"{item_label} {normalized} already registered by {existing.source_module}")
    if policy == "keep":
        record_registry_event(
            registry=registry,
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
        registry=registry,
        name=normalized,
        action=action,
        policy=policy,
        existing_source_module=existing.source_module,
        new_source_module=source,
        detail="replaced existing registration",
    )
    return None


def unregister_named(mapping: Mapping[str, RegistrationT], name: str) -> Optional[RegistrationT]:
    normalized = normalize_preprocess_op(name)
    return mapping.pop(normalized, None)  # type: ignore[attr-defined]


def get_named(mapping: Mapping[str, RegistrationT], name: str, kind: str) -> RegistrationT:
    normalized = normalize_preprocess_op(name)
    registration = mapping.get(normalized)
    if registration is None:
        raise KeyError(f"unknown {kind}: {normalized}")
    return registration


def list_names(mapping: Mapping[str, Any]) -> List[str]:
    return sorted(mapping.keys())


def list_details(
    items: Iterable[RegistrationT],
    *,
    key: Callable[[RegistrationT], Any],
    serializer: Callable[[RegistrationT], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [serializer(item) for item in sorted(items, key=key)]


def list_domains(
    items: Iterable[RegistrationT],
    *,
    key: Callable[[RegistrationT], Any],
    item_name_attr: str,
    list_key: str,
) -> List[Dict[str, Any]]:
    return summarize_registry_domains(
        sorted(items, key=key),
        item_name_attr=item_name_attr,
        list_key=list_key,
    )

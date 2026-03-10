from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


_REGISTRY_EVENTS: List[Dict[str, Any]] = []
_MAX_EVENTS = 200


def record_registry_event(
    *,
    registry: str,
    name: str,
    action: str,
    policy: str,
    existing_source_module: str,
    new_source_module: str,
    detail: str | None = None,
) -> Dict[str, Any]:
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "registry": str(registry),
        "name": str(name),
        "action": str(action),
        "policy": str(policy),
        "existing_source_module": str(existing_source_module),
        "new_source_module": str(new_source_module),
        "detail": (str(detail) if detail else None),
    }
    _REGISTRY_EVENTS.append(event)
    if len(_REGISTRY_EVENTS) > _MAX_EVENTS:
        del _REGISTRY_EVENTS[:-_MAX_EVENTS]
    return event


def list_registry_events(limit: int | None = None) -> List[Dict[str, Any]]:
    if limit is None or limit <= 0:
        return list(_REGISTRY_EVENTS)
    return list(_REGISTRY_EVENTS[-limit:])


def clear_registry_events() -> None:
    _REGISTRY_EVENTS.clear()

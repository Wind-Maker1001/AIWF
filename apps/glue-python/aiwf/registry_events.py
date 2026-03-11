from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from aiwf.runtime_state import get_runtime_state

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
    state = get_runtime_state()
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
    state.registry_events.append(event)
    if len(state.registry_events) > _MAX_EVENTS:
        del state.registry_events[:-_MAX_EVENTS]
    return event


def list_registry_events(limit: int | None = None) -> List[Dict[str, Any]]:
    state = get_runtime_state()
    if limit is None or limit <= 0:
        return list(state.registry_events)
    return list(state.registry_events[-limit:])


def clear_registry_events() -> None:
    get_runtime_state().registry_events.clear()

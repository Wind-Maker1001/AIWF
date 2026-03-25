from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List

from aiwf.paths import resolve_bus_root


WORKFLOW_SANDBOX_AUTOFIX_SCHEMA_VERSION = "workflow_sandbox_autofix_state.v1"
WORKFLOW_SANDBOX_AUTOFIX_OWNER = "glue-python"
WORKFLOW_SANDBOX_AUTOFIX_SOURCE = "glue-python.governance.workflow_sandbox_autofix"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def workflow_sandbox_autofix_store_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_SANDBOX_AUTOFIX_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_sandbox_autofix_state.v1.json")


def normalize_workflow_sandbox_autofix_state(value: Dict[str, Any] | None) -> Dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    return {
        "schema_version": WORKFLOW_SANDBOX_AUTOFIX_SCHEMA_VERSION,
        "owner": WORKFLOW_SANDBOX_AUTOFIX_OWNER,
        "source_of_truth": WORKFLOW_SANDBOX_AUTOFIX_SOURCE,
        "violation_events": deepcopy(source.get("violation_events") if isinstance(source.get("violation_events"), list) else []),
        "forced_isolation_mode": str(source.get("forced_isolation_mode") or ""),
        "forced_until": str(source.get("forced_until") or ""),
        "last_actions": deepcopy(source.get("last_actions") if isinstance(source.get("last_actions"), list) else []),
        "green_streak": max(0, int(source.get("green_streak") or 0)),
        "updated_at": str(source.get("updated_at") or now_iso()),
    }


def get_workflow_sandbox_autofix_state() -> Dict[str, Any]:
    file_path = workflow_sandbox_autofix_store_path()
    if not os.path.exists(file_path):
        return normalize_workflow_sandbox_autofix_state({})
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return normalize_workflow_sandbox_autofix_state(payload if isinstance(payload, dict) else {})


def save_workflow_sandbox_autofix_state(state: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_workflow_sandbox_autofix_state(state)
    file_path = workflow_sandbox_autofix_store_path()
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return normalized


def list_workflow_sandbox_autofix_actions(limit: int = 120) -> List[Dict[str, Any]]:
    state = get_workflow_sandbox_autofix_state()
    actions = state.get("last_actions") if isinstance(state.get("last_actions"), list) else []
    return deepcopy(list(reversed(actions[-max(1, min(1000, int(limit or 120))):])))

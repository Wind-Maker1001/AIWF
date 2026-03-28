from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root
from aiwf.governance_workflow_versions import (
    get_workflow_version,
)


WORKFLOW_APP_SCHEMA_VERSION = "workflow_app_registry_entry.v1"
WORKFLOW_APP_STORE_SCHEMA_VERSION = "workflow_app_registry_store.v1"
WORKFLOW_APP_OWNER = "glue-python"
WORKFLOW_APP_SOURCE = "glue-python.governance.workflow_apps"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def workflow_app_store_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_APP_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_apps.v1.json")


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def validate_workflow_app_id(value: str) -> str:
    app_id = str(value or "").strip()
    if not app_id:
        raise ValueError("workflow app id is required")
    if len(app_id) > 160:
        raise ValueError("workflow app id must be 160 characters or fewer")
    return app_id


def validate_published_version_id(value: str) -> str:
    version_id = str(value or "").strip()
    if not version_id:
        raise ValueError("workflow app published_version_id is required")
    if len(version_id) > 160:
        raise ValueError("workflow app published_version_id must be 160 characters or fewer")
    return version_id


def normalize_workflow_app_payload(
    payload: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    app_id = validate_workflow_app_id(source.get("app_id") or current.get("app_id") or "")
    published_version_id = validate_published_version_id(
        source.get("published_version_id") or current.get("published_version_id") or ""
    )
    version_item = get_workflow_version(published_version_id)
    if version_item is None:
        raise ValueError(f"workflow app published_version_id not found: {published_version_id}")
    workflow_id = str(
        source.get("workflow_id")
        or current.get("workflow_id")
        or version_item.get("workflow_id")
        or ""
    ).strip()
    if not workflow_id:
        raise ValueError("workflow app workflow_id is required")
    name = str(
        source.get("name")
        or current.get("name")
        or version_item.get("workflow_name")
        or app_id
    ).strip() or app_id
    params_schema = source.get("params_schema") if source.get("params_schema") is not None else current.get("params_schema")
    template_policy = source.get("template_policy") if source.get("template_policy") is not None else current.get("template_policy")
    if params_schema is None:
        params_schema = {}
    if template_policy is None:
        template_policy = {}
    if not isinstance(params_schema, dict):
        raise ValueError("workflow app params_schema must be an object")
    if not isinstance(template_policy, dict):
        raise ValueError("workflow app template_policy must be an object")
    created_at = str(source.get("created_at") or current.get("created_at") or now_iso())
    updated_at = str(source.get("updated_at") or now_iso())
    return {
        "schema_version": WORKFLOW_APP_SCHEMA_VERSION,
        "owner": WORKFLOW_APP_OWNER,
        "source_of_truth": WORKFLOW_APP_SOURCE,
        "app_id": app_id,
        "name": name,
        "workflow_id": workflow_id,
        "published_version_id": published_version_id,
        "params_schema": _clone(params_schema),
        "template_policy": _clone(template_policy),
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _read_store() -> Dict[str, Any]:
    file_path = workflow_app_store_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": WORKFLOW_APP_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "items": [],
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    items = payload.get("items")
    return {
        "schema_version": str(payload.get("schema_version") or WORKFLOW_APP_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "items": items if isinstance(items, list) else [],
    }


def _write_store(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    file_path = workflow_app_store_path()
    payload = {
        "schema_version": WORKFLOW_APP_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "items": items,
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def list_workflow_apps(limit: int = 200) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(5000, int(limit or 200)))
    items = []
    for raw in _read_store()["items"]:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(normalize_workflow_app_payload(raw, existing=raw))
        except ValueError:
            continue
    items.sort(key=lambda item: (str(item.get("updated_at") or ""), str(item.get("app_id") or "")), reverse=True)
    return items[:safe_limit]


def get_workflow_app(app_id: str) -> Optional[Dict[str, Any]]:
    normalized_id = validate_workflow_app_id(app_id)
    for item in list_workflow_apps(5000):
        if str(item.get("app_id") or "") == normalized_id:
            return item
    return None


def save_workflow_app(payload: Dict[str, Any]) -> Dict[str, Any]:
    current_items = list_workflow_apps(5000)
    desired_id = validate_workflow_app_id(payload.get("app_id") or "")
    existing = None
    for item in current_items:
        if str(item.get("app_id") or "") == desired_id:
            existing = item
            break
    normalized = normalize_workflow_app_payload(payload, existing=existing)
    next_items = [item for item in current_items if str(item.get("app_id") or "") != desired_id]
    next_items.insert(0, normalized)
    _write_store(next_items)
    return normalized

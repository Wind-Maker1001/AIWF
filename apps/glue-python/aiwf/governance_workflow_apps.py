from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.node_config_contract_runtime import (
    find_unknown_workflow_node_types,
)
from aiwf.paths import resolve_bus_root


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


def validate_workflow_graph(graph: Any) -> Dict[str, Any]:
    if not isinstance(graph, dict):
        raise ValueError("workflow app graph must be an object")
    workflow_id = str(graph.get("workflow_id") or "").strip()
    version = str(graph.get("version") or "").strip()
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not workflow_id:
        raise ValueError("workflow app graph requires workflow_id")
    if not version:
        raise ValueError("workflow app graph requires version")
    if not isinstance(nodes, list):
        raise ValueError("workflow app graph requires nodes array")
    if not isinstance(edges, list):
        raise ValueError("workflow app graph requires edges array")
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"workflow app graph nodes[{index}] must be an object")
        if not str(node.get("id") or "").strip():
            raise ValueError(f"workflow app graph nodes[{index}] requires id")
        if not str(node.get("type") or "").strip():
            raise ValueError(f"workflow app graph nodes[{index}] requires type")
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            raise ValueError(f"workflow app graph edges[{index}] must be an object")
        if not str(edge.get("from") or "").strip():
            raise ValueError(f"workflow app graph edges[{index}] requires from")
        if not str(edge.get("to") or "").strip():
            raise ValueError(f"workflow app graph edges[{index}] requires to")
    unknown_node_types = find_unknown_workflow_node_types(graph)
    if unknown_node_types:
        raise ValueError("workflow app graph contains unregistered node types: " + ", ".join(unknown_node_types))
    return _clone(graph)


def normalize_workflow_app_payload(
    payload: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    app_id = validate_workflow_app_id(source.get("app_id") or current.get("app_id") or "")
    graph = validate_workflow_graph(source.get("graph") if source.get("graph") is not None else current.get("graph"))
    name = str(source.get("name") or current.get("name") or graph.get("name") or app_id).strip() or app_id
    workflow_id = str(source.get("workflow_id") or current.get("workflow_id") or graph.get("workflow_id") or "").strip()
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
        "graph": graph,
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

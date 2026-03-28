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


WORKFLOW_VERSION_SCHEMA_VERSION = "workflow_version_snapshot.v1"
WORKFLOW_VERSION_STORE_SCHEMA_VERSION = "workflow_version_store.v1"
WORKFLOW_VERSION_OWNER = "glue-python"
WORKFLOW_VERSION_SOURCE = "glue-python.governance.workflow_versions"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def workflow_version_store_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_VERSION_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_versions.v1.json")


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def validate_workflow_graph(graph: Any) -> Dict[str, Any]:
    if not isinstance(graph, dict):
        raise ValueError("workflow version graph must be an object")
    workflow_id = str(graph.get("workflow_id") or "").strip()
    version = str(graph.get("version") or "").strip()
    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not workflow_id:
        raise ValueError("workflow version graph requires workflow_id")
    if not version:
        raise ValueError("workflow version graph requires version")
    if not isinstance(nodes, list):
        raise ValueError("workflow version graph requires nodes array")
    if not isinstance(edges, list):
        raise ValueError("workflow version graph requires edges array")
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise ValueError(f"workflow version graph nodes[{index}] must be an object")
        if not str(node.get("id") or "").strip():
            raise ValueError(f"workflow version graph nodes[{index}] requires id")
        if not str(node.get("type") or "").strip():
            raise ValueError(f"workflow version graph nodes[{index}] requires type")
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            raise ValueError(f"workflow version graph edges[{index}] must be an object")
        if not str(edge.get("from") or "").strip():
            raise ValueError(f"workflow version graph edges[{index}] requires from")
        if not str(edge.get("to") or "").strip():
            raise ValueError(f"workflow version graph edges[{index}] requires to")
    unknown_node_types = find_unknown_workflow_node_types(graph)
    if unknown_node_types:
        raise ValueError("workflow version graph contains unregistered node types: " + ", ".join(unknown_node_types))
    return _clone(graph)


def validate_version_id(value: str) -> str:
    version_id = str(value or "").strip()
    if not version_id:
        raise ValueError("version_id is required")
    if len(version_id) > 160:
        raise ValueError("version_id must be 160 characters or fewer")
    return version_id


def normalize_workflow_version_payload(
    payload: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    version_id = validate_version_id(source.get("version_id") or current.get("version_id") or "")
    graph = validate_workflow_graph(source.get("graph") if source.get("graph") is not None else current.get("graph"))
    workflow_id = str(source.get("workflow_id") or current.get("workflow_id") or graph.get("workflow_id") or "").strip()
    workflow_name = str(source.get("workflow_name") or current.get("workflow_name") or graph.get("name") or workflow_id).strip() or workflow_id
    ts = str(source.get("ts") or current.get("ts") or now_iso())
    return {
        "schema_version": WORKFLOW_VERSION_SCHEMA_VERSION,
        "owner": WORKFLOW_VERSION_OWNER,
        "source_of_truth": WORKFLOW_VERSION_SOURCE,
        "version_id": version_id,
        "ts": ts,
        "workflow_id": workflow_id,
        "workflow_name": workflow_name,
        "graph": graph,
    }


def _read_store() -> Dict[str, Any]:
    file_path = workflow_version_store_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "items": [],
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    items = payload.get("items")
    return {
        "schema_version": str(payload.get("schema_version") or WORKFLOW_VERSION_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "items": items if isinstance(items, list) else [],
    }


def _write_store(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    file_path = workflow_version_store_path()
    payload = {
        "schema_version": WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "items": items,
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def list_workflow_versions(limit: int = 200, workflow_name: str = "") -> List[Dict[str, Any]]:
    safe_limit = max(1, min(5000, int(limit or 200)))
    items: List[Dict[str, Any]] = []
    for raw in _read_store()["items"]:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(normalize_workflow_version_payload(raw, existing=raw))
        except ValueError:
            continue
    key = str(workflow_name or "").strip()
    if key:
        items = [item for item in items if str(item.get("workflow_name") or "") == key]
    items.sort(key=lambda item: (str(item.get("ts") or ""), str(item.get("version_id") or "")), reverse=True)
    return items[:safe_limit]


def get_workflow_version(version_id: str) -> Optional[Dict[str, Any]]:
    target = validate_version_id(version_id)
    for item in list_workflow_versions(5000):
        if str(item.get("version_id") or "") == target:
            return item
    return None


def save_workflow_version(payload: Dict[str, Any]) -> Dict[str, Any]:
    current_items = list_workflow_versions(5000)
    desired_id = validate_version_id(payload.get("version_id") or "")
    existing = None
    for item in current_items:
        if str(item.get("version_id") or "") == desired_id:
            existing = item
            break
    normalized = normalize_workflow_version_payload(payload, existing=existing)
    next_items = [item for item in current_items if str(item.get("version_id") or "") != desired_id]
    next_items.insert(0, normalized)
    _write_store(next_items)
    return normalized


def compare_workflow_versions(version_a: str, version_b: str) -> Dict[str, Any]:
    a = get_workflow_version(version_a)
    b = get_workflow_version(version_b)
    if not a or not b:
        raise ValueError("version not found")
    graph_a = a.get("graph") if isinstance(a.get("graph"), dict) else {}
    graph_b = b.get("graph") if isinstance(b.get("graph"), dict) else {}
    nodes_a = graph_a.get("nodes") if isinstance(graph_a.get("nodes"), list) else []
    nodes_b = graph_b.get("nodes") if isinstance(graph_b.get("nodes"), list) else []
    edges_a = graph_a.get("edges") if isinstance(graph_a.get("edges"), list) else []
    edges_b = graph_b.get("edges") if isinstance(graph_b.get("edges"), list) else []

    map_a = {str(node.get("id") or ""): node for node in nodes_a if isinstance(node, dict)}
    map_b = {str(node.get("id") or ""): node for node in nodes_b if isinstance(node, dict)}
    all_node_ids = sorted(set(map_a.keys()) | set(map_b.keys()))
    node_diff = []
    for node_id in all_node_ids:
        node_a = map_a.get(node_id)
        node_b = map_b.get(node_id)
        if node_a is None:
            node_diff.append({"id": node_id, "change": "added", "type_a": "", "type_b": str(node_b.get("type") or "")})
            continue
        if node_b is None:
            node_diff.append({"id": node_id, "change": "removed", "type_a": str(node_a.get("type") or ""), "type_b": ""})
            continue
        type_changed = str(node_a.get("type") or "") != str(node_b.get("type") or "")
        config_changed = json.dumps(node_a.get("config") or {}, sort_keys=True, ensure_ascii=False) != json.dumps(node_b.get("config") or {}, sort_keys=True, ensure_ascii=False)
        node_diff.append({
            "id": node_id,
            "change": "updated" if (type_changed or config_changed) else "same",
            "type_a": str(node_a.get("type") or ""),
            "type_b": str(node_b.get("type") or ""),
            "type_changed": type_changed,
            "config_changed": config_changed,
        })

    def edge_key(edge: Any) -> str:
        item = edge if isinstance(edge, dict) else {}
        return f"{str(item.get('from') or '')}->{str(item.get('to') or '')}:{json.dumps(item.get('when', None), sort_keys=True, ensure_ascii=False)}"

    set_a = {edge_key(edge) for edge in edges_a}
    set_b = {edge_key(edge) for edge in edges_b}
    added_edges = [item for item in sorted(set_b) if item not in set_a]
    removed_edges = [item for item in sorted(set_a) if item not in set_b]
    changed_nodes = len([item for item in node_diff if str(item.get("change") or "") != "same"])

    return {
        "ok": True,
        "summary": {
            "version_a": str(version_a or ""),
            "version_b": str(version_b or ""),
            "nodes_a": len(nodes_a),
            "nodes_b": len(nodes_b),
            "edges_a": len(edges_a),
            "edges_b": len(edges_b),
            "changed_nodes": changed_nodes,
            "added_edges": len(added_edges),
            "removed_edges": len(removed_edges),
        },
        "node_diff": deepcopy(node_diff),
        "added_edges": deepcopy(added_edges),
        "removed_edges": deepcopy(removed_edges),
    }

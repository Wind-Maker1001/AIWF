from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root


WORKFLOW_RUN_AUDIT_SCHEMA_VERSION = "workflow_run_audit_entry.v1"
WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION = "workflow_audit_event.v1"
WORKFLOW_RUN_AUDIT_OWNER = "glue-python"
WORKFLOW_RUN_AUDIT_SOURCE = "glue-python.governance.workflow_run_audit"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def workflow_run_store_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_RUN_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_runs.v1.jsonl")


def workflow_audit_event_store_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_AUDIT_EVENT_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_audit_events.v1.jsonl")


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    items: List[Dict[str, Any]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            items.append(payload)
    return items


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")


def normalize_workflow_run_item(
    item: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    run_id = str(source.get("run_id") or current.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("workflow run_id is required")
    result = source.get("result") if source.get("result") is not None else current.get("result")
    payload = source.get("payload") if source.get("payload") is not None else current.get("payload")
    config = source.get("config") if source.get("config") is not None else current.get("config")
    if result is None:
        result = {}
    if payload is None:
        payload = {}
    if config is None:
        config = {}
    if not isinstance(result, dict):
        raise ValueError("workflow run result must be an object")
    if not isinstance(payload, dict):
        raise ValueError("workflow run payload must be an object")
    if not isinstance(config, dict):
        raise ValueError("workflow run config must be an object")
    return {
        "schema_version": WORKFLOW_RUN_AUDIT_SCHEMA_VERSION,
        "owner": WORKFLOW_RUN_AUDIT_OWNER,
        "source_of_truth": WORKFLOW_RUN_AUDIT_SOURCE,
        "ts": str(source.get("ts") or current.get("ts") or now_iso()),
        "run_id": run_id,
        "workflow_id": str(source.get("workflow_id") or current.get("workflow_id") or result.get("workflow_id") or "").strip(),
        "status": str(source.get("status") or current.get("status") or result.get("status") or "").strip(),
        "ok": bool(source.get("ok") if "ok" in source else current.get("ok", result.get("ok", False))),
        "payload": _clone(payload),
        "config": _clone(config),
        "result": _clone(result),
    }


def record_workflow_run(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_workflow_run_item(item, existing=item)
    _append_jsonl(workflow_run_store_path(), normalized)
    return normalized


def list_workflow_runs(limit: int = 200) -> List[Dict[str, Any]]:
    items = _read_jsonl(workflow_run_store_path())
    normalized: List[Dict[str, Any]] = []
    for item in reversed(items):
        try:
            normalized.append(normalize_workflow_run_item(item, existing=item))
        except ValueError:
            continue
        if len(normalized) >= max(1, min(5000, int(limit or 200))):
            break
    return normalized


def get_workflow_run(run_id: str) -> Optional[Dict[str, Any]]:
    target = str(run_id or "").strip()
    if not target:
        raise ValueError("workflow run_id is required")
    for item in list_workflow_runs(5000):
        if str(item.get("run_id") or "") == target:
            return item
    return None


def run_timeline(run_id: str) -> Dict[str, Any]:
    found = get_workflow_run(run_id)
    if not found:
        raise ValueError("run not found")
    rows = found.get("result", {}).get("node_runs")
    node_runs = rows if isinstance(rows, list) else []
    timeline = [
        {
            "node_id": str(node.get("id") or ""),
            "type": str(node.get("type") or ""),
            "status": str(node.get("status") or ""),
            "started_at": str(node.get("started_at") or ""),
            "ended_at": str(node.get("ended_at") or ""),
            "seconds": float(node.get("seconds") or 0),
        }
        for node in node_runs
        if isinstance(node, dict)
    ]
    timeline.sort(key=lambda item: str(item.get("started_at") or ""))
    return {
        "ok": True,
        "run_id": str(run_id or ""),
        "status": str(found.get("result", {}).get("status") or found.get("status") or ""),
        "timeline": timeline,
    }


def failure_summary(limit: int = 400) -> Dict[str, Any]:
    runs = list_workflow_runs(limit)
    failed_runs = [item for item in runs if not bool(item.get("result", {}).get("ok", item.get("ok", False)))]
    by_node: Dict[str, Dict[str, Any]] = {}
    for run in failed_runs:
        node_runs = run.get("result", {}).get("node_runs")
        rows = node_runs if isinstance(node_runs, list) else []
        for node in rows:
            if not isinstance(node, dict):
                continue
            if str(node.get("status") or "") != "failed":
                continue
            key = str(node.get("type") or "unknown")
            if key not in by_node:
                by_node[key] = {"failed": 0, "samples": []}
            by_node[key]["failed"] += 1
            if len(by_node[key]["samples"]) < 3:
                by_node[key]["samples"].append(str(node.get("error") or "")[:200])
    return {
        "ok": True,
        "total_runs": len(runs),
        "failed_runs": len(failed_runs),
        "by_node": by_node,
    }


def normalize_workflow_audit_event(
    item: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    action = str(source.get("action") or current.get("action") or "").strip()
    if not action:
        raise ValueError("workflow audit action is required")
    detail = source.get("detail") if source.get("detail") is not None else current.get("detail")
    if detail is None:
        detail = {}
    if not isinstance(detail, dict):
        raise ValueError("workflow audit detail must be an object")
    return {
        "schema_version": WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION,
        "owner": WORKFLOW_RUN_AUDIT_OWNER,
        "source_of_truth": WORKFLOW_RUN_AUDIT_SOURCE,
        "ts": str(source.get("ts") or current.get("ts") or now_iso()),
        "action": action,
        "detail": _clone(detail),
    }


def record_workflow_audit_event(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_workflow_audit_event(item, existing=item)
    _append_jsonl(workflow_audit_event_store_path(), normalized)
    return normalized


def list_workflow_audit_events(limit: int = 200, action: str = "") -> List[Dict[str, Any]]:
    items = _read_jsonl(workflow_audit_event_store_path())
    target_action = str(action or "").strip()
    normalized: List[Dict[str, Any]] = []
    for item in reversed(items):
        try:
            normalized_item = normalize_workflow_audit_event(item, existing=item)
        except ValueError:
            continue
        if target_action and str(normalized_item.get("action") or "") != target_action:
            continue
        normalized.append(normalized_item)
        if len(normalized) >= max(1, min(5000, int(limit or 200))):
            break
    return normalized

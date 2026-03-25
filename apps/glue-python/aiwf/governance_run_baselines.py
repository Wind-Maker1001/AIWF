from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root


RUN_BASELINE_SCHEMA_VERSION = "run_baseline_entry.v1"
RUN_BASELINE_STORE_SCHEMA_VERSION = "run_baseline_store.v1"
RUN_BASELINE_OWNER = "glue-python"
RUN_BASELINE_SOURCE = "glue-python.governance.run_baselines"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def run_baseline_store_path() -> str:
    configured = str(os.getenv("AIWF_RUN_BASELINE_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_run_baselines.v1.json")


def normalize_run_baseline_payload(
    payload: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    baseline_id = str(source.get("baseline_id") or current.get("baseline_id") or "").strip()
    run_id = str(source.get("run_id") or current.get("run_id") or "").strip()
    if not baseline_id:
        raise ValueError("baseline_id is required")
    if not run_id:
        raise ValueError("run_id is required")
    return {
        "schema_version": RUN_BASELINE_SCHEMA_VERSION,
        "owner": RUN_BASELINE_OWNER,
        "source_of_truth": RUN_BASELINE_SOURCE,
        "baseline_id": baseline_id,
        "name": str(source.get("name") or current.get("name") or run_id).strip() or run_id,
        "run_id": run_id,
        "workflow_id": str(source.get("workflow_id") or current.get("workflow_id") or "").strip(),
        "created_at": str(source.get("created_at") or current.get("created_at") or now_iso()),
        "notes": str(source.get("notes") or current.get("notes") or "").strip(),
    }


def _read_store() -> Dict[str, Any]:
    file_path = run_baseline_store_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": RUN_BASELINE_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "items": [],
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    items = payload.get("items")
    return {
        "schema_version": str(payload.get("schema_version") or RUN_BASELINE_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "items": items if isinstance(items, list) else [],
    }


def _write_store(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    file_path = run_baseline_store_path()
    payload = {
        "schema_version": RUN_BASELINE_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "items": items,
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def list_run_baselines(limit: int = 200) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for raw in _read_store()["items"]:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(normalize_run_baseline_payload(raw, existing=raw))
        except ValueError:
            continue
    items.sort(key=lambda item: (str(item.get("created_at") or ""), str(item.get("baseline_id") or "")), reverse=True)
    return items[:max(1, min(5000, int(limit or 200)))]


def get_run_baseline(baseline_id: str) -> Optional[Dict[str, Any]]:
    target = str(baseline_id or "").strip()
    if not target:
        raise ValueError("baseline_id is required")
    for item in list_run_baselines(5000):
        if str(item.get("baseline_id") or "") == target:
            return item
    return None


def save_run_baseline(payload: Dict[str, Any]) -> Dict[str, Any]:
    current_items = list_run_baselines(5000)
    desired_id = str(payload.get("baseline_id") or "").strip()
    if not desired_id:
        raise ValueError("baseline_id is required")
    existing = None
    for item in current_items:
        if str(item.get("baseline_id") or "") == desired_id:
            existing = item
            break
    normalized = normalize_run_baseline_payload(payload, existing=existing)
    next_items = [item for item in current_items if str(item.get("baseline_id") or "") != desired_id]
    next_items.insert(0, normalized)
    _write_store(next_items)
    return normalized

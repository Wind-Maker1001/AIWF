from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root


MANUAL_REVIEW_SCHEMA_VERSION = "manual_review_item.v1"
MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION = "manual_review_queue_store.v1"
MANUAL_REVIEW_HISTORY_STORE_SCHEMA_VERSION = "manual_review_history_store.v1"
MANUAL_REVIEW_OWNER = "glue-python"
MANUAL_REVIEW_SOURCE = "glue-python.governance.manual_reviews"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def manual_review_queue_store_path() -> str:
    configured = str(os.getenv("AIWF_MANUAL_REVIEW_QUEUE_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "manual_review_queue.v1.json")


def manual_review_history_store_path() -> str:
    configured = str(os.getenv("AIWF_MANUAL_REVIEW_HISTORY_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "manual_review_history.v1.jsonl")


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def normalize_manual_review_item(
    item: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    run_id = str(source.get("run_id") or current.get("run_id") or "").strip()
    review_key = str(source.get("review_key") or source.get("node_id") or current.get("review_key") or current.get("node_id") or "").strip()
    if not run_id:
        raise ValueError("manual review run_id is required")
    if not review_key:
        raise ValueError("manual review review_key is required")
    status = str(source.get("status") or current.get("status") or "pending").strip().lower() or "pending"
    return {
        "schema_version": MANUAL_REVIEW_SCHEMA_VERSION,
        "owner": MANUAL_REVIEW_OWNER,
        "source_of_truth": MANUAL_REVIEW_SOURCE,
        "run_id": run_id,
        "review_key": review_key,
        "workflow_id": str(source.get("workflow_id") or current.get("workflow_id") or "").strip(),
        "node_id": str(source.get("node_id") or current.get("node_id") or review_key).strip(),
        "reviewer": str(source.get("reviewer") or current.get("reviewer") or "").strip(),
        "comment": str(source.get("comment") or current.get("comment") or "").strip(),
        "created_at": str(source.get("created_at") or current.get("created_at") or now_iso()),
        "decided_at": str(source.get("decided_at") or current.get("decided_at") or ""),
        "status": status,
        "approved": bool(source.get("approved") if "approved" in source else current.get("approved", False)),
    }


def _read_queue_store() -> Dict[str, Any]:
    file_path = manual_review_queue_store_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "items": [],
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    items = payload.get("items")
    return {
        "schema_version": str(payload.get("schema_version") or MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "items": items if isinstance(items, list) else [],
    }


def _write_queue_store(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    file_path = manual_review_queue_store_path()
    payload = {
        "schema_version": MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "items": items,
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def list_manual_reviews(limit: int = 200) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(5000, int(limit or 200)))
    items: List[Dict[str, Any]] = []
    for raw in _read_queue_store()["items"]:
        if not isinstance(raw, dict):
            continue
        try:
            normalized = normalize_manual_review_item(raw, existing=raw)
        except ValueError:
            continue
        if str(normalized.get("status") or "") != "pending":
            continue
        items.append(normalized)
    items.sort(key=lambda item: (str(item.get("created_at") or ""), str(item.get("run_id") or "")), reverse=True)
    return items[:safe_limit]


def _append_manual_review_history(item: Dict[str, Any]) -> None:
    file_path = manual_review_history_store_path()
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, ensure_ascii=False))
        handle.write("\n")


def list_manual_review_history(limit: int = 200) -> List[Dict[str, Any]]:
    file_path = manual_review_history_store_path()
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    items: List[Dict[str, Any]] = []
    for line in reversed(lines):
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue
        try:
            normalized = normalize_manual_review_item(raw, existing=raw)
        except ValueError:
            continue
        items.append(normalized)
        if len(items) >= max(1, min(5000, int(limit or 200))):
            break
    return items


def filter_manual_review_history(items: List[Dict[str, Any]], filter_obj: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    filter_src = filter_obj if isinstance(filter_obj, dict) else {}
    run_id = str(filter_src.get("run_id") or "").strip()
    reviewer = str(filter_src.get("reviewer") or "").strip().lower()
    status = str(filter_src.get("status") or "").strip().lower()
    date_from = str(filter_src.get("date_from") or "").strip()
    date_to = str(filter_src.get("date_to") or "").strip()
    filtered = []
    for item in items or []:
        if run_id and str(item.get("run_id") or "") != run_id:
            continue
        if reviewer and reviewer not in str(item.get("reviewer") or "").lower():
            continue
        if status and str(item.get("status") or "").lower() != status:
            continue
        decided_at = str(item.get("decided_at") or "")
        if date_from and decided_at < date_from:
            continue
        if date_to and decided_at > date_to:
            continue
        filtered.append(item)
    return filtered


def enqueue_manual_reviews(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    incoming = items if isinstance(items, list) else []
    current = list_manual_reviews(5000)
    by_key: Dict[str, Dict[str, Any]] = {}
    for item in current:
        by_key[f"{item['run_id']}::{item['review_key']}"] = item
    for item in incoming:
        normalized = normalize_manual_review_item(item)
        normalized["status"] = "pending"
        by_key[f"{normalized['run_id']}::{normalized['review_key']}"] = normalized
    next_items = list(by_key.values())
    _write_queue_store(next_items)
    return next_items


def submit_manual_review(
    run_id: str,
    review_key: str,
    *,
    approved: bool,
    reviewer: str,
    comment: str,
) -> Dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_review_key = str(review_key or "").strip()
    if not normalized_run_id:
        raise ValueError("manual review run_id is required")
    if not normalized_review_key:
        raise ValueError("manual review review_key is required")

    queue = list_manual_reviews(5000)
    target = None
    remaining = []
    for item in queue:
        if str(item.get("run_id") or "") == normalized_run_id and str(item.get("review_key") or "") == normalized_review_key:
            target = item
            continue
        remaining.append(item)
    if target is None:
        raise ValueError("review task not found")
    _write_queue_store(remaining)
    history_item = normalize_manual_review_item(
        {
            **target,
            "approved": bool(approved),
            "reviewer": str(reviewer or "").strip(),
            "comment": str(comment or "").strip(),
            "status": "approved" if approved else "rejected",
            "decided_at": now_iso(),
        },
        existing=target,
    )
    _append_manual_review_history(history_item)
    return {
        "item": history_item,
        "remaining": len(remaining),
    }

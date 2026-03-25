from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root


QUALITY_RULE_SET_SCHEMA_VERSION = "quality_rule_set.v1"
QUALITY_RULE_SET_STORE_SCHEMA_VERSION = "quality_rule_set_store.v1"
QUALITY_RULE_SET_OWNER = "glue-python"
QUALITY_RULE_SET_SOURCE = "glue-python.governance.quality_rule_sets"

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def quality_rule_set_store_path() -> str:
    configured = str(os.getenv("AIWF_QUALITY_RULE_SET_STORE_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "quality_rule_sets.v1.json")


def validate_quality_rule_set_id(value: str) -> str:
    set_id = str(value or "").strip()
    if not set_id:
        raise ValueError("quality rule set id is required")
    if not _ID_RE.fullmatch(set_id):
        raise ValueError(
            "quality rule set id must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
        )
    return set_id


def _read_store() -> Dict[str, Any]:
    file_path = quality_rule_set_store_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "sets": [],
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    sets = payload.get("sets")
    return {
        "schema_version": str(payload.get("schema_version") or QUALITY_RULE_SET_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "sets": sets if isinstance(sets, list) else [],
    }


def _write_store(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    file_path = quality_rule_set_store_path()
    payload = {
        "schema_version": QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "sets": items,
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def normalize_quality_rule_set_payload(
    payload: Dict[str, Any],
    *,
    existing: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    current = existing if isinstance(existing, dict) else {}
    set_id = validate_quality_rule_set_id(source.get("id") or current.get("id") or "")
    rules = source.get("rules")
    if rules is None:
        rules = current.get("rules")
    if not isinstance(rules, dict):
        raise ValueError("quality rule set rules must be an object")

    created_at = str(source.get("created_at") or current.get("created_at") or now_iso())
    updated_at = str(source.get("updated_at") or now_iso())
    name = str(source.get("name") or current.get("name") or set_id).strip() or set_id
    version = str(source.get("version") or current.get("version") or "v1").strip() or "v1"
    scope = str(source.get("scope") or current.get("scope") or "workflow").strip() or "workflow"

    return {
        "schema_version": QUALITY_RULE_SET_SCHEMA_VERSION,
        "owner": QUALITY_RULE_SET_OWNER,
        "source_of_truth": QUALITY_RULE_SET_SOURCE,
        "id": set_id,
        "name": name,
        "version": version,
        "scope": scope,
        "rules": deepcopy(rules),
        "created_at": created_at,
        "updated_at": updated_at,
    }


def list_quality_rule_sets(limit: int = 500) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(5000, int(limit or 500)))
    items = []
    for raw in _read_store()["sets"]:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(normalize_quality_rule_set_payload(raw, existing=raw))
        except ValueError:
            continue
    items.sort(key=lambda item: (str(item.get("updated_at") or ""), str(item.get("id") or "")), reverse=True)
    return items[:safe_limit]


def get_quality_rule_set(set_id: str) -> Optional[Dict[str, Any]]:
    normalized_id = validate_quality_rule_set_id(set_id)
    for item in list_quality_rule_sets(5000):
        if str(item.get("id") or "") == normalized_id:
            return item
    return None


def save_quality_rule_set(payload: Dict[str, Any]) -> Dict[str, Any]:
    current_store = list_quality_rule_sets(5000)
    existing = None
    desired_id = validate_quality_rule_set_id(payload.get("id") or "")
    for item in current_store:
        if str(item.get("id") or "") == desired_id:
            existing = item
            break
    normalized = normalize_quality_rule_set_payload(payload, existing=existing)
    next_items = [item for item in current_store if str(item.get("id") or "") != desired_id]
    next_items.insert(0, normalized)
    _write_store(next_items)
    return normalized


def remove_quality_rule_set(set_id: str) -> bool:
    normalized_id = validate_quality_rule_set_id(set_id)
    current_store = list_quality_rule_sets(5000)
    next_items = [item for item in current_store if str(item.get("id") or "") != normalized_id]
    removed = len(next_items) != len(current_store)
    if removed:
        _write_store(next_items)
    return removed

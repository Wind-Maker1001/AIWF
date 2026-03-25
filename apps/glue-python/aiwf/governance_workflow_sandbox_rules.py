from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiwf.paths import resolve_bus_root


WORKFLOW_SANDBOX_RULE_SCHEMA_VERSION = "workflow_sandbox_alert_rules.v1"
WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION = "workflow_sandbox_alert_rule_store.v1"
WORKFLOW_SANDBOX_RULE_OWNER = "glue-python"
WORKFLOW_SANDBOX_RULE_SOURCE = "glue-python.governance.workflow_sandbox_rules"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_governance_root() -> str:
    configured = str(os.getenv("AIWF_GOVERNANCE_ROOT") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_bus_root(), "governance")


def workflow_sandbox_rules_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_SANDBOX_RULES_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_sandbox_alert_rules.v1.json")


def workflow_sandbox_rule_versions_path() -> str:
    configured = str(os.getenv("AIWF_WORKFLOW_SANDBOX_RULE_VERSIONS_PATH") or "").strip()
    if configured:
        return os.path.normpath(configured)
    return os.path.join(resolve_governance_root(), "workflow_sandbox_alert_rule_versions.v1.jsonl")


def _parse_iso_ts(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_workflow_sandbox_rules(rules: Dict[str, Any] | None) -> Dict[str, Any]:
    source = rules if isinstance(rules, dict) else {}

    def to_list(value: Any) -> List[str]:
        items = value if isinstance(value, list) else []
        normalized = []
        for item in items:
            text = str(item or "").strip().lower()
            if text:
                normalized.append(text)
        return sorted(set(normalized))

    mute_src = source.get("mute_until_by_key")
    mute_values = mute_src if isinstance(mute_src, dict) else {}
    mute_until_by_key: Dict[str, str] = {}
    for key, value in mute_values.items():
        normalized_key = str(key or "").strip().lower()
        normalized_ts = _parse_iso_ts(value)
        if not normalized_key or not normalized_ts:
            continue
        mute_until_by_key[normalized_key] = normalized_ts

    return {
        "schema_version": WORKFLOW_SANDBOX_RULE_SCHEMA_VERSION,
        "owner": WORKFLOW_SANDBOX_RULE_OWNER,
        "source_of_truth": WORKFLOW_SANDBOX_RULE_SOURCE,
        "whitelist_codes": to_list(source.get("whitelist_codes")),
        "whitelist_node_types": to_list(source.get("whitelist_node_types")),
        "whitelist_keys": to_list(source.get("whitelist_keys")),
        "mute_until_by_key": mute_until_by_key,
        "updated_at": now_iso(),
    }


def _read_rules_store() -> Dict[str, Any]:
    file_path = workflow_sandbox_rules_path()
    if not os.path.exists(file_path):
        return {
            "schema_version": WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
            "updated_at": None,
            "rules": normalize_workflow_sandbox_rules({}),
        }
    with open(file_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    rules = payload.get("rules")
    return {
        "schema_version": str(payload.get("schema_version") or WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION),
        "updated_at": payload.get("updated_at"),
        "rules": normalize_workflow_sandbox_rules(rules if isinstance(rules, dict) else {}),
    }


def _write_rules_store(rules: Dict[str, Any]) -> Dict[str, Any]:
    file_path = workflow_sandbox_rules_path()
    payload = {
        "schema_version": WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
        "updated_at": now_iso(),
        "rules": normalize_workflow_sandbox_rules(rules),
    }
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return payload


def get_workflow_sandbox_rules() -> Dict[str, Any]:
    return _read_rules_store()["rules"]


def append_workflow_sandbox_rule_version(rules: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    item = {
        "version_id": f"{int(datetime.now(timezone.utc).timestamp() * 1000)}_{os.urandom(4).hex()}",
        "ts": now_iso(),
        "rules": normalize_workflow_sandbox_rules(rules),
        "meta": meta if isinstance(meta, dict) else {},
    }
    file_path = workflow_sandbox_rule_versions_path()
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, ensure_ascii=False))
        handle.write("\n")
    return item


def set_workflow_sandbox_rules(rules: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    normalized = normalize_workflow_sandbox_rules(rules)
    _write_rules_store(normalized)
    version = append_workflow_sandbox_rule_version(
        normalized,
        meta if isinstance(meta, dict) and meta else {"reason": "set_rules"},
    )
    return {
        "rules": normalized,
        "version": version,
    }


def list_workflow_sandbox_rule_versions(limit: int = 200) -> List[Dict[str, Any]]:
    file_path = workflow_sandbox_rule_versions_path()
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle.readlines() if line.strip()]
    items: List[Dict[str, Any]] = []
    for line in reversed(lines):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        rules = payload.get("rules")
        items.append({
            "version_id": str(payload.get("version_id") or ""),
            "ts": str(payload.get("ts") or ""),
            "rules": normalize_workflow_sandbox_rules(rules if isinstance(rules, dict) else {}),
            "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
        })
        if len(items) >= max(1, min(5000, int(limit or 200))):
            break
    return items


def get_workflow_sandbox_rule_version(version_id: str) -> Optional[Dict[str, Any]]:
    target = str(version_id or "").strip()
    if not target:
        return None
    for item in list_workflow_sandbox_rule_versions(5000):
        if str(item.get("version_id") or "") == target:
            return item
    return None


def rollback_workflow_sandbox_rule_version(version_id: str) -> Optional[Dict[str, Any]]:
    hit = get_workflow_sandbox_rule_version(version_id)
    if hit is None:
        return None
    normalized = normalize_workflow_sandbox_rules(hit.get("rules"))
    _write_rules_store(normalized)
    version = append_workflow_sandbox_rule_version(
        normalized,
        {"reason": "rollback", "from_version_id": str(version_id or "")},
    )
    return {
        "rules": normalized,
        "version": version,
    }

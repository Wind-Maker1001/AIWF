from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List


NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY = "contracts/desktop/node_config_contracts.v1.json"
NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json"
RUST_OPERATOR_MANIFEST_DEFAULT_AUTHORITY = "contracts/rust/operators_manifest.v1.json"
WORKFLOW_RUNTIME_ONLY_NODE_TYPES = (
    "compute_rust",
    "md_output",
)
SUPPORTED_VALIDATOR_KINDS = (
    "aggregate_defs",
    "ai_providers",
    "array",
    "boolean",
    "computed_fields",
    "conditional_required_non_empty",
    "constraint_defs",
    "enum",
    "integer_min",
    "json_compatible",
    "json_object",
    "manifest_object",
    "object",
    "op_in_allowed_ops",
    "paired_required",
    "row_objects",
    "rules_object",
    "slot_bindings",
    "string",
    "string_array_non_empty",
    "string_non_empty",
    "window_functions",
    "workflow_steps",
)
VALIDATION_ERROR_CODES = (
    "array_min_items",
    "conditional_required",
    "empty_key",
    "enum_not_allowed",
    "json_not_compatible",
    "membership_required",
    "min_value",
    "missing_one_of",
    "paired_required",
    "required",
    "string_empty",
    "type_array",
    "type_boolean",
    "type_integer",
    "type_number",
    "type_object",
    "type_string",
    "undefined_not_allowed",
    "unknown_node_type",
    "unsupported_validator_kind",
    "validation_error",
)


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def _is_plain_object(value: Any) -> bool:
    return isinstance(value, dict)


def _resolve_repo_root() -> Path:
    configured = str(os.getenv("AIWF_REPO_ROOT") or "").strip()
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[3]


def resolve_node_config_contract_path() -> Path:
    configured = str(os.getenv("AIWF_NODE_CONFIG_CONTRACT_PATH") or "").strip()
    if configured:
        return Path(configured).resolve()
    return _resolve_repo_root() / "contracts" / "desktop" / "node_config_contracts.v1.json"


def resolve_rust_operator_manifest_path() -> Path:
    configured = str(os.getenv("AIWF_RUST_OPERATOR_MANIFEST_PATH") or "").strip()
    if configured:
        return Path(configured).resolve()
    return _resolve_repo_root() / "contracts" / "rust" / "operators_manifest.v1.json"


@lru_cache(maxsize=2)
def load_registered_workflow_node_types() -> List[str]:
    contract = load_node_config_contract_set()
    rust_manifest_path = resolve_rust_operator_manifest_path()
    rust_payload = json.loads(rust_manifest_path.read_text(encoding="utf-8"))
    local = [
        str(item or "").strip()
        for item in (contract.get("contract_types") or [])
        if str(item or "").strip()
    ]
    rust = [
        str(item.get("operator") or "").strip()
        for item in (
            rust_payload.get("operators")
            if isinstance(rust_payload.get("operators"), list)
            else []
        )
        if isinstance(item, dict)
        and bool(item.get("desktop_exposable"))
        and str(item.get("operator") or "").strip()
    ]
    return sorted(set(local + rust + list(WORKFLOW_RUNTIME_ONLY_NODE_TYPES)))


def find_unknown_workflow_node_types(graph: Any) -> List[str]:
    nodes = graph.get("nodes") if _is_plain_object(graph) else None
    if not isinstance(nodes, list):
        return []
    known = set(load_registered_workflow_node_types())
    unknown = {
        str(node.get("type") or "").strip()
        for node in nodes
        if _is_plain_object(node) and str(node.get("type") or "").strip() and str(node.get("type") or "").strip() not in known
    }
    return sorted(unknown)


@lru_cache(maxsize=4)
def _load_node_config_contract_set_cached(contract_path: str) -> Dict[str, Any]:
    path = Path(contract_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    nodes = payload.get("nodes") if isinstance(payload.get("nodes"), list) else []

    contract_by_type: Dict[str, Dict[str, Any]] = {}
    quality_by_type: Dict[str, str] = {}
    for item in nodes:
        if not isinstance(item, dict):
            continue
        node_type = str(item.get("type") or "").strip()
        if not node_type:
            continue
        quality = str(item.get("quality") or "").strip()
        validators = item.get("validators") if isinstance(item.get("validators"), list) else []
        contract_by_type[node_type] = {
            "type": node_type,
            "quality": quality,
            "validators": _clone(validators),
        }
        quality_by_type[node_type] = quality

    return {
        "schema_version": str(payload.get("schema_version") or "").strip(),
        "authority": str(payload.get("authority") or NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY).strip()
        or NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY,
        "owner": str(payload.get("owner") or "").strip(),
        "contract_path": str(path),
        "contract_by_type": contract_by_type,
        "contract_types": sorted(contract_by_type.keys()),
        "quality_by_type": quality_by_type,
    }


def load_node_config_contract_set() -> Dict[str, Any]:
    contract_path = str(resolve_node_config_contract_path())
    return _load_node_config_contract_set_cached(contract_path)


def build_node_config_contract_runtime_summary() -> Dict[str, Any]:
    contract = load_node_config_contract_set()
    rust_manifest_path = resolve_rust_operator_manifest_path()
    return {
        "schema_version": str(contract.get("schema_version") or "").strip(),
        "authority": str(contract.get("authority") or "").strip(),
        "contract_path": str(contract.get("contract_path") or "").strip(),
        "registered_workflow_node_type_authorities": [
            NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY,
            RUST_OPERATOR_MANIFEST_DEFAULT_AUTHORITY,
        ],
        "runtime_only_node_types": list(WORKFLOW_RUNTIME_ONLY_NODE_TYPES),
        "rust_operator_manifest_path": str(rust_manifest_path),
        "registered_workflow_node_types": load_registered_workflow_node_types(),
        "contract_types": list(contract.get("contract_types") or []),
        "quality_by_type": _clone(contract.get("quality_by_type") or {}),
        "supported_validator_kinds": list(SUPPORTED_VALIDATOR_KINDS),
        "validation_error_codes": list(VALIDATION_ERROR_CODES),
    }


__all__ = [
    "NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY",
    "NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY",
    "SUPPORTED_VALIDATOR_KINDS",
    "VALIDATION_ERROR_CODES",
    "build_node_config_contract_runtime_summary",
    "find_unknown_workflow_node_types",
    "load_node_config_contract_set",
    "load_registered_workflow_node_types",
    "resolve_node_config_contract_path",
    "resolve_rust_operator_manifest_path",
]

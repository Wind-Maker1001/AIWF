from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List


NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY = "contracts/desktop/node_config_contracts.v1.json"
NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json"
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


def resolve_workflow_node_catalog_contract_path() -> Path:
    configured = str(os.getenv("AIWF_WORKFLOW_NODE_CATALOG_CONTRACT_PATH") or "").strip()
    if configured:
        return Path(configured).resolve()
    return _resolve_repo_root() / "apps" / "dify-desktop" / "workflow_node_catalog_contract.js"


def resolve_desktop_rust_operator_manifest_path() -> Path:
    configured = str(os.getenv("AIWF_DESKTOP_RUST_OPERATOR_MANIFEST_PATH") or "").strip()
    if configured:
        return Path(configured).resolve()
    return _resolve_repo_root() / "apps" / "dify-desktop" / "workflow_chiplets" / "domains" / "rust_operator_manifest.generated.js"


def _extract_frozen_json_literal(file_path: Path, marker: str) -> Any:
    text = file_path.read_text(encoding="utf-8")
    marker_index = text.find(marker)
    if marker_index < 0:
        raise ValueError(f"marker not found in {file_path}: {marker}")
    index = marker_index + len(marker)
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text) or text[index] not in "[{":
        raise ValueError(f"expected JSON literal after marker in {file_path}: {marker}")
    opener = text[index]
    closer = "]" if opener == "[" else "}"
    depth = 0
    in_string = False
    escaped = False
    for pos in range(index, len(text)):
        char = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                literal = re.sub(r",(\s*[\]}])", r"\1", text[index : pos + 1])
                return json.loads(literal)
    raise ValueError(f"unterminated JSON literal in {file_path}: {marker}")


@lru_cache(maxsize=2)
def load_registered_workflow_node_types() -> List[str]:
    local_types = _extract_frozen_json_literal(
        resolve_workflow_node_catalog_contract_path(),
        "const LOCAL_WORKFLOW_NODE_TYPES = Object.freeze(",
    )
    rust_metadata = _extract_frozen_json_literal(
        resolve_desktop_rust_operator_manifest_path(),
        "const KNOWN_RUST_OPERATOR_METADATA = deepFreeze(",
    )
    local = [
        str(item or "").strip()
        for item in (local_types if isinstance(local_types, list) else [])
        if str(item or "").strip()
    ]
    rust = [
        str(operator or "").strip()
        for operator, item in (rust_metadata.items() if isinstance(rust_metadata, dict) else [])
        if isinstance(item, dict) and bool(item.get("desktop_exposable")) and str(operator or "").strip()
    ]
    return sorted(set(local + rust))


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
    return {
        "schema_version": str(contract.get("schema_version") or "").strip(),
        "authority": str(contract.get("authority") or "").strip(),
        "contract_path": str(contract.get("contract_path") or "").strip(),
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
]

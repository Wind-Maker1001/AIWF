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

_MISSING = object()


def _clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


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


def normalize_validation_error_item(message: str) -> Dict[str, str]:
    text = str(message or "").strip()
    if re.match(r"^workflow contains unregistered node types:", text):
        return {"path": "workflow.nodes", "code": "unknown_node_type", "message": text}
    path = text
    code = "validation_error"

    path_patterns = [
        r"^(.*?) keys must not be empty$",
        r"^(.*?) must match .*$",
        r"^(.*?) must be included in .* when both are provided$",
        r"^(.*?) is required when .*$",
        r"^(.*?) requires one of .*$",
        r"^(.*?) must contain at least one node$",
        r"^(.*?) must not be empty$",
        r"^(.*?) must be .*$",
        r"^(.*?) is required$",
    ]
    for pattern in path_patterns:
        match = re.match(pattern, text)
        if match:
            path = str(match.group(1) or "").strip() or text
            break

    if text.endswith(" must be a boolean"):
        code = "type_boolean"
    elif text.endswith(" must be a string"):
        code = "type_string"
    elif text.endswith(" must not be empty"):
        code = "string_empty"
    elif " must be one of: " in text:
        code = "enum_not_allowed"
    elif text.endswith(" must be an array"):
        code = "type_array"
    elif text.endswith(" must contain at least one node"):
        code = "array_min_items"
    elif text.endswith(" must be an object"):
        code = "type_object"
    elif text.endswith(" keys must not be empty"):
        code = "empty_key"
    elif text.endswith(" must be JSON-compatible"):
        code = "json_not_compatible"
    elif text.endswith(" must be an integer"):
        code = "type_integer"
    elif text.endswith(" must be a number"):
        code = "type_number"
    elif " must be >= " in text:
        code = "min_value"
    elif " requires one of " in text:
        code = "missing_one_of"
    elif " is required when " in text and text.endswith(" is provided"):
        code = "paired_required"
    elif " is required when " in text:
        code = "conditional_required"
    elif " must be included in " in text and text.endswith(" when both are provided"):
        code = "membership_required"
    elif " validator kind unsupported: " in text:
        code = "unsupported_validator_kind"
    elif text.endswith(" must not be undefined"):
        code = "undefined_not_allowed"
    elif text.endswith(" is required"):
        code = "required"

    return {"path": path, "code": code, "message": text}


def build_validation_error_items(errors: List[str]) -> List[Dict[str, str]]:
    return [normalize_validation_error_item(message) for message in errors if str(message or "").strip()]


def _is_plain_object(value: Any) -> bool:
    return isinstance(value, dict)


def _append(errors: List[str], message: str) -> None:
    if message not in errors:
        errors.append(message)


def _validate_optional_boolean(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, bool):
        _append(errors, f"{label} must be a boolean")


def _validate_optional_string(value: Any, label: str, errors: List[str], *, non_empty: bool = False) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, str):
        _append(errors, f"{label} must be a string")
        return
    if non_empty and not value.strip():
        _append(errors, f"{label} must not be empty")


def _validate_optional_enum(value: Any, allowed: List[Any], label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, str):
        _append(errors, f"{label} must be a string")
        return
    normalized = value.strip().lower()
    if not normalized:
        _append(errors, f"{label} must not be empty")
        return
    allowed_values = [str(item or "").strip().lower() for item in allowed if str(item or "").strip()]
    if normalized not in allowed_values:
        _append(errors, f"{label} must be one of: {', '.join(str(item) for item in allowed)}")


def _validate_optional_array(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")


def _validate_optional_object(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not _is_plain_object(value):
        _append(errors, f"{label} must be an object")


def _validate_json_compatible_value(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING or value is None:
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_compatible_value(item, f"{label}[{index}]", errors)
        return
    if _is_plain_object(value):
        for key, item in value.items():
            if not str(key or "").strip():
                _append(errors, f"{label} keys must not be empty")
                continue
            _validate_json_compatible_value(item, f"{label}.{key}", errors)
        return
    if isinstance(value, (str, int, float, bool)):
        return
    _append(errors, f"{label} must be JSON-compatible")


def _validate_optional_json_object(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not _is_plain_object(value):
        _append(errors, f"{label} must be an object")
        return
    for key, item in value.items():
        if not str(key or "").strip():
            _append(errors, f"{label} keys must not be empty")
            continue
        _validate_json_compatible_value(item, f"{label}.{key}", errors)


def _validate_optional_row_objects(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")


def _validate_optional_string_array(
    value: Any,
    label: str,
    errors: List[str],
    *,
    non_empty: bool = False,
) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not isinstance(item, str):
            _append(errors, f"{label}[{index}] must be a string")
            continue
        if non_empty and not item.strip():
            _append(errors, f"{label}[{index}] must not be empty")


def _coerce_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_optional_integer(value: Any, label: str, errors: List[str], *, minimum: int = 0) -> None:
    if value is _MISSING:
        return
    number = _coerce_number(value)
    if number is None or not number.is_integer():
        _append(errors, f"{label} must be an integer")
        return
    if number < minimum:
        _append(errors, f"{label} must be >= {minimum}")


def _validate_optional_number(value: Any, label: str, errors: List[str], *, minimum: float = 0) -> None:
    if value is _MISSING:
        return
    number = _coerce_number(value)
    if number is None:
        _append(errors, f"{label} must be a number")
        return
    if number < minimum:
        _append(errors, f"{label} must be >= {minimum}")


def _validate_rules_object(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not _is_plain_object(value):
        _append(errors, f"{label} must be an object")
        return
    _validate_optional_string_array(value.get("required_columns", _MISSING), f"{label}.required_columns", errors, non_empty=True)
    _validate_optional_string_array(value.get("forbidden_columns", _MISSING), f"{label}.forbidden_columns", errors, non_empty=True)
    _validate_optional_string_array(value.get("unique_columns", _MISSING), f"{label}.unique_columns", errors, non_empty=True)


def _validate_manifest_object(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not _is_plain_object(value):
        _append(errors, f"{label} must be an object")
        return
    _validate_optional_string(value.get("name", _MISSING), f"{label}.name", errors, non_empty=True)
    _validate_optional_string(value.get("version", _MISSING), f"{label}.version", errors, non_empty=True)
    _validate_optional_string(value.get("api_version", _MISSING), f"{label}.api_version", errors, non_empty=True)
    _validate_optional_string(value.get("entry", _MISSING), f"{label}.entry", errors, non_empty=True)
    _validate_optional_string(value.get("command", _MISSING), f"{label}.command", errors, non_empty=True)
    _validate_optional_boolean(value.get("enabled", _MISSING), f"{label}.enabled", errors)
    _validate_optional_string_array(value.get("capabilities", _MISSING), f"{label}.capabilities", errors, non_empty=True)
    _validate_optional_string_array(value.get("args", _MISSING), f"{label}.args", errors)


def _validate_computed_fields(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        target = str(item.get("as") or item.get("name") or item.get("field") or "").strip()
        if not target:
            _append(errors, f"{label}[{index}] requires one of as/name/field")
        if "expr" in item and not isinstance(item.get("expr"), str):
            _append(errors, f"{label}[{index}].expr must be a string")


def _validate_workflow_steps(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        if not str(item.get("id") or "").strip():
            _append(errors, f"{label}[{index}].id is required")
        _validate_optional_string_array(item.get("depends_on", _MISSING), f"{label}[{index}].depends_on", errors, non_empty=True)
        _validate_optional_string(item.get("operator", _MISSING), f"{label}[{index}].operator", errors)


def _validate_constraint_defs(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        kind = str(item.get("kind") or "").strip().lower()
        if not kind:
            _append(errors, f"{label}[{index}].kind is required")
            continue
        if kind not in {"sum_equals", "non_negative"}:
            _append(errors, f"{label}[{index}].kind must be one of: sum_equals, non_negative")
            continue
        if kind == "sum_equals":
            _validate_optional_string_array(item.get("left", _MISSING), f"{label}[{index}].left", errors, non_empty=True)
            _validate_optional_string(item.get("right", _MISSING), f"{label}[{index}].right", errors, non_empty=True)
            _validate_optional_number(item.get("tolerance", _MISSING), f"{label}[{index}].tolerance", errors, minimum=0)
        if kind == "non_negative":
            _validate_optional_string(item.get("field", _MISSING), f"{label}[{index}].field", errors, non_empty=True)


def _validate_aggregate_defs(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        if not str(item.get("op") or "").strip():
            _append(errors, f"{label}[{index}].op is required")
        if not str(item.get("as") or "").strip():
            _append(errors, f"{label}[{index}].as is required")


def _validate_ai_providers(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        identity = str(item.get("name") or item.get("model") or item.get("endpoint") or "").strip()
        if not identity:
            _append(errors, f"{label}[{index}] requires one of name/model/endpoint")
        _validate_optional_string(item.get("name", _MISSING), f"{label}[{index}].name", errors)
        _validate_optional_string(item.get("model", _MISSING), f"{label}[{index}].model", errors)
        _validate_optional_string(item.get("endpoint", _MISSING), f"{label}[{index}].endpoint", errors)


def _validate_window_functions(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not isinstance(value, list):
        _append(errors, f"{label} must be an array")
        return
    for index, item in enumerate(value):
        if not _is_plain_object(item):
            _append(errors, f"{label}[{index}] must be an object")
            continue
        if not str(item.get("op") or "").strip():
            _append(errors, f"{label}[{index}].op is required")
        if not str(item.get("as") or "").strip():
            _append(errors, f"{label}[{index}].as is required")


def _validate_slot_bindings(value: Any, label: str, errors: List[str]) -> None:
    if value is _MISSING:
        return
    if not _is_plain_object(value):
        _append(errors, f"{label} must be an object")
        return
    for key, slot_value in value.items():
        if not str(key or "").strip():
            _append(errors, f"{label} keys must not be empty")
            continue
        if key == "chart_main" and _is_plain_object(slot_value):
            _validate_optional_array(slot_value.get("categories", _MISSING), f"{label}.{key}.categories", errors)
            _validate_optional_array(slot_value.get("series", _MISSING), f"{label}.{key}.series", errors)


def _get_config_value_at_path(config: Any, raw_path: Any) -> Any:
    path = str(raw_path or "").strip()
    if not path:
        return _MISSING
    current: Any = config
    for segment in path.split("."):
        if not segment:
            continue
        if not _is_plain_object(current) or segment not in current:
            return _MISSING
        current = current[segment]
    return current


def _validate_conditional_required_non_empty(
    config: Dict[str, Any],
    rule: Dict[str, Any],
    label: str,
    prefix: str,
    errors: List[str],
) -> None:
    expected = [
        str(item or "").strip().lower()
        for item in (rule.get("one_of") if isinstance(rule.get("one_of"), list) else [])
        if str(item or "").strip()
    ]
    when_value = str(_get_config_value_at_path(config, rule.get("when_path")) or "").strip().lower()
    if not when_value or when_value not in expected:
        return
    value = _get_config_value_at_path(config, rule.get("path"))
    if not _has_present_value(value):
        _append(
            errors,
            f"{label} is required when {prefix}.{str(rule.get('when_path') or '').strip()} is {when_value}",
        )


def _has_present_value(value: Any) -> bool:
    if value is _MISSING or value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _validate_paired_required(
    config: Dict[str, Any],
    rule: Dict[str, Any],
    label: str,
    prefix: str,
    errors: List[str],
) -> None:
    value = _get_config_value_at_path(config, rule.get("path"))
    paired_value = _get_config_value_at_path(config, rule.get("paired_path"))
    has_value = _has_present_value(value)
    has_paired_value = paired_value is not _MISSING
    if not has_value and has_paired_value:
        _append(
            errors,
            f"{label} is required when {prefix}.{str(rule.get('paired_path') or '').strip()} is provided",
        )
        return
    if has_value and not has_paired_value:
        _append(
            errors,
            f"{prefix}.{str(rule.get('paired_path') or '').strip()} is required when {label} is provided",
        )


def _validate_op_in_allowed_ops(
    config: Dict[str, Any],
    rule: Dict[str, Any],
    label: str,
    prefix: str,
    errors: List[str],
) -> None:
    op = str(_get_config_value_at_path(config, rule.get("path")) or "").strip().lower()
    allowed = _get_config_value_at_path(config, rule.get("allowed_path"))
    normalized_allowed = [
        str(item or "").strip().lower()
        for item in (allowed if isinstance(allowed, list) else [])
        if str(item or "").strip()
    ]
    if op and normalized_allowed and op not in normalized_allowed:
        _append(
            errors,
            f"{label} must be included in {prefix}.{str(rule.get('allowed_path') or '').strip()} when both are provided",
        )


def validate_contract_backed_node_config(node_type: str, config: Any, prefix: str) -> List[str]:
    contract = load_node_config_contract_set().get("contract_by_type", {}).get(str(node_type or "").strip())
    if contract is None:
        return []

    errors: List[str] = []
    if not _is_plain_object(config):
        _append(errors, f"{prefix} must be an object")
        return errors

    validators = contract.get("validators") if isinstance(contract.get("validators"), list) else []
    for index, rule in enumerate(validators):
        kind = str(rule.get("kind") or "").strip()
        path = str(rule.get("path") or "").strip()
        value = _get_config_value_at_path(config, path)
        label = f"{prefix}.{path}" if path else f"{prefix}.contract[{index}]"
        if kind == "boolean":
            _validate_optional_boolean(value, label, errors)
        elif kind == "string":
            _validate_optional_string(value, label, errors)
        elif kind == "string_non_empty":
            _validate_optional_string(value, label, errors, non_empty=True)
        elif kind == "enum":
            _validate_optional_enum(value, rule.get("allowed") if isinstance(rule.get("allowed"), list) else [], label, errors)
        elif kind == "array":
            _validate_optional_array(value, label, errors)
        elif kind == "object":
            _validate_optional_object(value, label, errors)
        elif kind == "row_objects":
            _validate_optional_row_objects(value, label, errors)
        elif kind == "string_array_non_empty":
            _validate_optional_string_array(value, label, errors, non_empty=True)
        elif kind == "integer_min":
            _validate_optional_integer(value, label, errors, minimum=int(rule.get("min") or 0))
        elif kind == "json_object":
            _validate_optional_json_object(value, label, errors)
        elif kind == "json_compatible":
            _validate_json_compatible_value(value, label, errors)
        elif kind == "rules_object":
            _validate_rules_object(value, label, errors)
        elif kind == "computed_fields":
            _validate_computed_fields(value, label, errors)
        elif kind == "workflow_steps":
            _validate_workflow_steps(value, label, errors)
        elif kind == "constraint_defs":
            _validate_constraint_defs(value, label, errors)
        elif kind == "aggregate_defs":
            _validate_aggregate_defs(value, label, errors)
        elif kind == "window_functions":
            _validate_window_functions(value, label, errors)
        elif kind == "slot_bindings":
            _validate_slot_bindings(value, label, errors)
        elif kind == "manifest_object":
            _validate_manifest_object(value, label, errors)
        elif kind == "ai_providers":
            _validate_ai_providers(value, label, errors)
        elif kind == "conditional_required_non_empty":
            _validate_conditional_required_non_empty(config, rule, label, prefix, errors)
        elif kind == "paired_required":
            _validate_paired_required(config, rule, label, prefix, errors)
        elif kind == "op_in_allowed_ops":
            _validate_op_in_allowed_ops(config, rule, label, prefix, errors)
        else:
            _append(errors, f"{prefix}.contract[{index}] validator kind unsupported: {kind}")
    return errors


def validate_workflow_graph_node_configs(graph: Any, *, label_prefix: str = "workflow") -> List[str]:
    nodes = graph.get("nodes") if _is_plain_object(graph) else None
    if not isinstance(nodes, list):
        return []

    errors: List[str] = []
    for index, node in enumerate(nodes):
        if not _is_plain_object(node):
            continue
        node_type = str(node.get("type") or "").strip()
        config = node.get("config", _MISSING)
        if config is _MISSING:
            config = {}
        errors.extend(validate_contract_backed_node_config(node_type, config, f"{label_prefix}.nodes[{index}].config"))
    return errors


__all__ = [
    "NODE_CONFIG_CONTRACT_DEFAULT_AUTHORITY",
    "NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY",
    "SUPPORTED_VALIDATOR_KINDS",
    "VALIDATION_ERROR_CODES",
    "build_validation_error_items",
    "build_node_config_contract_runtime_summary",
    "find_unknown_workflow_node_types",
    "load_node_config_contract_set",
    "load_registered_workflow_node_types",
    "normalize_validation_error_item",
    "resolve_node_config_contract_path",
    "validate_contract_backed_node_config",
    "validate_workflow_graph_node_configs",
]

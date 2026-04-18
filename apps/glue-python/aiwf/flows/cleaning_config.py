from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional

from aiwf.quality_contract import normalize_value_for_field


def normalize_key(value: str) -> str:
    return (value or "").strip().lower()


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    parsed = to_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except Exception:
        return None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    normalized = normalize_value_for_field(value, "amount")
    if isinstance(normalized, (int, float)) and not isinstance(normalized, bool):
        return float(normalized)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "").replace("，", "")
    try:
        return float(text)
    except Exception:
        return None


def to_decimal(value: Any) -> Optional[Decimal]:
    parsed = to_float(value)
    if parsed is None:
        return None
    try:
        return Decimal(str(parsed))
    except (InvalidOperation, ValueError):
        return None


def to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def quantize_decimal(value: Decimal, digits: int) -> Decimal:
    quantizer = Decimal("1").scaleb(-digits)
    return value.quantize(quantizer, rounding=ROUND_HALF_UP)


def rules_dict(params: Dict[str, Any]) -> Dict[str, Any]:
    rules = params.get("rules")
    if isinstance(rules, dict):
        return rules
    return {}


def rule_param(params: Dict[str, Any], key: str, default: Any = None) -> Any:
    rules = rules_dict(params)
    if key in rules:
        return rules.get(key)
    return params.get(key, default)


def is_generic_rules_enabled(params: Dict[str, Any]) -> bool:
    rules = rules_dict(params)
    if str(rules.get("platform_mode", "")).strip().lower() == "generic":
        return True
    generic_keys = {
        "rename_map",
        "casts",
        "filters",
        "required_fields",
        "default_values",
        "include_fields",
        "exclude_fields",
        "deduplicate_by",
        "sort_by",
        "computed_fields",
        "string_ops",
        "date_ops",
        "field_ops",
        "null_values",
        "trim_strings",
        "lowercase_fields",
        "uppercase_fields",
    }
    return any(key in rules for key in generic_keys)


def validate_cleaning_rules_impl(
    params: Dict[str, Any],
    *,
    validate_artifact_selection_config_with_tokens: Callable[..., Dict[str, Any]],
    list_cleaning_artifact_tokens: Callable[[], List[str]],
    list_office_artifact_tokens: Callable[[], List[str]],
) -> Dict[str, Any]:
    if not isinstance(params, dict):
        return {"ok": False, "errors": ["params must be an object"], "warnings": []}

    rules = params.get("rules") if isinstance(params.get("rules"), dict) else params
    if not isinstance(rules, dict):
        return {"ok": False, "errors": ["rules must be an object"], "warnings": []}

    errors: List[str] = []
    warnings: List[str] = []

    allowed_keys = {
        "platform_mode",
        "rename_map",
        "casts",
        "filters",
        "required_fields",
        "default_values",
        "include_fields",
        "exclude_fields",
        "deduplicate_by",
        "deduplicate_keep",
        "sort_by",
        "computed_fields",
        "string_ops",
        "date_ops",
        "field_ops",
        "null_values",
        "trim_strings",
        "lowercase_fields",
        "uppercase_fields",
        "id_field",
        "amount_field",
        "amount_round_digits",
        "drop_negative_amount",
        "min_amount",
        "max_amount",
        "deduplicate_by_id",
        "sort_by_id",
        "allow_empty_output",
        "local_parquet_strict",
        "max_invalid_rows",
        "max_filtered_rows",
        "min_output_rows",
        "max_invalid_ratio",
        "max_required_missing_ratio",
        "quality_rules",
        "image_rules",
        "xlsx_rules",
        "sheet_profiles",
        "sheet_allowlist",
        "include_hidden_sheets",
        "canonical_profile",
        "template_expected_profile",
        "cleaning_template",
        "cleaning_spec_v2",
        "blank_output_expected",
        "profile_mismatch_action",
        "force_local_cleaning",
        "use_rust_v2",
        "rust_v2_timeout_seconds",
        "artifact_selection",
        "office_outputs_enabled",
        "enabled_office_artifacts",
        "disabled_office_artifacts",
        "enabled_core_artifacts",
        "disabled_core_artifacts",
    }
    unknown = [key for key in rules.keys() if key not in allowed_keys]
    if unknown:
        warnings.append(f"unknown rule keys: {', '.join(sorted(unknown))}")

    if "platform_mode" in rules and str(rules.get("platform_mode", "")).strip().lower() not in {"generic", ""}:
        errors.append("platform_mode must be 'generic' when provided")

    if "rename_map" in rules and not isinstance(rules.get("rename_map"), dict):
        errors.append("rename_map must be an object")
    if "casts" in rules and not isinstance(rules.get("casts"), dict):
        errors.append("casts must be an object")
    if "default_values" in rules and not isinstance(rules.get("default_values"), dict):
        errors.append("default_values must be an object")
    if "cleaning_spec_v2" in rules and not isinstance(rules.get("cleaning_spec_v2"), dict):
        errors.append("cleaning_spec_v2 must be an object")
    for key in ["quality_rules", "image_rules", "xlsx_rules", "sheet_profiles"]:
        if key in rules and not isinstance(rules.get(key), dict):
            errors.append(f"{key} must be an object")

    for key in ["required_fields", "include_fields", "exclude_fields", "deduplicate_by", "lowercase_fields", "uppercase_fields", "null_values", "sheet_allowlist"]:
        if key in rules and not isinstance(rules.get(key), list):
            errors.append(f"{key} must be an array")
    if "include_hidden_sheets" in rules and not isinstance(rules.get("include_hidden_sheets"), bool):
        errors.append("include_hidden_sheets must be boolean")
    if "blank_output_expected" in rules and not isinstance(rules.get("blank_output_expected"), bool):
        errors.append("blank_output_expected must be boolean")
    if "profile_mismatch_action" in rules:
        action = str(rules.get("profile_mismatch_action", "")).strip().lower()
        if action not in {"", "warn", "block"}:
            errors.append("profile_mismatch_action must be 'warn' or 'block'")

    if "filters" in rules:
        filters = rules.get("filters")
        if not isinstance(filters, list):
            errors.append("filters must be an array")
        else:
            allowed_ops = {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in", "contains", "regex", "exists", "not_exists"}
            for index, item in enumerate(filters):
                if not isinstance(item, dict):
                    errors.append(f"filters[{index}] must be an object")
                    continue
                op = str(item.get("op", "eq")).strip().lower()
                if op not in allowed_ops:
                    errors.append(f"filters[{index}].op must be one of {sorted(allowed_ops)}")
                if op not in {"exists", "not_exists"} and "field" not in item:
                    errors.append(f"filters[{index}].field is required")

    if "deduplicate_keep" in rules:
        keep = str(rules.get("deduplicate_keep", "")).strip().lower()
        if keep not in {"first", "last"}:
            errors.append("deduplicate_keep must be 'first' or 'last'")

    if "sort_by" in rules:
        sort_by = rules.get("sort_by")
        if not isinstance(sort_by, list):
            errors.append("sort_by must be an array")
        else:
            for index, item in enumerate(sort_by):
                if isinstance(item, str):
                    continue
                if not isinstance(item, dict):
                    errors.append(f"sort_by[{index}] must be a string or object")
                    continue
                order = str(item.get("order", "asc")).strip().lower()
                if order not in {"asc", "desc"}:
                    errors.append(f"sort_by[{index}].order must be 'asc' or 'desc'")

    if "amount_round_digits" in rules:
        digits = to_int(rules.get("amount_round_digits"))
        if digits is None or digits < 0 or digits > 6:
            errors.append("amount_round_digits must be integer in range [0,6]")

    if "max_invalid_ratio" in rules:
        ratio = to_decimal(rules.get("max_invalid_ratio"))
        if ratio is None or ratio < 0 or ratio > 1:
            errors.append("max_invalid_ratio must be a number in [0,1]")
    if "max_required_missing_ratio" in rules:
        ratio2 = to_decimal(rules.get("max_required_missing_ratio"))
        if ratio2 is None or ratio2 < 0 or ratio2 > 1:
            errors.append("max_required_missing_ratio must be a number in [0,1]")

    for key in ["max_invalid_rows", "max_filtered_rows", "min_output_rows"]:
        if key in rules:
            value = to_int(rules.get(key))
            if value is None or value < 0:
                errors.append(f"{key} must be a non-negative integer")

    artifact_validation = validate_artifact_selection_config_with_tokens(
        params,
        allowed_core_tokens=list_cleaning_artifact_tokens(),
        allowed_office_tokens=list_office_artifact_tokens(),
    )
    errors.extend(artifact_validation.get("errors", []))
    warnings.extend(artifact_validation.get("warnings", []))

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

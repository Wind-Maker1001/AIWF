from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Dict


def _quality_gate_param(
    params: Dict[str, Any],
    rule_param: Callable[[Dict[str, Any], str, Any], Any],
    key: str,
    default: Any = None,
) -> Any:
    quality_rules = params.get("quality_rules") if isinstance(params.get("quality_rules"), dict) else {}
    if key in quality_rules:
        return quality_rules.get(key)
    if key == "required_fields" and "required_columns" in quality_rules:
        return quality_rules.get("required_columns")
    if key == "required_fields" and isinstance(params.get("xlsx_rules"), dict) and "required_columns" in params["xlsx_rules"]:
        return params["xlsx_rules"].get("required_columns")
    return rule_param(params, key, default)


def apply_quality_gates_impl(
    quality: Dict[str, Any],
    params: Dict[str, Any],
    *,
    to_int: Callable[[Any], int | None],
    to_decimal: Callable[[Any], Decimal | None],
    rule_param: Callable[[Dict[str, Any], str, Any], Any],
) -> Dict[str, Any]:
    input_rows = int(quality.get("input_rows", 0))
    output_rows = int(quality.get("output_rows", 0))
    invalid_rows = int(quality.get("invalid_rows", 0))
    filtered_rows = int(quality.get("filtered_rows", 0))
    required_missing_ratio = to_decimal(quality.get("required_missing_ratio"))
    duplicate_rows_removed = int(quality.get("duplicate_rows_removed", 0))

    max_invalid_rows = to_int(_quality_gate_param(params, rule_param, "max_invalid_rows"))
    max_filtered_rows = to_int(_quality_gate_param(params, rule_param, "max_filtered_rows"))
    min_output_rows = to_int(_quality_gate_param(params, rule_param, "min_output_rows"))
    max_invalid_ratio = to_decimal(_quality_gate_param(params, rule_param, "max_invalid_ratio"))
    max_required_missing_ratio = to_decimal(_quality_gate_param(params, rule_param, "max_required_missing_ratio"))
    max_duplicate_rows_removed = to_int(_quality_gate_param(params, rule_param, "max_duplicate_rows_removed"))
    allow_empty_output_value = _quality_gate_param(params, rule_param, "allow_empty_output", None)
    allow_empty_output = None if allow_empty_output_value is None else str(allow_empty_output_value).strip().lower() in {"1", "true", "yes", "on"}

    if max_invalid_rows is not None and invalid_rows > max_invalid_rows:
        raise RuntimeError(
            f"quality gate failed: invalid_rows={invalid_rows} exceeds max_invalid_rows={max_invalid_rows}"
        )
    if max_filtered_rows is not None and filtered_rows > max_filtered_rows:
        raise RuntimeError(
            f"quality gate failed: filtered_rows={filtered_rows} exceeds max_filtered_rows={max_filtered_rows}"
        )
    if min_output_rows is not None and output_rows < min_output_rows:
        raise RuntimeError(
            f"quality gate failed: output_rows={output_rows} below min_output_rows={min_output_rows}"
        )
    if max_invalid_ratio is not None:
        ratio = (Decimal(invalid_rows) / Decimal(input_rows)) if input_rows > 0 else Decimal("0")
        if ratio > max_invalid_ratio:
            raise RuntimeError(
                f"quality gate failed: invalid_ratio={float(ratio):.6f} exceeds max_invalid_ratio={float(max_invalid_ratio):.6f}"
            )
    if max_required_missing_ratio is not None and required_missing_ratio is not None:
        if required_missing_ratio > max_required_missing_ratio:
            raise RuntimeError(
                f"quality gate failed: required_missing_ratio={float(required_missing_ratio):.6f} exceeds max_required_missing_ratio={float(max_required_missing_ratio):.6f}"
            )
    if max_duplicate_rows_removed is not None and duplicate_rows_removed > max_duplicate_rows_removed:
        raise RuntimeError(
            f"quality gate failed: duplicate_rows_removed={duplicate_rows_removed} exceeds max_duplicate_rows_removed={max_duplicate_rows_removed}"
        )
    if allow_empty_output is False and output_rows <= 0:
        raise RuntimeError("quality gate failed: output_rows=0 while allow_empty_output=false")

    return {
        "max_invalid_rows": max_invalid_rows,
        "max_filtered_rows": max_filtered_rows,
        "min_output_rows": min_output_rows,
        "max_invalid_ratio": (float(max_invalid_ratio) if max_invalid_ratio is not None else None),
        "max_required_missing_ratio": (float(max_required_missing_ratio) if max_required_missing_ratio is not None else None),
        "max_duplicate_rows_removed": max_duplicate_rows_removed,
        "allow_empty_output": allow_empty_output,
        "evaluated": True,
    }

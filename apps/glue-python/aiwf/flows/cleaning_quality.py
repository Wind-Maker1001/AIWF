from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Dict


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

    max_invalid_rows = to_int(rule_param(params, "max_invalid_rows"))
    max_filtered_rows = to_int(rule_param(params, "max_filtered_rows"))
    min_output_rows = to_int(rule_param(params, "min_output_rows"))
    max_invalid_ratio = to_decimal(rule_param(params, "max_invalid_ratio"))

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

    return {
        "max_invalid_rows": max_invalid_rows,
        "max_filtered_rows": max_filtered_rows,
        "min_output_rows": min_output_rows,
        "max_invalid_ratio": (float(max_invalid_ratio) if max_invalid_ratio is not None else None),
        "evaluated": True,
    }

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Dict, List


def _update_numeric_summary(summary: Dict[str, Any], value: Decimal) -> None:
    if summary["count"] == 0:
        summary["min"] = value
        summary["max"] = value
    else:
        if value < summary["min"]:
            summary["min"] = value
        if value > summary["max"]:
            summary["max"] = value
    summary["count"] += 1
    summary["sum"] += value


def _finalize_numeric_summary(
    summary: Dict[str, Any],
    *,
    quantize_decimal: Callable[[Decimal, int], Decimal],
) -> Dict[str, float]:
    avg = summary["sum"] / Decimal(summary["count"])
    return {
        "sum": float(quantize_decimal(summary["sum"], 2)),
        "min": float(quantize_decimal(summary["min"], 2)),
        "max": float(quantize_decimal(summary["max"], 2)),
        "avg": float(quantize_decimal(avg, 2)),
    }


def build_profile_impl(
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    source: str,
    *,
    to_decimal: Callable[[Any], Decimal | None],
    quantize_decimal: Callable[[Decimal, int], Decimal],
) -> Dict[str, Any]:
    field_set = set()
    numeric_summaries: Dict[str, Dict[str, Any]] = {}
    amount_summary = {
        "count": 0,
        "sum": Decimal("0"),
        "min": Decimal("0"),
        "max": Decimal("0"),
    }

    for row in rows:
        amount_value = to_decimal(row.get("amount"))
        if amount_value is not None:
            _update_numeric_summary(amount_summary, amount_value)

        for field, raw_value in row.items():
            field_set.add(field)
            numeric_value = to_decimal(raw_value)
            if numeric_value is None:
                continue
            summary = numeric_summaries.setdefault(
                field,
                {
                    "count": 0,
                    "sum": Decimal("0"),
                    "min": Decimal("0"),
                    "max": Decimal("0"),
                },
            )
            _update_numeric_summary(summary, numeric_value)

    numeric_stats = {
        field: _finalize_numeric_summary(summary, quantize_decimal=quantize_decimal)
        for field, summary in sorted(numeric_summaries.items())
        if summary["count"] > 0
    }

    if amount_summary["count"] > 0:
        sum_amount = float(quantize_decimal(amount_summary["sum"], 2))
        min_amount = float(quantize_decimal(amount_summary["min"], 2))
        max_amount = float(quantize_decimal(amount_summary["max"], 2))
        avg_amount = float(quantize_decimal(amount_summary["sum"] / Decimal(amount_summary["count"]), 2))
    else:
        sum_amount = 0.0
        min_amount = 0.0
        max_amount = 0.0
        avg_amount = 0.0

    return {
        "rows": len(rows),
        "cols": len(field_set),
        "sum_amount": sum_amount,
        "min_amount": min_amount,
        "max_amount": max_amount,
        "avg_amount": avg_amount,
        "quality": quality,
        "fields": sorted(field_set),
        "numeric_stats": numeric_stats,
        "source": source,
    }

from __future__ import annotations

from typing import Any, Callable, Dict, List


def clean_rows_simple(raw_rows: List[Dict[str, Any]], params: Dict[str, Any], hooks: Dict[str, Callable[..., Any]]) -> Dict[str, Any]:
    is_generic_rules_enabled = hooks["is_generic_rules_enabled"]
    clean_rows_generic = hooks["clean_rows_generic"]
    to_int = hooks["to_int"]
    to_bool = hooks["to_bool"]
    rule_param = hooks["rule_param"]
    to_decimal = hooks["to_decimal"]
    normalize_key = hooks["normalize_key"]
    quantize_decimal = hooks["quantize_decimal"]

    if is_generic_rules_enabled(params):
        return clean_rows_generic(raw_rows, params)

    digits = to_int(rule_param(params, "amount_round_digits"))
    if digits is None:
        digits = 2
    digits = max(0, min(6, digits))

    drop_negative = to_bool(rule_param(params, "drop_negative_amount", False), default=False)
    deduplicate = to_bool(rule_param(params, "deduplicate_by_id", True), default=True)
    dedup_keep = str(rule_param(params, "deduplicate_keep", "last")).strip().lower()
    if dedup_keep not in {"first", "last"}:
        dedup_keep = "last"
    sort_by_id = to_bool(rule_param(params, "sort_by_id", True), default=True)

    min_amount = to_decimal(rule_param(params, "min_amount"))
    max_amount = to_decimal(rule_param(params, "max_amount"))
    id_field = str(rule_param(params, "id_field", "id")).strip() or "id"
    amount_field = str(rule_param(params, "amount_field", "amount")).strip() or "amount"

    normalized: List[Dict[str, Any]] = []
    invalid_rows = 0
    filtered_rows = 0
    invalid_id_rows = 0
    invalid_amount_rows = 0
    negative_filtered_rows = 0
    min_filtered_rows = 0
    max_filtered_rows = 0

    for row in raw_rows:
        obj = {normalize_key(k): v for k, v in dict(row or {}).items()}
        id_val = to_int(obj.get(normalize_key(id_field)))
        amount_val = to_decimal(obj.get(normalize_key(amount_field)))
        if id_val is None:
            invalid_id_rows += 1
            invalid_rows += 1
            continue
        if amount_val is None:
            invalid_amount_rows += 1
            invalid_rows += 1
            continue
        if drop_negative and amount_val < 0:
            negative_filtered_rows += 1
            filtered_rows += 1
            continue
        if min_amount is not None and amount_val < min_amount:
            min_filtered_rows += 1
            filtered_rows += 1
            continue
        if max_amount is not None and amount_val > max_amount:
            max_filtered_rows += 1
            filtered_rows += 1
            continue
        normalized.append({"id": id_val, "amount": float(quantize_decimal(amount_val, digits))})

    duplicate_rows_removed = 0
    cleaned = normalized
    if deduplicate:
        deduped: Dict[int, Dict[str, Any]] = {}
        if dedup_keep == "first":
            for row in normalized:
                if row["id"] not in deduped:
                    deduped[row["id"]] = row
        else:
            for row in normalized:
                deduped[row["id"]] = row
        cleaned = list(deduped.values())
        duplicate_rows_removed = len(normalized) - len(cleaned)

    if sort_by_id:
        cleaned = sorted(cleaned, key=lambda x: x["id"])

    quality = {
        "input_rows": len(raw_rows),
        "output_rows": len(cleaned),
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "rule_hits": {
            "invalid_id": invalid_id_rows,
            "invalid_amount": invalid_amount_rows,
            "filtered_negative": negative_filtered_rows,
            "filtered_min_amount": min_filtered_rows,
            "filtered_max_amount": max_filtered_rows,
            "deduplicate_removed": duplicate_rows_removed,
        },
    }
    return {"rows": cleaned, "quality": quality}

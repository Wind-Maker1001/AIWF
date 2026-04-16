from __future__ import annotations

from typing import Any, Callable, Dict, List


def clean_rows_simple(raw_rows: List[Dict[str, Any]], params: Dict[str, Any], hooks: Dict[str, Callable[..., Any]]) -> Dict[str, Any]:
    is_generic_rules_enabled = hooks["is_generic_rules_enabled"]
    clean_rows_generic = hooks["clean_rows_generic"]
    to_int = hooks["to_int"]
    to_bool = hooks["to_bool"]
    rule_param = hooks["rule_param"]
    rules_dict = hooks["rules_dict"]
    to_decimal = hooks["to_decimal"]
    normalize_key = hooks["normalize_key"]
    quantize_decimal = hooks["quantize_decimal"]
    try:
        sample_limit = max(0, min(100, int(params.get("audit_sample_limit", 5) or 5)))
    except Exception:
        sample_limit = 5

    def add_reason_sample(reason: str, payload: Dict[str, Any]) -> None:
        items = reason_samples.setdefault(reason, [])
        if len(items) < sample_limit:
            items.append(dict(payload))

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
    reason_samples: Dict[str, List[Dict[str, Any]]] = {
        "invalid_object": [],
        "cast_failed": [],
        "required_missing": [],
        "filter_rejected": [],
        "duplicate_removed": [],
    }

    for row in raw_rows:
        obj = {normalize_key(k): v for k, v in dict(row or {}).items()}
        id_val = to_int(obj.get(normalize_key(id_field)))
        amount_val = to_decimal(obj.get(normalize_key(amount_field)))
        if id_val is None:
            invalid_id_rows += 1
            invalid_rows += 1
            add_reason_sample(
                "required_missing",
                {
                    "reason": "invalid_id",
                    "field": normalize_key(id_field),
                    "row": dict(obj),
                },
            )
            continue
        if amount_val is None:
            invalid_amount_rows += 1
            invalid_rows += 1
            add_reason_sample(
                "required_missing",
                {
                    "reason": "invalid_amount",
                    "field": normalize_key(amount_field),
                    "row": dict(obj),
                },
            )
            continue
        if drop_negative and amount_val < 0:
            negative_filtered_rows += 1
            filtered_rows += 1
            add_reason_sample(
                "filter_rejected",
                {
                    "reason": "filtered_negative",
                    "field": "amount",
                    "row": {"id": id_val, "amount": float(amount_val)},
                },
            )
            continue
        if min_amount is not None and amount_val < min_amount:
            min_filtered_rows += 1
            filtered_rows += 1
            add_reason_sample(
                "filter_rejected",
                {
                    "reason": "filtered_min_amount",
                    "field": "amount",
                    "threshold": float(min_amount),
                    "row": {"id": id_val, "amount": float(amount_val)},
                },
            )
            continue
        if max_amount is not None and amount_val > max_amount:
            max_filtered_rows += 1
            filtered_rows += 1
            add_reason_sample(
                "filter_rejected",
                {
                    "reason": "filtered_max_amount",
                    "field": "amount",
                    "threshold": float(max_amount),
                    "row": {"id": id_val, "amount": float(amount_val)},
                },
            )
            continue
        normalized.append({"id": id_val, "amount": float(quantize_decimal(amount_val, digits))})

    duplicate_rows_removed = 0
    duplicate_review_required_count = 0
    cleaned = normalized
    if deduplicate:
        deduped: Dict[int, Dict[str, Any]] = {}
        if dedup_keep == "first":
            for row in normalized:
                if row["id"] not in deduped:
                    deduped[row["id"]] = row
                else:
                    duplicate_review_required_count += 1
                    add_reason_sample(
                        "duplicate_removed",
                        {
                            "reason": "deduplicate_removed",
                            "deduplicate_keep": dedup_keep,
                            "row": dict(row),
                        },
                    )
        else:
            for row in normalized:
                if row["id"] in deduped:
                    duplicate_review_required_count += 1
                    add_reason_sample(
                        "duplicate_removed",
                        {
                            "reason": "deduplicate_removed",
                            "deduplicate_keep": dedup_keep,
                            "row": dict(deduped[row["id"]]),
                        },
                    )
                deduped[row["id"]] = row
        cleaned = list(deduped.values())
        duplicate_rows_removed = len(normalized) - len(cleaned)

    if sort_by_id:
        cleaned = sorted(cleaned, key=lambda x: x["id"])

    rules = rules_dict(params)
    quality_rules = params.get("quality_rules") if isinstance(params.get("quality_rules"), dict) else {}
    required_fields = quality_rules.get("required_fields")
    if not isinstance(required_fields, list):
        required_fields = rules.get("required_fields") if isinstance(rules.get("required_fields"), list) else ["id", "amount"]
    required_missing_cells = 0
    required_missing_by_field: Dict[str, int] = {}
    for field in [str(item) for item in required_fields]:
        missing = 0
        for row in cleaned:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                missing += 1
        required_missing_cells += missing
        required_missing_by_field[field] = missing
    required_total_cells = len(cleaned) * len(required_fields) if required_fields else 0
    required_missing_ratio = (
        float(required_missing_cells) / float(required_total_cells)
        if required_total_cells > 0
        else 0.0
    )

    quality = {
        "input_rows": len(raw_rows),
        "output_rows": len(cleaned),
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "duplicate_review_required_count": duplicate_review_required_count,
        "required_fields": [str(item) for item in required_fields],
        "required_missing_cells": required_missing_cells,
        "required_missing_by_field": required_missing_by_field,
        "required_missing_ratio": required_missing_ratio,
        "rule_hits": {
            "invalid_id": invalid_id_rows,
            "invalid_amount": invalid_amount_rows,
            "filtered_negative": negative_filtered_rows,
            "filtered_min_amount": min_filtered_rows,
            "filtered_max_amount": max_filtered_rows,
            "deduplicate_removed": duplicate_rows_removed,
        },
    }
    return {"rows": cleaned, "quality": quality, "reason_samples": reason_samples}

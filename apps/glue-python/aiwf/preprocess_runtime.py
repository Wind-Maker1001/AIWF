from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


def preprocess_rows_impl(
    rows: List[Dict[str, Any]],
    spec: Dict[str, Any],
    *,
    normalize_header: Callable[[str], str],
    normalize_amount: Callable[[Any, int], Any],
    normalize_date: Callable[[Any, str, List[str]], Any],
    apply_field_transform: Callable[[Any, str, Dict[str, Any]], Tuple[Any, bool]],
    filter_match: Callable[[Dict[str, Any], Dict[str, Any]], bool],
    chunk_text: Callable[[str, str, int], List[str]],
    to_canonical_evidence_row: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    apply_conflict_detection: Callable[[List[Dict[str, Any]], Dict[str, Any]], Tuple[List[Dict[str, Any]], int]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    header_map = spec.get("header_map") if isinstance(spec.get("header_map"), dict) else {}
    null_values = [str(x).strip().lower() for x in (spec.get("null_values") or ["null", "none", "na", "n/a"])]
    amount_fields = [str(x) for x in (spec.get("amount_fields") or ["amount"])]
    date_fields = [str(x) for x in (spec.get("date_fields") or [])]
    amount_round_digits = int(spec.get("amount_round_digits", 2))
    trim_strings = bool(spec.get("trim_strings", True))
    drop_empty_rows = bool(spec.get("drop_empty_rows", True))
    date_output_format = str(spec.get("date_output_format", "%Y-%m-%d"))
    date_input_formats = spec.get("date_input_formats") or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
    ]
    defaults = spec.get("default_values") if isinstance(spec.get("default_values"), dict) else {}
    include_fields = [str(x) for x in (spec.get("include_fields") or [])]
    exclude_fields = [str(x) for x in (spec.get("exclude_fields") or [])]
    field_transforms = spec.get("field_transforms") if isinstance(spec.get("field_transforms"), list) else []
    row_filters = spec.get("row_filters") if isinstance(spec.get("row_filters"), list) else []
    deduplicate_by = [str(x) for x in (spec.get("deduplicate_by") or [])]
    deduplicate_keep = str(spec.get("deduplicate_keep") or "first").strip().lower()
    standardize_evidence = bool(spec.get("standardize_evidence", False))
    evidence_schema = spec.get("evidence_schema") if isinstance(spec.get("evidence_schema"), dict) else {}
    chunk_mode = str(spec.get("chunk_mode") or "none").strip().lower()
    chunk_field = str(spec.get("chunk_field") or ("claim_text" if standardize_evidence else "text")).strip()
    chunk_max_chars = int(spec.get("chunk_max_chars", 500))

    out: List[Dict[str, Any]] = []
    dropped_empty = 0
    normalized_amount_cells = 0
    normalized_date_cells = 0
    transformed_cells = 0
    dropped_by_filters = 0
    duplicate_rows_removed = 0
    standardized_rows = 0
    chunked_rows_created = 0

    for raw in rows:
        row: Dict[str, Any] = {}
        for key, value in dict(raw or {}).items():
            normalized_key = header_map.get(key, normalize_header(key))
            normalized_value = value
            if isinstance(normalized_value, str) and trim_strings:
                normalized_value = normalized_value.strip()
            if isinstance(normalized_value, str) and normalized_value.strip().lower() in null_values:
                normalized_value = None
            row[normalized_key] = normalized_value

        for key, default_value in defaults.items():
            if row.get(key) is None:
                row[key] = default_value

        for field in amount_fields:
            if field in row and row[field] is not None:
                normalized_value = normalize_amount(row[field], amount_round_digits)
                if normalized_value != row[field]:
                    normalized_amount_cells += 1
                row[field] = normalized_value

        for field in date_fields:
            if field in row and row[field] is not None:
                normalized_value = normalize_date(row[field], date_output_format, [str(x) for x in date_input_formats])
                if normalized_value != row[field]:
                    normalized_date_cells += 1
                row[field] = normalized_value

        for transform in field_transforms:
            if not isinstance(transform, dict):
                continue
            field = str(transform.get("field") or "")
            op = str(transform.get("op") or "")
            if not field or not op:
                continue
            normalized_value, changed = apply_field_transform(row.get(field), op, transform)
            if changed:
                transformed_cells += 1
            row[field] = normalized_value

        if include_fields:
            row = {key: row.get(key) for key in include_fields}
        for key in exclude_fields:
            row.pop(key, None)

        if row_filters and any(not filter_match(row, item if isinstance(item, dict) else {}) for item in row_filters):
            dropped_by_filters += 1
            continue

        if drop_empty_rows and all(value is None or str(value).strip() == "" for value in row.values()):
            dropped_empty += 1
            continue

        chunk_targets = chunk_text(str(row.get(chunk_field) or ""), chunk_mode, chunk_max_chars)
        if not chunk_targets:
            chunk_targets = [None]
        chunked_rows_created += max(0, len(chunk_targets) - 1)
        for chunk_index, chunk_value in enumerate(chunk_targets):
            result_row = dict(row)
            if chunk_value is not None:
                result_row[chunk_field] = chunk_value
                result_row["chunk_seq"] = chunk_index
            if standardize_evidence:
                result_row = to_canonical_evidence_row(result_row, evidence_schema)
                standardized_rows += 1
            out.append(result_row)

    if deduplicate_by:
        unique_rows: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        if deduplicate_keep == "last":
            for row in out:
                key = tuple(row.get(field) for field in deduplicate_by)
                unique_rows[key] = row
            deduped = list(unique_rows.values())
        else:
            for row in out:
                key = tuple(row.get(field) for field in deduplicate_by)
                if key not in unique_rows:
                    unique_rows[key] = row
            deduped = list(unique_rows.values())
        duplicate_rows_removed = len(out) - len(deduped)
        out = deduped

    out, conflict_rows_marked = apply_conflict_detection(out, spec)

    summary = {
        "input_rows": len(rows),
        "output_rows": len(out),
        "dropped_empty_rows": dropped_empty,
        "dropped_by_filters": dropped_by_filters,
        "duplicate_rows_removed": duplicate_rows_removed,
        "normalized_amount_cells": normalized_amount_cells,
        "normalized_date_cells": normalized_date_cells,
        "transformed_cells": transformed_cells,
        "standardized_rows": standardized_rows,
        "chunked_rows_created": chunked_rows_created,
        "conflict_rows_marked": conflict_rows_marked,
    }
    return out, summary

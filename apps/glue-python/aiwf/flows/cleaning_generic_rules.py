from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Tuple


def _normalize_null(v: Any, null_tokens: List[str]) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        if s.lower() in null_tokens:
            return None
        return s
    return v


def _cast_value(v: Any, cast_type: str, *, to_int: Callable[..., Any], to_float: Callable[..., Any], to_bool: Callable[..., Any]) -> Tuple[Any, bool]:
    if v is None:
        return None, True
    t = (cast_type or "").strip().lower()
    if t in {"str", "string"}:
        return str(v), True
    if t in {"int", "integer"}:
        i = to_int(v)
        return i, i is not None
    if t in {"float", "double", "number", "decimal"}:
        f = to_float(v)
        return f, f is not None
    if t in {"bool", "boolean"}:
        return to_bool(v, default=False), True
    return v, True


def _filter_match(row: Dict[str, Any], f: Dict[str, Any], *, to_float: Callable[..., Any]) -> bool:
    field = str(f.get("field") or "").strip()
    op = str(f.get("op") or "eq").strip().lower()
    target = f.get("value")
    if not field:
        return True
    val = row.get(field)

    if op == "exists":
        return val is not None
    if op == "not_exists":
        return val is None
    if op == "eq":
        return val == target
    if op == "ne":
        return val != target
    if op in {"gt", "gte", "lt", "lte"}:
        a = to_float(val)
        b = to_float(target)
        if a is None or b is None:
            return False
        if op == "gt":
            return a > b
        if op == "gte":
            return a >= b
        if op == "lt":
            return a < b
        return a <= b
    if op == "in":
        arr = target if isinstance(target, list) else []
        return val in arr
    if op == "not_in":
        arr = target if isinstance(target, list) else []
        return val not in arr
    if op == "contains":
        return str(target) in str(val)
    if op == "regex":
        try:
            return re.search(str(target), str(val)) is not None
        except re.error:
            return False
    return True


def _eval_expr_arg(token: str, row: Dict[str, Any], *, to_float: Callable[..., Any]) -> Any:
    text = str(token or "").strip()
    if text.startswith("$"):
        return row.get(text[1:])
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    num = to_float(text)
    if num is not None:
        return num
    return row.get(text, text)


def _eval_simple_computed_expr(expr: str, row: Dict[str, Any], *, to_float: Callable[..., Any]) -> Any:
    text = str(expr or "").strip()
    if not text:
        return None
    match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\((.*)\)$", text)
    if not match:
        return row.get(text[1:]) if text.startswith("$") else row.get(text, text)
    fn_name = match.group(1).strip().lower()
    args = [item.strip() for item in match.group(2).split(",")] if match.group(2).strip() else []
    values = [_eval_expr_arg(item, row, to_float=to_float) for item in args]

    def num(index: int) -> float:
        value = values[index] if index < len(values) else 0
        parsed = to_float(value)
        return float(parsed) if parsed is not None else 0.0

    if fn_name == "add":
        return sum(num(i) for i in range(len(values)))
    if fn_name == "sub":
        return num(0) - num(1)
    if fn_name == "mul":
        return num(0) * num(1)
    if fn_name == "div":
        denominator = num(1)
        return None if denominator == 0 else num(0) / denominator
    if fn_name == "concat":
        return "".join("" if value is None else str(value) for value in values)
    if fn_name == "coalesce":
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
        return None
    if fn_name == "lower":
        return str(values[0] if values else "").lower()
    if fn_name == "upper":
        return str(values[0] if values else "").upper()
    if fn_name == "trim":
        return str(values[0] if values else "").strip()
    return None


def _parse_ymd_simple(value: Any) -> tuple[int, int, int] | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = (
        text.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .replace(".", "-")
    )
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    matched = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", normalized)
    if matched:
        year, month, day = matched.groups()
        return int(year), int(month), int(day)
    digits = re.sub(r"\D+", "", text)
    matched = re.match(r"^(\d{4})(\d{2})(\d{2})$", digits)
    if matched:
        year, month, day = matched.groups()
        return int(year), int(month), int(day)
    return None


def _round_half_up(value: float, digits: int) -> float:
    factor = 10 ** max(0, int(digits))
    return round(value * factor) / factor


def _apply_field_op(current: Any, op_obj: Dict[str, Any], *, to_float: Callable[..., Any]) -> tuple[Any, bool]:
    kind = str(op_obj.get("op") or "").strip().lower()
    if not kind:
        return current, False
    cur = "" if current is None else str(current)
    if kind == "trim":
        return cur.strip(), True
    if kind == "lower":
        return cur.lower(), True
    if kind == "upper":
        return cur.upper(), True
    if kind == "collapse_whitespace":
        return re.sub(r"\s+", " ", cur).strip(), True
    if kind == "remove_urls":
        return re.sub(r"https?://\S+|www\.\S+", "", cur).strip(), True
    if kind == "remove_emails":
        return re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "", cur).strip(), True
    if kind == "regex_replace":
        pattern = str(op_obj.get("pattern") or "")
        replacement = str(op_obj.get("replace") or op_obj.get("to") or "")
        try:
            return re.sub(pattern, replacement, cur), True
        except re.error:
            return current, False
    if kind == "extract_regex":
        pattern = str(op_obj.get("pattern") or "")
        group = int(op_obj.get("group", 0) or 0)
        try:
            match = re.search(pattern, cur)
        except re.error:
            return current, False
        if not match:
            return current, False
        try:
            return match.group(group), True
        except IndexError:
            return current, False
    if kind == "parse_number":
        parsed = to_float(cur.replace(",", "").replace("$", "").strip())
        return (parsed if parsed is not None else current), (parsed is not None)
    if kind == "round_number":
        parsed = to_float(current)
        if parsed is None:
            return current, False
        return _round_half_up(float(parsed), int(op_obj.get("digits", 2) or 2)), True
    if kind == "scale_number":
        parsed = to_float(current)
        multiplier = to_float(op_obj.get("multiplier"))
        if parsed is None or multiplier is None:
            return current, False
        return float(parsed) * float(multiplier), True
    if kind == "map_value":
        mapping = op_obj.get("mapping") if isinstance(op_obj.get("mapping"), dict) else {}
        key = cur.strip()
        if key in mapping:
            return mapping[key], True
        return current, False
    if kind == "parse_date":
        parsed = _parse_ymd_simple(cur)
        if parsed is None:
            return current, False
        year, month, day = parsed
        return f"{year:04d}-{month:02d}-{day:02d}", True
    return current, False


def clean_rows_generic(raw_rows: List[Dict[str, Any]], params: Dict[str, Any], hooks: Dict[str, Callable[..., Any]]) -> Dict[str, Any]:
    rules_dict = hooks["rules_dict"]
    to_bool = hooks["to_bool"]
    to_int = hooks["to_int"]
    to_float = hooks["to_float"]

    rules = rules_dict(params)
    null_values = [str(x).strip().lower() for x in (rules.get("null_values") or ["null", "none", "na", "n/a"])]
    rename_map = rules.get("rename_map") if isinstance(rules.get("rename_map"), dict) else {}
    casts = rules.get("casts") if isinstance(rules.get("casts"), dict) else {}
    defaults = rules.get("default_values") if isinstance(rules.get("default_values"), dict) else {}
    required_fields = rules.get("required_fields") if isinstance(rules.get("required_fields"), list) else []
    quality_rules = params.get("quality_rules") if isinstance(params.get("quality_rules"), dict) else {}
    gate_required_fields = quality_rules.get("required_fields")
    if not isinstance(gate_required_fields, list):
        gate_required_fields = required_fields
    include_fields = rules.get("include_fields") if isinstance(rules.get("include_fields"), list) else []
    exclude_fields = rules.get("exclude_fields") if isinstance(rules.get("exclude_fields"), list) else []
    filters = rules.get("filters") if isinstance(rules.get("filters"), list) else []
    deduplicate_by = rules.get("deduplicate_by") if isinstance(rules.get("deduplicate_by"), list) else []
    deduplicate_keep = str(rules.get("deduplicate_keep", "last")).strip().lower()
    if deduplicate_keep not in {"first", "last"}:
        deduplicate_keep = "last"
    sort_by = rules.get("sort_by") if isinstance(rules.get("sort_by"), list) else []
    trim_strings = to_bool(rules.get("trim_strings"), default=True)
    lowercase_fields = set(str(x) for x in (rules.get("lowercase_fields") or []))
    uppercase_fields = set(str(x) for x in (rules.get("uppercase_fields") or []))
    computed_fields = rules.get("computed_fields") if isinstance(rules.get("computed_fields"), dict) else {}
    string_ops = rules.get("string_ops") if isinstance(rules.get("string_ops"), list) else []
    date_ops = rules.get("date_ops") if isinstance(rules.get("date_ops"), list) else []
    field_ops = rules.get("field_ops") if isinstance(rules.get("field_ops"), list) else []
    try:
        sample_limit = max(0, min(100, int(params.get("audit_sample_limit", 5) or 5)))
    except Exception:
        sample_limit = 5

    def add_reason_sample(reason: str, payload: Dict[str, Any]) -> None:
        items = reason_samples.setdefault(reason, [])
        if len(items) < sample_limit:
            items.append(dict(payload))

    out: List[Dict[str, Any]] = []
    invalid_rows = 0
    filtered_rows = 0
    cast_failed_rows = 0
    required_failed_rows = 0
    filter_rejected_rows = 0
    string_ops_applied = 0
    date_ops_applied = 0
    field_ops_applied = 0
    reason_samples: Dict[str, List[Dict[str, Any]]] = {
        "invalid_object": [],
        "cast_failed": [],
        "required_missing": [],
        "filter_rejected": [],
        "duplicate_removed": [],
    }

    for raw in raw_rows:
        if not isinstance(raw, dict):
            invalid_rows += 1
            add_reason_sample(
                "invalid_object",
                {
                    "reason": "invalid_object",
                    "value": raw,
                },
            )
            continue
        row = dict(raw or {})

        for k, v in list(row.items()):
            vv = _normalize_null(v, null_values)
            if isinstance(vv, str) and trim_strings:
                vv = vv.strip()
            row[k] = vv

        for old_k, new_k in rename_map.items():
            if old_k in row:
                row[new_k] = row.pop(old_k)

        if include_fields:
            row = {k: row.get(k) for k in include_fields}
        for k in exclude_fields:
            row.pop(k, None)

        for k, dv in defaults.items():
            if row.get(k) is None:
                row[k] = dv

        for field, expr in computed_fields.items():
            if isinstance(expr, str) and str(expr).strip():
                row[str(field)] = _eval_simple_computed_expr(str(expr), row, to_float=to_float)

        for k in list(row.keys()):
            if isinstance(row[k], str):
                if k in lowercase_fields:
                    row[k] = row[k].lower()
                if k in uppercase_fields:
                    row[k] = row[k].upper()

        for op in string_ops:
            if not isinstance(op, dict):
                continue
            field = str(op.get("field") or "").strip()
            kind = str(op.get("op") or "").strip().lower()
            if not field or not kind or field not in row:
                continue
            value = row.get(field)
            if kind == "trim" and isinstance(value, str):
                row[field] = value.strip()
                string_ops_applied += 1
            elif kind == "lower" and isinstance(value, str):
                row[field] = value.lower()
                string_ops_applied += 1
            elif kind == "upper" and isinstance(value, str):
                row[field] = value.upper()
                string_ops_applied += 1
            elif kind == "replace" and isinstance(value, str):
                row[field] = value.replace(str(op.get("from") or ""), str(op.get("to") or ""))
                string_ops_applied += 1

        cast_failed = False
        failed_fields: List[str] = []
        for k, ctype in casts.items():
            v, ok = _cast_value(row.get(k), str(ctype), to_int=to_int, to_float=to_float, to_bool=to_bool)
            row[k] = v
            if not ok:
                cast_failed = True
                failed_fields.append(str(k))
        if cast_failed:
            cast_failed_rows += 1
            invalid_rows += 1
            add_reason_sample(
                "cast_failed",
                {
                    "reason": "cast_failed",
                    "fields": failed_fields,
                    "row": dict(row),
                },
            )
            continue

        for op in date_ops:
            if not isinstance(op, dict):
                continue
            field = str(op.get("field") or "").strip()
            kind = str(op.get("op") or "").strip().lower()
            out_field = str(op.get("as") or field).strip()
            if not field or not kind or not out_field or field not in row:
                continue
            parsed = _parse_ymd_simple(row.get(field))
            if parsed is None:
                row[out_field] = None
            else:
                year, month, day = parsed
                if kind == "parse_ymd":
                    row[out_field] = f"{year:04d}-{month:02d}-{day:02d}"
                elif kind == "year":
                    row[out_field] = year
                elif kind == "month":
                    row[out_field] = month
                elif kind == "day":
                    row[out_field] = day
                else:
                    continue
            date_ops_applied += 1

        for op in field_ops:
            if not isinstance(op, dict):
                continue
            field = str(op.get("field") or "").strip()
            out_field = str(op.get("as") or field).strip()
            if not field or not out_field or field not in row:
                continue
            next_value, changed = _apply_field_op(row.get(field), op, to_float=to_float)
            row[out_field] = next_value
            if changed:
                field_ops_applied += 1

        missing_fields = [str(k) for k in required_fields if row.get(str(k)) is None]
        missing_required = bool(missing_fields)
        if missing_required:
            required_failed_rows += 1
            invalid_rows += 1
            add_reason_sample(
                "required_missing",
                {
                    "reason": "required_failed",
                    "fields": missing_fields,
                    "row": dict(row),
                },
            )
            continue

        failed_filter = next(
            (
                f if isinstance(f, dict) else {}
                for f in filters
                if not _filter_match(row, f if isinstance(f, dict) else {}, to_float=to_float)
            ),
            None,
        )
        if failed_filter is not None:
            filter_rejected_rows += 1
            filtered_rows += 1
            add_reason_sample(
                "filter_rejected",
                {
                    "reason": "filtered_rules",
                    "filter": dict(failed_filter),
                    "row": dict(row),
                },
            )
            continue

        out.append(row)

    duplicate_rows_removed = 0
    if deduplicate_by:
        key_fields = [str(x) for x in deduplicate_by]
        d: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        if deduplicate_keep == "first":
            for r in out:
                key = tuple(r.get(k) for k in key_fields)
                if key not in d:
                    d[key] = r
                else:
                    add_reason_sample(
                        "duplicate_removed",
                        {
                            "reason": "deduplicate_removed",
                            "key": list(key),
                            "deduplicate_keep": deduplicate_keep,
                            "row": dict(r),
                        },
                    )
        else:
            for r in out:
                key = tuple(r.get(k) for k in key_fields)
                if key in d:
                    add_reason_sample(
                        "duplicate_removed",
                        {
                            "reason": "deduplicate_removed",
                            "key": list(key),
                            "deduplicate_keep": deduplicate_keep,
                            "row": dict(d[key]),
                        },
                    )
                d[key] = r
        deduped = list(d.values())
        duplicate_rows_removed = len(out) - len(deduped)
        out = deduped

    if sort_by:
        for spec in reversed(sort_by):
            if isinstance(spec, dict):
                field = str(spec.get("field") or "")
                reverse = str(spec.get("order") or "asc").strip().lower() == "desc"
            else:
                field = str(spec)
                reverse = False
            if field:
                out.sort(key=lambda x: (x.get(field) is None, x.get(field)), reverse=reverse)

    required_missing_cells = 0
    required_missing_by_field: Dict[str, int] = {}
    if gate_required_fields:
        for field in [str(item) for item in gate_required_fields]:
            missing = 0
            for row in out:
                value = row.get(field)
                if value is None or str(value).strip() == "":
                    missing += 1
            required_missing_cells += missing
            required_missing_by_field[field] = missing
    required_total_cells = len(out) * len(gate_required_fields) if gate_required_fields else 0
    required_missing_ratio = (
        float(required_missing_cells) / float(required_total_cells)
        if required_total_cells > 0
        else 0.0
    )

    quality = {
        "input_rows": len(raw_rows),
        "output_rows": len(out),
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "required_fields": [str(item) for item in gate_required_fields],
        "required_missing_cells": required_missing_cells,
        "required_missing_by_field": required_missing_by_field,
        "required_missing_ratio": required_missing_ratio,
        "rule_hits": {
            "cast_failed": cast_failed_rows,
            "required_failed": required_failed_rows,
            "filtered_rules": filter_rejected_rows,
            "deduplicate_removed": duplicate_rows_removed,
            "string_ops": string_ops_applied,
            "date_ops": date_ops_applied,
            "field_ops": field_ops_applied,
        },
    }
    return {"rows": out, "quality": quality, "reason_samples": reason_samples}

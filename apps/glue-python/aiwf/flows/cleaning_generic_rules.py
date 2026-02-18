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

    out: List[Dict[str, Any]] = []
    invalid_rows = 0
    filtered_rows = 0
    cast_failed_rows = 0
    required_failed_rows = 0
    filter_rejected_rows = 0

    for raw in raw_rows:
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

        for k in list(row.keys()):
            if isinstance(row[k], str):
                if k in lowercase_fields:
                    row[k] = row[k].lower()
                if k in uppercase_fields:
                    row[k] = row[k].upper()

        cast_failed = False
        for k, ctype in casts.items():
            v, ok = _cast_value(row.get(k), str(ctype), to_int=to_int, to_float=to_float, to_bool=to_bool)
            row[k] = v
            if not ok:
                cast_failed = True
        if cast_failed:
            cast_failed_rows += 1
            invalid_rows += 1
            continue

        missing_required = any(row.get(str(k)) is None for k in required_fields)
        if missing_required:
            required_failed_rows += 1
            invalid_rows += 1
            continue

        if any(not _filter_match(row, f if isinstance(f, dict) else {}, to_float=to_float) for f in filters):
            filter_rejected_rows += 1
            filtered_rows += 1
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
            for r in out:
                key = tuple(r.get(k) for k in key_fields)
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

    quality = {
        "input_rows": len(raw_rows),
        "output_rows": len(out),
        "invalid_rows": invalid_rows,
        "filtered_rows": filtered_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "rule_hits": {
            "cast_failed": cast_failed_rows,
            "required_failed": required_failed_rows,
            "filtered_rules": filter_rejected_rows,
            "deduplicate_removed": duplicate_rows_removed,
        },
    }
    return {"rows": out, "quality": quality}

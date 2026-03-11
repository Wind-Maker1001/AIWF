from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple


def _normalize_header(name: str) -> str:
    text = (name or "").strip().lower()
    text = re.sub(r"[\s\-\/]+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "col"


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.startswith("$"):
        text = text[1:]
    try:
        return float(text)
    except Exception:
        return None


def _normalize_amount(value: Any, digits: int = 2) -> Any:
    parsed = _to_float(value)
    if parsed is None:
        return value
    return round(parsed, digits)


def _normalize_date(value: Any, output_fmt: str, formats: List[str]) -> Any:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).strftime(output_fmt)
        except ValueError:
            continue
    return value


def _filter_field(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("field") or "").strip()


def _filter_value(row: Dict[str, Any], cfg: Dict[str, Any]) -> Any:
    field = _filter_field(cfg)
    if not field:
        return None
    return row.get(field)


def _transform_trim(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.strip(), True
    return value, False


def _transform_lower(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.lower(), True
    return value, False


def _transform_upper(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return value.upper(), True
    return value, False


def _transform_collapse_whitespace(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip(), True
    return value, False


def _transform_remove_urls(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"https?://\S+|www\.\S+", "", value).strip(), True
    return value, False


def _transform_remove_emails(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if isinstance(value, str):
        return re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "", value).strip(), True
    return value, False


def _transform_regex_replace(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if not isinstance(value, str):
        return value, False
    pattern = str(cfg.get("pattern") or "")
    replacement = str(cfg.get("replace") or "")
    try:
        return re.sub(pattern, replacement, value), True
    except re.error:
        return value, False


def _transform_parse_number(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    parsed = _to_float(value)
    return (parsed if parsed is not None else value), (parsed is not None)


def _transform_round_number(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    digits = int(cfg.get("digits", 2))
    parsed = _to_float(value)
    if parsed is None:
        return value, False
    return round(parsed, digits), True


def _transform_parse_date(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    output_fmt = str(cfg.get("output_format") or "%Y-%m-%d")
    input_formats = cfg.get("input_formats") or [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
    ]
    if isinstance(value, str):
        normalized = _normalize_date(value, output_fmt, [str(item) for item in input_formats])
        return normalized, normalized != value
    return value, False


def _transform_extract_regex(value: Any, cfg: Dict[str, Any]) -> Tuple[Any, bool]:
    if not isinstance(value, str):
        return value, False
    pattern = str(cfg.get("pattern") or "")
    group = int(cfg.get("group", 0))
    try:
        match = re.search(pattern, value)
    except re.error:
        return value, False
    if not match:
        return value, False
    try:
        return match.group(group), True
    except IndexError:
        return value, False


def _filter_exists(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    value = _filter_value(row, cfg)
    return value is not None and str(value).strip() != ""


def _filter_not_exists(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return not _filter_exists(row, cfg)


def _filter_eq(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return _filter_value(row, cfg) == cfg.get("value")


def _filter_ne(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return _filter_value(row, cfg) != cfg.get("value")


def _filter_compare_numeric(row: Dict[str, Any], cfg: Dict[str, Any], op: str) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    left = _to_float(_filter_value(row, cfg))
    right = _to_float(cfg.get("value"))
    if left is None or right is None:
        return False
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    return left <= right


def _filter_gt(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "gt")


def _filter_gte(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "gte")


def _filter_lt(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "lt")


def _filter_lte(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _filter_compare_numeric(row, cfg, "lte")


def _filter_in(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    values = cfg.get("value") if isinstance(cfg.get("value"), list) else []
    return _filter_value(row, cfg) in values


def _filter_not_in(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    values = cfg.get("value") if isinstance(cfg.get("value"), list) else []
    return _filter_value(row, cfg) not in values


def _filter_contains(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    return str(cfg.get("value")) in str(_filter_value(row, cfg))


def _filter_regex(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    try:
        return re.search(str(cfg.get("value")), str(_filter_value(row, cfg))) is not None
    except re.error:
        return False


def _filter_not_regex(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    field = _filter_field(cfg)
    if not field:
        return True
    try:
        return re.search(str(cfg.get("value")), str(_filter_value(row, cfg))) is None
    except re.error:
        return False


def register_builtin_preprocess_ops(
    register_field_transform: Callable[..., Any],
    register_row_filter: Callable[..., Any],
) -> None:
    domain = {
        "name": "preprocess",
        "label": "Preprocess",
        "backend": "python",
        "builtin": True,
    }
    register_field_transform("trim", _transform_trim, domain="preprocess", domain_metadata=domain)
    register_field_transform("lower", _transform_lower, domain="preprocess", domain_metadata=domain)
    register_field_transform("upper", _transform_upper, domain="preprocess", domain_metadata=domain)
    register_field_transform("collapse_whitespace", _transform_collapse_whitespace, domain="preprocess", domain_metadata=domain)
    register_field_transform("remove_urls", _transform_remove_urls, domain="preprocess", domain_metadata=domain)
    register_field_transform("remove_emails", _transform_remove_emails, domain="preprocess", domain_metadata=domain)
    register_field_transform("regex_replace", _transform_regex_replace, domain="preprocess", domain_metadata=domain)
    register_field_transform("parse_number", _transform_parse_number, domain="preprocess", domain_metadata=domain)
    register_field_transform("round_number", _transform_round_number, domain="preprocess", domain_metadata=domain)
    register_field_transform("parse_date", _transform_parse_date, domain="preprocess", domain_metadata=domain)
    register_field_transform("extract_regex", _transform_extract_regex, domain="preprocess", domain_metadata=domain)

    register_row_filter("exists", _filter_exists, requires_field=False, domain="preprocess", domain_metadata=domain)
    register_row_filter("not_exists", _filter_not_exists, requires_field=False, domain="preprocess", domain_metadata=domain)
    register_row_filter("eq", _filter_eq, domain="preprocess", domain_metadata=domain)
    register_row_filter("ne", _filter_ne, domain="preprocess", domain_metadata=domain)
    register_row_filter("gt", _filter_gt, domain="preprocess", domain_metadata=domain)
    register_row_filter("gte", _filter_gte, domain="preprocess", domain_metadata=domain)
    register_row_filter("lt", _filter_lt, domain="preprocess", domain_metadata=domain)
    register_row_filter("lte", _filter_lte, domain="preprocess", domain_metadata=domain)
    register_row_filter("in", _filter_in, domain="preprocess", domain_metadata=domain)
    register_row_filter("not_in", _filter_not_in, domain="preprocess", domain_metadata=domain)
    register_row_filter("contains", _filter_contains, domain="preprocess", domain_metadata=domain)
    register_row_filter("regex", _filter_regex, domain="preprocess", domain_metadata=domain)
    register_row_filter("not_regex", _filter_not_regex, domain="preprocess", domain_metadata=domain)

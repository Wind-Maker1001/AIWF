from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Dict, List, Mapping, Optional, Sequence

from aiwf.canonical_profiles import (
    get_default_header_aliases,
    get_profile_header_aliases,
    get_profile_registry,
    get_profile_spec,
)


_HEADER_MAPPING_MODE_DEFAULT = "strict"
_HEADER_MAPPING_MODE_VALUES = {"strict", "auto"}
_AUTO_ONLY_HEADER_ALIASES: dict[str, list[str]] = {
    "amount": ["amt"],
    "currency": ["ccy"],
    "biz_date": ["biz dt", "biz_dt"],
    "published_at": ["pub dt"],
    "account_no": ["acct no", "acct#", "acct_num", "acct_no"],
    "txn_date": ["txn dt", "post dt", "posting dt"],
    "debit_amount": ["dr"],
    "credit_amount": ["cr"],
    "balance": ["bal"],
    "counterparty_name": ["cp"],
    "remark": ["memo"],
    "ref_no": ["ref no"],
    "customer_name": ["cust name", "customer name"],
    "phone": ["mobile no", "tel no"],
    "city": ["city", "city name"],
    "source_url": ["src url"],
    "source_title": ["src title"],
}


_DEFAULT_HEADER_ALIASES: dict[str, list[str]] = {
    "id": ["id", "record_id", "row_id", "identifier", "编号", "序号", "单号", "记录编号"],
    "amount": ["amount", "amt", "total_amount", "金额", "总金额", "发生额", "收款金额", "付款金额"],
    "currency": ["currency", "ccy", "币种", "货币", "结算币种"],
    "biz_date": ["biz_date", "date", "transaction_date", "业务日期", "发生日期", "交易日期", "记账日期", "入账日期"],
    "published_at": ["published_at", "publish_date", "published_date", "发布日期", "发布时间", "发布时点"],
    "customer_name": ["customer_name", "name", "customer", "客户", "客户名称", "客户名", "姓名", "联系人", "联系人姓名"],
    "phone": ["phone", "mobile", "tel", "telephone", "手机", "手机号", "手机号码", "电话", "联系电话", "座机"],
    "claim_text": ["claim_text", "text", "content", "正文", "内容", "文本", "观点", "论点", "主张"],
    "source_url": ["source_url", "url", "link", "链接", "网址", "来源链接", "来源网址", "原文链接"],
    "source_title": ["source_title", "title", "标题", "来源标题", "文章标题", "文档标题"],
    "speaker": ["speaker", "author", "name", "作者", "发言人", "说话人", "发布者"],
    "stance": ["stance", "立场", "态度"],
    "confidence": ["confidence", "置信度", "可信度"],
}

_PROFILE_HEADER_ALIASES: dict[str, dict[str, list[str]]] = {
    "finance_statement": {
        "id": ["编号", "序号", "单号"],
        "amount": ["金额", "总金额", "发生额", "本期金额", "收款金额", "付款金额"],
        "currency": ["币种", "货币", "结算币种"],
        "biz_date": ["业务日期", "发生日期", "交易日期", "记账日期", "入账日期"],
        "published_at": ["发布日期", "发布时间"],
    },
    "bank_statement": {
        "account_no": ["账号", "账户", "账户号", "卡号"],
        "txn_date": ["交易日期", "记账日期", "入账日期", "日期"],
        "debit_amount": ["借方金额", "支出", "付款金额"],
        "credit_amount": ["贷方金额", "收入", "收款金额"],
        "balance": ["余额", "账户余额"],
        "counterparty_name": ["对方户名", "对手方", "交易对手"],
        "remark": ["摘要", "附言", "备注", "用途"],
        "ref_no": ["流水号", "交易流水号", "凭证号"],
        "txn_type": ["交易类型", "业务类型", "方向"],
        "currency": ["币种", "货币", "结算币种"],
    },
    "customer_contact": {
        "customer_name": ["客户", "客户名称", "客户名", "姓名", "联系人", "联系人姓名"],
        "phone": ["手机", "手机号", "手机号码", "电话", "联系电话", "座机"],
    },
    "debate_evidence": {
        "claim_text": ["正文", "内容", "文本", "观点", "论点", "主张"],
        "source_title": ["标题", "来源标题", "文章标题", "文档标题"],
        "source_url": ["链接", "网址", "来源链接", "来源网址", "原文链接"],
        "published_at": ["发布日期", "发布时间"],
        "speaker": ["作者", "发言人", "说话人", "发布者"],
        "stance": ["立场", "态度"],
    },
}

_PROFILE_SPECS: dict[str, dict[str, Any]] = {
    "finance_statement": {
        "required_fields": ["id", "amount"],
        "string_fields": ["currency"],
        "numeric_fields": ["id", "amount"],
        "date_fields": ["biz_date", "published_at"],
    },
    "bank_statement": {
        "required_fields": ["account_no", "txn_date"],
        "string_fields": ["account_no", "currency", "counterparty_name", "remark", "ref_no", "txn_type"],
        "numeric_fields": ["debit_amount", "credit_amount", "amount", "balance"],
        "date_fields": ["txn_date"],
    },
    "customer_contact": {
        "required_fields": ["customer_name", "phone"],
        "string_fields": ["customer_name", "city", "phone"],
        "numeric_fields": [],
        "date_fields": [],
    },
    "debate_evidence": {
        "required_fields": ["claim_text"],
        "string_fields": ["claim_text", "speaker", "source_url", "source_title", "stance"],
        "numeric_fields": ["confidence"],
        "date_fields": ["published_at"],
    },
}

_DEFAULT_HEADER_ALIASES = get_default_header_aliases()
_PROFILE_HEADER_ALIASES = {
    name: get_profile_header_aliases(name)
    for name in get_profile_registry().keys()
}
_PROFILE_SPECS = {
    name: get_profile_spec(name)
    for name in get_profile_registry().keys()
}


def _normalize_display_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "").strip())
    text = text.replace("\u3000", " ")
    text = re.sub(r"[‐‑–—−]", "-", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_token(value: Any) -> str:
    text = _normalize_display_text(value).lower()
    text = re.sub(r"[\s\-/:]+", "_", text)
    text = re.sub(r"[^0-9a-z_\u4e00-\u9fff]+", "", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _normalize_text_body(value: Any) -> str:
    text = _normalize_display_text(value).lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _symbol_ratio(text: str) -> float:
    normalized = str(text or "")
    if not normalized:
        return 0.0
    printable = len(re.findall(r"[0-9A-Za-z\u4e00-\u9fff\s]", normalized))
    return max(0.0, float(len(normalized) - printable) / float(len(normalized)))


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _normalize_numeric_text(value: Any) -> str:
    text = _normalize_display_text(value)
    text = text.replace("人民币", "").replace("¥", "").replace("￥", "")
    text = text.replace(" ", "")
    translation = str.maketrans(
        {
            "O": "0",
            "o": "0",
            "〇": "0",
            "I": "1",
            "l": "1",
            "|": "1",
        }
    )
    return text.translate(translation)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    try:
        text = _normalize_numeric_text(value).replace(",", "")
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _to_int(value: Any) -> Optional[int]:
    number = _to_float(value)
    if number is None or not math.isfinite(number):
        return None
    return int(number)


def _load_rapidfuzz():
    try:
        from rapidfuzz import fuzz, process  # type: ignore

        return fuzz, process
    except Exception:
        return None, None


def _load_dateparser():
    try:
        import dateparser  # type: ignore

        return dateparser
    except Exception:
        return None


def _load_phonenumbers():
    try:
        import phonenumbers  # type: ignore

        return phonenumbers
    except Exception:
        return None


def _load_pandera():
    try:
        import pandas as pd  # type: ignore
        import pandera.pandas as pa  # type: ignore

        return pd, pa
    except Exception:
        return None, None


_NEGATIVE_AMOUNT_TOKENS = [
    "\u501f",
    "\u501f\u65b9",
    "\u652f\u51fa",
    "\u4ed8\u6b3e",
    "debit",
    "dr",
]
_POSITIVE_AMOUNT_TOKENS = [
    "\u8d37",
    "\u8d37\u65b9",
    "\u6536\u5165",
    "\u6536\u6b3e",
    "credit",
    "cr",
]
_AMOUNT_UNIT_TOKENS = [
    (100000000.0, ["\u4ebf\u5143", "\u4ebf", "billion"]),
    (10000.0, ["\u4e07\u5143", "\u4e07"]),
    (1000.0, ["\u5343\u5143", "\u5343", "thousand"]),
    (1000000.0, ["million"]),
]
_AMOUNT_CURRENCY_TOKENS = [
    "\u4eba\u6c11\u5e01",
    "\u5143",
    "\u5706",
    "$",
    "\xa5",
    "\uffe5",
    "\u20ac",
    "\xa3",
    "usd",
    "cny",
    "rmb",
    "eur",
    "jpy",
    "gbp",
    "hkd",
]


def _remove_amount_token(text: str, token: str) -> str:
    if re.fullmatch(r"[A-Za-z]+", token):
        return re.sub(re.escape(token), "", text, flags=re.IGNORECASE)
    return text.replace(token, "")


def _detect_amount_sign(text: str) -> float:
    normalized = _normalize_display_text(text)
    lowered = normalized.lower()
    if (
        (normalized.startswith("(") and normalized.endswith(")"))
        or (normalized.startswith("（") and normalized.endswith("）"))
        or normalized.endswith("-")
        or normalized.startswith("-")
    ):
        return -1.0
    if any(token.lower() in lowered for token in _NEGATIVE_AMOUNT_TOKENS):
        return -1.0
    if any(token.lower() in lowered for token in _POSITIVE_AMOUNT_TOKENS):
        return 1.0
    return 1.0


def _detect_amount_multiplier(raw_header: Any) -> float:
    text = _normalize_display_text(raw_header)
    lowered = text.lower()
    for multiplier, tokens in _AMOUNT_UNIT_TOKENS:
        if any(token.lower() in lowered for token in tokens):
            return multiplier
    return 1.0


def _parse_amount_value(value: Any, *, raw_header: Any = None) -> Optional[float]:
    text = _normalize_numeric_text(value)
    if not text:
        return None
    multiplier = _detect_amount_multiplier(raw_header)
    lowered = text.lower()
    for unit_multiplier, tokens in _AMOUNT_UNIT_TOKENS:
        if any(token.lower() in lowered for token in tokens):
            multiplier = max(multiplier, unit_multiplier)
    sign = _detect_amount_sign(text)
    compact = text
    for _unit_multiplier, tokens in _AMOUNT_UNIT_TOKENS:
        for token in tokens:
            compact = _remove_amount_token(compact, token)
    for token in _AMOUNT_CURRENCY_TOKENS + _NEGATIVE_AMOUNT_TOKENS + _POSITIVE_AMOUNT_TOKENS:
        compact = _remove_amount_token(compact, token)
    compact = compact.replace(",", "").replace("，", "")
    compact = compact.strip().strip("()（）").rstrip("-").lstrip("+-")
    if not compact:
        return None
    try:
        return float(compact) * multiplier * sign
    except Exception:
        return None


def _parse_int_like(value: Any) -> Optional[int]:
    text = _normalize_numeric_text(value).replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _parse_date_value(value: Any) -> Optional[str]:
    text = _normalize_display_text(value)
    if not text:
        return None
    dateparser = _load_dateparser()
    if dateparser is not None:
        parsed = dateparser.parse(
            text,
            languages=["zh", "en"],
            settings={"DATE_ORDER": "YMD", "PREFER_DAY_OF_MONTH": "first"},
        )
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d")
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
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    digits = re.sub(r"\D+", "", text)
    matched = re.match(r"^(\d{4})(\d{2})(\d{2})$", digits)
    if matched:
        year, month, day = matched.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return None


def _parse_phone_value(value: Any) -> Optional[str]:
    text = _normalize_display_text(value)
    if not text:
        return None
    phonenumbers = _load_phonenumbers()
    if phonenumbers is not None:
        try:
            parsed = phonenumbers.parse(text, "CN")
            if phonenumbers.is_possible_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            pass
    digits = re.sub(r"\D+", "", text)
    if len(digits) == 11 and digits.startswith("1"):
        return f"+86{digits}"
    if len(digits) == 13 and digits.startswith("86"):
        return f"+{digits}"
    if len(digits) >= 10:
        return digits
    return None


def quality_rules_from_spec(spec: Mapping[str, Any], modality: str) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    base_rules = spec.get("quality_rules")
    if isinstance(base_rules, dict):
        merged.update(base_rules)
    modality_key = f"{modality}_rules"
    modality_rules = spec.get(modality_key)
    if isinstance(modality_rules, dict):
        merged.update(modality_rules)
    return merged


def resolve_canonical_profile(spec: Mapping[str, Any]) -> str:
    quality_rules = spec.get("quality_rules") if isinstance(spec.get("quality_rules"), dict) else {}
    value = spec.get("canonical_profile") or quality_rules.get("canonical_profile") or spec.get("profile") or ""
    return str(value or "").strip().lower()


def resolve_header_mapping_mode(spec: Optional[Mapping[str, Any]] = None) -> str:
    spec_obj = spec or {}
    quality_rules = spec_obj.get("quality_rules") if isinstance(spec_obj.get("quality_rules"), dict) else {}
    value = spec_obj.get("header_mapping_mode") or quality_rules.get("header_mapping_mode") or ""
    mode = str(value or "").strip().lower()
    if mode not in _HEADER_MAPPING_MODE_VALUES:
        return _HEADER_MAPPING_MODE_DEFAULT
    return mode


def header_mapping_runtime_info(spec: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
    requested_mode = resolve_header_mapping_mode(spec)
    fuzz, process = _load_rapidfuzz()
    fuzzy_available = fuzz is not None and process is not None
    date_affinity_available = _load_dateparser() is not None
    if requested_mode != "auto":
        return {
            "requested_mode": requested_mode,
            "effective_mode": "strict",
            "fuzzy_available": fuzzy_available,
            "value_affinity_available": date_affinity_available,
            "fallback_reason": "",
        }
    if not fuzzy_available:
        return {
            "requested_mode": "auto",
            "effective_mode": "strict",
            "fuzzy_available": False,
            "value_affinity_available": False,
            "fallback_reason": "rapidfuzz_unavailable",
        }
    return {
        "requested_mode": "auto",
        "effective_mode": "auto",
        "fuzzy_available": True,
        "value_affinity_available": date_affinity_available,
        "fallback_reason": "" if date_affinity_available else "dateparser_unavailable",
    }


def _flatten_sheet_profiles(spec: Mapping[str, Any]) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    profile_name = resolve_canonical_profile(spec)
    if profile_name in _PROFILE_HEADER_ALIASES:
        for field, values in _PROFILE_HEADER_ALIASES[profile_name].items():
            default_values = list(_DEFAULT_HEADER_ALIASES.get(str(field), []))
            aliases[str(field)] = [str(item) for item in values] + default_values
    for field, values in _DEFAULT_HEADER_ALIASES.items():
        if field not in aliases:
            aliases[field] = list(values)
    profiles = spec.get("sheet_profiles")
    if isinstance(profiles, dict):
        for profile in profiles.values():
            if not isinstance(profile, dict):
                continue
            profile_aliases = profile.get("aliases")
            if not isinstance(profile_aliases, dict):
                continue
            for field, values in profile_aliases.items():
                if isinstance(values, list):
                    aliases.setdefault(str(field), [])
                    aliases[str(field)].extend([str(item) for item in values if str(item).strip()])
    return aliases


def _merge_header_alias_maps(
    base_aliases: Mapping[str, Sequence[str]],
    extra_aliases: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {
        str(field): [str(item) for item in values if str(item).strip()]
        for field, values in base_aliases.items()
    }
    for field, values in extra_aliases.items():
        key = str(field)
        merged.setdefault(key, [])
        merged[key].extend([str(item) for item in values if str(item).strip()])
    return merged


def _header_aliases_for_mode(spec: Mapping[str, Any], mode: str) -> dict[str, list[str]]:
    aliases = _flatten_sheet_profiles(spec)
    if mode != "auto":
        return aliases
    return _merge_header_alias_maps(aliases, _AUTO_ONLY_HEADER_ALIASES)


def _profile_field_sets(profile_name: str) -> tuple[set[str], set[str], set[str], set[str]]:
    profile = _PROFILE_SPECS.get(profile_name, {})
    required_fields = {str(item) for item in (profile.get("required_fields") or []) if str(item).strip()}
    numeric_fields = {str(item) for item in (profile.get("numeric_fields") or []) if str(item).strip()}
    date_fields = {str(item) for item in (profile.get("date_fields") or []) if str(item).strip()}
    field_union = required_fields | numeric_fields | date_fields | {
        str(item) for item in (profile.get("string_fields") or []) if str(item).strip()
    }
    return required_fields, numeric_fields, date_fields, field_union


def _token_display(token: str) -> str:
    return str(token or "").replace("_", " ").strip()


def _header_value_affinity(
    field: str,
    sample_values: Sequence[Any],
    *,
    raw_header: str,
    profile_name: str,
    value_affinity_available: bool,
) -> tuple[float, float]:
    non_empty = [item for item in sample_values if item not in {None, ""} and str(item).strip()]
    if not non_empty:
        return 0.0, 0.0
    _required_fields, numeric_fields, date_fields, _field_union = _profile_field_sets(profile_name)
    field_key = str(field or "").strip()
    sample = non_empty[:10]
    if field_key in numeric_fields and value_affinity_available:
        parsed = 0
        for value in sample:
            normalized = normalize_value_for_field(value, field_key, raw_header=raw_header)
            try:
                float(str(normalized).replace(",", ""))
                parsed += 1
            except Exception:
                continue
        rate = parsed / max(1, len(sample))
        return (0.05, rate) if rate >= 0.7 else (0.0, rate)
    if field_key in date_fields and value_affinity_available:
        parsed = 0
        for value in sample:
            normalized = normalize_value_for_field(value, field_key, raw_header=raw_header)
            if normalized not in {None, ""}:
                parsed += 1
        rate = parsed / max(1, len(sample))
        return (0.05, rate) if rate >= 0.7 else (0.0, rate)
    return 0.0, 0.0


def analyze_header_mapping(
    name: str,
    spec: Optional[Mapping[str, Any]] = None,
    *,
    sample_values: Optional[Sequence[Any]] = None,
) -> dict[str, Any]:
    spec_obj = dict(spec or {})
    normalized = _normalize_token(name)
    if not normalized:
        return {
            "canonical_field": "",
            "confidence": 0.0,
            "matched_token": "",
            "match_strategy": "unresolved",
            "alternatives": [],
            "resolved": False,
            "normalized": "",
        }

    runtime = header_mapping_runtime_info(spec_obj)
    effective_mode = str(runtime.get("effective_mode") or "strict")
    aliases = _header_aliases_for_mode(spec_obj, effective_mode)
    profile_name = resolve_canonical_profile(spec_obj)
    required_fields, _numeric_fields, _date_fields, _field_union = _profile_field_sets(profile_name)

    exact_matches: list[str] = []
    for field, candidates in aliases.items():
        normalized_candidates = {_normalize_token(field), *[_normalize_token(item) for item in candidates]}
        if normalized in normalized_candidates:
            exact_matches.append(field)
    if exact_matches:
        unique_fields: list[str] = []
        for item in exact_matches:
            field_name = str(item or "").strip()
            if field_name and field_name not in unique_fields:
                unique_fields.append(field_name)
        if len(unique_fields) == 1 or effective_mode != "auto":
            field = unique_fields[0]
            return {
                "canonical_field": field,
                "confidence": 1.0,
                "matched_token": normalized,
                "match_strategy": "exact",
                "alternatives": [{"field": field, "confidence": 1.0}],
                "resolved": True,
                "normalized": normalized,
            }
        return {
            "canonical_field": "",
            "confidence": 0.55,
            "matched_token": normalized,
            "match_strategy": "unresolved",
            "alternatives": [{"field": field, "confidence": 1.0} for field in unique_fields[:3]],
            "resolved": False,
            "normalized": normalized,
        }

    substring_matches: list[tuple[str, str]] = []
    for field, candidates in aliases.items():
        for candidate in [field, *candidates]:
            candidate_token = _normalize_token(candidate)
            if not candidate_token:
                continue
            if len(candidate_token) < 2 and candidate_token not in {"id", "url"}:
                continue
            if candidate_token in normalized:
                substring_matches.append((field, candidate_token))
    if substring_matches:
        ranked_matches = sorted(substring_matches, key=lambda item: (-len(item[1]), item[1], item[0]))
        unique_fields = []
        for field, _candidate in ranked_matches:
            if field not in unique_fields:
                unique_fields.append(field)
        if len(unique_fields) == 1 or effective_mode != "auto":
            best_field, best_candidate = ranked_matches[0]
            return {
                "canonical_field": best_field,
                "confidence": 0.88,
                "matched_token": best_candidate,
                "match_strategy": "substring",
                "alternatives": [{"field": best_field, "confidence": 0.88}],
                "resolved": True,
                "normalized": normalized,
            }
        return {
            "canonical_field": "",
            "confidence": 0.55,
            "matched_token": normalized,
            "match_strategy": "unresolved",
            "alternatives": [{"field": field, "confidence": 0.88} for field in unique_fields[:3]],
            "resolved": False,
            "normalized": normalized,
        }

    fuzz, process = _load_rapidfuzz()
    sample_values_list = [item for item in (sample_values or [])]
    if effective_mode == "auto" and fuzz is not None and process is not None:
        choice_best: dict[str, dict[str, Any]] = {}
        left = _token_display(normalized)
        for field, candidates in aliases.items():
            for candidate in [field, *candidates]:
                candidate_token = _normalize_token(candidate)
                if not candidate_token:
                    continue
                right = _token_display(candidate_token)
                base_score = (
                    float(fuzz.token_set_ratio(left, right)) * 0.5
                    + float(fuzz.token_sort_ratio(left, right)) * 0.3
                    + float(fuzz.partial_ratio(left, right)) * 0.2
                )
                alias_bonus = 6.0 if candidate_token != _normalize_token(field) else 0.0
                required_bonus = 4.0 if field in required_fields else 0.0
                affinity_bonus, affinity_rate = _header_value_affinity(
                    field,
                    sample_values_list,
                    raw_header=str(name or ""),
                    profile_name=profile_name,
                    value_affinity_available=bool(runtime.get("value_affinity_available")),
                )
                score = min(100.0, base_score + alias_bonus + required_bonus + (affinity_bonus * 100.0))
                strategy = "fuzzy+value_affinity" if affinity_bonus > 0.0 else "fuzzy"
                existing = choice_best.get(field)
                if existing is None or score > float(existing.get("score", 0.0)):
                    choice_best[field] = {
                        "field": field,
                        "score": score,
                        "matched_token": candidate_token,
                        "match_strategy": strategy,
                        "affinity_rate": affinity_rate,
                    }
        alternatives = sorted(choice_best.values(), key=lambda item: (-float(item["score"]), item["field"]))
        alt_payload = [
            {"field": str(item["field"]), "confidence": round(float(item["score"]) / 100.0, 6)}
            for item in alternatives[:3]
        ]
        if alternatives:
            best = alternatives[0]
            second_score = float(alternatives[1]["score"]) if len(alternatives) > 1 else 0.0
            best_score = float(best["score"])
            if best_score >= 82.0 and (best_score - second_score) >= 6.0:
                return {
                    "canonical_field": str(best["field"]),
                    "confidence": round(best_score / 100.0, 6),
                    "matched_token": str(best.get("matched_token") or ""),
                    "match_strategy": str(best.get("match_strategy") or "fuzzy"),
                    "alternatives": alt_payload,
                    "resolved": True,
                    "normalized": normalized,
                }
        return {
            "canonical_field": "",
            "confidence": 0.55,
            "matched_token": normalized,
            "match_strategy": "unresolved",
            "alternatives": alt_payload,
            "resolved": False,
            "normalized": normalized,
        }

    if fuzz is not None and process is not None:
        choices: list[tuple[str, str]] = []
        for field, candidates in aliases.items():
            choices.append((field, _normalize_token(field)))
            choices.extend((field, _normalize_token(item)) for item in candidates)
        search_space = [item[1] for item in choices if item[1]]
        if search_space:
            matched = process.extractOne(normalized, search_space, scorer=fuzz.ratio)
            if matched:
                matched_value = str(matched[0] or "")
                matched_score = float(matched[1] or 0.0)
                if matched_score >= 85:
                    for field, candidate in choices:
                        if candidate == matched_value:
                            return {
                                "canonical_field": field,
                                "confidence": round(matched_score / 100.0, 6),
                                "matched_token": matched_value,
                                "match_strategy": "fuzzy",
                                "alternatives": [{"field": field, "confidence": round(matched_score / 100.0, 6)}],
                                "resolved": True,
                                "normalized": normalized,
                            }

    return {
        "canonical_field": "",
        "confidence": 0.55,
        "matched_token": normalized,
        "match_strategy": "unresolved",
        "alternatives": [],
        "resolved": False,
        "normalized": normalized,
    }


def canonicalize_header(
    name: str,
    spec: Optional[Mapping[str, Any]] = None,
    *,
    sample_values: Optional[Sequence[Any]] = None,
) -> tuple[str, float, str]:
    details = analyze_header_mapping(name, spec, sample_values=sample_values)
    if details.get("resolved"):
        return (
            str(details.get("canonical_field") or ""),
            float(details.get("confidence") or 0.0),
            str(details.get("matched_token") or ""),
        )
    normalized = str(details.get("normalized") or "")
    return (
        normalized,
        float(details.get("confidence") or 0.55),
        str(details.get("matched_token") or normalized),
    )


def normalize_value_for_field(value: Any, field_name: str, *, raw_header: Any = None) -> Any:
    field = _normalize_token(field_name)
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "amount" in field or "amt" in field or "score" in field or field in {"debit_amount", "credit_amount", "balance"}:
            parsed_amount = _parse_amount_value(value, raw_header=raw_header)
            return parsed_amount if parsed_amount is not None else float(value)
        if field == "id" or field.endswith("_id"):
            parsed_id = _parse_int_like(value)
            return parsed_id if parsed_id is not None else int(value)
        if "date" in field or field.endswith("_at") or field == "txn_date":
            parsed_date = _parse_date_value(value)
            return parsed_date if parsed_date is not None else str(value)
        return value
    text = _normalize_display_text(value)
    if not text:
        return ""

    if "amount" in field or "amt" in field or "score" in field or field in {"debit_amount", "credit_amount", "balance"}:
        parsed_amount = _parse_amount_value(text, raw_header=raw_header)
        return parsed_amount if parsed_amount is not None else text
    if field == "id" or field.endswith("_id"):
        parsed_id = _parse_int_like(text)
        return parsed_id if parsed_id is not None else text
    if "date" in field or field.endswith("_at") or field == "txn_date":
        parsed_date = _parse_date_value(text)
        return parsed_date if parsed_date is not None else text
    if "phone" in field or "mobile" in field or "tel" in field:
        parsed_phone = _parse_phone_value(text)
        return parsed_phone if parsed_phone is not None else text
    return text


def _apply_required_field_checks(rows: Sequence[Mapping[str, Any]], required_fields: Sequence[str]) -> tuple[dict[str, int], float]:
    required_missing: dict[str, int] = {}
    missing_cells = 0
    for field in required_fields:
        missing = 0
        for row in rows:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                missing += 1
        required_missing[str(field)] = missing
        missing_cells += missing
    ratio = _safe_ratio(missing_cells, max(1, len(rows) * max(1, len(required_fields))))
    return required_missing, ratio


def _validate_with_pandera(rows: Sequence[Mapping[str, Any]], required_fields: Sequence[str], spec: Mapping[str, Any]) -> list[str]:
    pd, pa = _load_pandera()
    if pd is None or pa is None or not required_fields:
        return []
    try:
        profile_name = resolve_canonical_profile(spec)
        profile = _PROFILE_SPECS.get(profile_name, {})
        columns: dict[str, Any] = {}
        for field in required_fields:
            columns[str(field)] = pa.Column(
                object,
                required=True,
                nullable=False,
                checks=pa.Check(
                    lambda s: s.map(lambda v: str(v).strip() != "" if v is not None else False),
                    element_wise=False,
                ),
            )
        for field in profile.get("numeric_fields") or []:
            columns[str(field)] = pa.Column(
                object,
                required=False,
                nullable=True,
                checks=pa.Check(
                    lambda s, field=field: s.map(
                        lambda v: (
                            normalize_value_for_field(v, field) not in {None, ""}
                            if v not in {None, ""}
                            else True
                        )
                    )
                ),
            )
        for field in profile.get("date_fields") or []:
            columns[str(field)] = pa.Column(
                object,
                required=False,
                nullable=True,
                checks=pa.Check(
                    lambda s, field=field: s.map(
                        lambda v: bool(normalize_value_for_field(v, field)) if v not in {None, ""} else True
                    )
                ),
            )
        schema = pa.DataFrameSchema(columns, strict=False, coerce=False)
        frame = pd.DataFrame(list(rows))
        for field in columns:
            if field not in frame.columns:
                frame[field] = None
            frame[field] = frame[field].astype(object)
        schema.validate(frame, lazy=True)
        return []
    except Exception as exc:
        return [f"pandera validation failed: {exc}"]


def build_image_quality_report(
    rows: Sequence[Mapping[str, Any]],
    image_blocks: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    rules = quality_rules_from_spec(spec, "image")
    errors: list[str] = []
    required_fields = rules.get("required_fields")
    if not isinstance(required_fields, list):
        profile_name = resolve_canonical_profile(spec)
        required_fields = list(_PROFILE_SPECS.get(profile_name, {}).get("required_fields") or [])
    blocks = list(image_blocks or [])
    text_blocks = [block for block in blocks if str(block.get("block_type") or "text") != "figure"]
    confidences = [_to_float(block.get("confidence")) for block in text_blocks]
    confidences = [float(item) for item in confidences if item is not None]
    texts = [_normalize_text_body(block.get("text")) for block in text_blocks if str(block.get("text") or "").strip()]

    avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    low_conf_ratio = _safe_ratio(sum(1 for item in confidences if item < 0.6), len(confidences))
    duplicate_line_ratio = 1.0 - _safe_ratio(len(set(texts)), len(texts)) if texts else 0.0
    empty_text_ratio = _safe_ratio(sum(1 for block in text_blocks if not str(block.get("text") or "").strip()), len(text_blocks))
    non_text_symbol_ratio = _safe_ratio(sum(_symbol_ratio(item) for item in texts), len(texts))

    avg_conf_min = _to_float(rules.get("ocr_confidence_avg_min"))
    if avg_conf_min is not None and avg_confidence < avg_conf_min:
        errors.append(f"ocr_confidence_avg={avg_confidence:.4f} below ocr_confidence_avg_min={avg_conf_min:.4f}")
    low_conf_max = _to_float(rules.get("low_confidence_block_ratio_max"))
    if low_conf_max is not None and low_conf_ratio > low_conf_max:
        errors.append(
            f"low_confidence_block_ratio={low_conf_ratio:.4f} exceeds low_confidence_block_ratio_max={low_conf_max:.4f}"
        )
    symbol_max = _to_float(rules.get("non_text_symbol_ratio_max"))
    if symbol_max is not None and non_text_symbol_ratio > symbol_max:
        errors.append(f"non_text_symbol_ratio={non_text_symbol_ratio:.4f} exceeds non_text_symbol_ratio_max={symbol_max:.4f}")
    duplicate_max = _to_float(rules.get("duplicate_line_ratio_max"))
    if duplicate_max is not None and duplicate_line_ratio > duplicate_max:
        errors.append(f"duplicate_line_ratio={duplicate_line_ratio:.4f} exceeds duplicate_line_ratio_max={duplicate_max:.4f}")
    empty_max = _to_float(rules.get("empty_text_block_ratio_max"))
    if empty_max is not None and empty_text_ratio > empty_max:
        errors.append(f"empty_text_block_ratio={empty_text_ratio:.4f} exceeds empty_text_block_ratio_max={empty_max:.4f}")
    if not rows:
        errors.append("image extraction produced no rows")
    if isinstance(required_fields, list) and required_fields:
        required_missing, required_missing_ratio = _apply_required_field_checks(rows, [str(item) for item in required_fields])
        max_required_missing_ratio = _to_float(rules.get("max_required_missing_ratio"))
        if max_required_missing_ratio is not None and required_missing_ratio > max_required_missing_ratio:
            errors.append(
                f"required_missing_ratio={required_missing_ratio:.4f} exceeds max_required_missing_ratio={max_required_missing_ratio:.4f}"
            )
        errors.extend(_validate_with_pandera(rows, [str(item) for item in required_fields], spec))
    else:
        required_missing = {}
        required_missing_ratio = 0.0

    return {
        "modality": "image",
        "ok": len(errors) == 0,
        "blocked": len(errors) > 0,
        "errors": errors,
        "metrics": {
            "block_count": len(blocks),
            "text_block_count": len(text_blocks),
            "ocr_confidence_avg": round(avg_confidence, 6),
            "low_confidence_block_ratio": round(low_conf_ratio, 6),
            "duplicate_line_ratio": round(duplicate_line_ratio, 6),
            "empty_text_block_ratio": round(empty_text_ratio, 6),
            "non_text_symbol_ratio": round(non_text_symbol_ratio, 6),
            "required_field_missing": required_missing,
            "required_missing_ratio": round(required_missing_ratio, 6),
        },
    }


def build_xlsx_quality_report(
    rows: Sequence[Mapping[str, Any]],
    sheet_frames: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    rules = quality_rules_from_spec(spec, "xlsx")
    errors: list[str] = []
    frames = list(sheet_frames or [])
    required_columns_raw = rules.get("required_columns")
    if not isinstance(required_columns_raw, list):
        required_columns_raw = rules.get("required_fields")
    if not isinstance(required_columns_raw, list):
        required_columns_raw = []
    required_columns = [str(item) for item in required_columns_raw if str(item).strip()]
    if not required_columns:
        shared_required = rules.get("required_fields")
        if isinstance(shared_required, list):
            required_columns = [str(item) for item in shared_required if str(item).strip()]

    present_columns = set()
    for frame in frames:
        for column in frame.get("columns") or []:
            present_columns.add(str(column))
    missing_columns = [field for field in required_columns if field not in present_columns]
    if missing_columns:
        errors.append(f"required_columns missing: {', '.join(missing_columns)}")

    header_confidences = [_to_float(frame.get("header_confidence")) for frame in frames]
    header_confidences = [float(item) for item in header_confidences if item is not None]
    avg_header_confidence = sum(header_confidences) / len(header_confidences) if header_confidences else 0.0

    numeric_parse_rate = _safe_ratio(
        sum(_to_int(frame.get("numeric_cells_parsed")) or 0 for frame in frames),
        sum(_to_int(frame.get("numeric_cells_total")) or 0 for frame in frames),
    )
    date_parse_rate = _safe_ratio(
        sum(_to_int(frame.get("date_cells_parsed")) or 0 for frame in frames),
        sum(_to_int(frame.get("date_cells_total")) or 0 for frame in frames),
    )
    blank_row_ratio = _safe_ratio(
        sum(_to_int(frame.get("blank_rows")) or 0 for frame in frames),
        sum((_to_int(frame.get("blank_rows")) or 0) + (_to_int(frame.get("row_count")) or 0) for frame in frames),
    )

    duplicate_key_ratio = 0.0
    unique_key_fields = rules.get("unique_keys")
    if not isinstance(unique_key_fields, list):
        unique_key_fields = rules.get("deduplicate_by")
    if isinstance(unique_key_fields, list) and unique_key_fields:
        key_fields = [str(item) for item in unique_key_fields if str(item).strip()]
        keys = [tuple(row.get(field) for field in key_fields) for row in rows]
        filtered_keys = [item for item in keys if any(part not in {None, ""} for part in item)]
        if filtered_keys:
            duplicate_key_ratio = 1.0 - _safe_ratio(len(set(filtered_keys)), len(filtered_keys))

    required_missing, required_missing_ratio = _apply_required_field_checks(rows, required_columns)

    header_confidence_min = _to_float(rules.get("header_confidence_min"))
    if header_confidence_min is not None and avg_header_confidence < header_confidence_min:
        errors.append(
            f"header_confidence={avg_header_confidence:.4f} below header_confidence_min={header_confidence_min:.4f}"
        )
    numeric_rate_min = _to_float(rules.get("numeric_parse_rate_min"))
    if numeric_rate_min is not None and numeric_parse_rate < numeric_rate_min:
        errors.append(f"numeric_parse_rate={numeric_parse_rate:.4f} below numeric_parse_rate_min={numeric_rate_min:.4f}")
    date_rate_min = _to_float(rules.get("date_parse_rate_min"))
    if date_rate_min is not None and date_parse_rate < date_rate_min:
        errors.append(f"date_parse_rate={date_parse_rate:.4f} below date_parse_rate_min={date_rate_min:.4f}")
    duplicate_key_ratio_max = _to_float(rules.get("duplicate_key_ratio_max"))
    if duplicate_key_ratio_max is not None and duplicate_key_ratio > duplicate_key_ratio_max:
        errors.append(
            f"duplicate_key_ratio={duplicate_key_ratio:.4f} exceeds duplicate_key_ratio_max={duplicate_key_ratio_max:.4f}"
        )
    blank_row_ratio_max = _to_float(rules.get("blank_row_ratio_max"))
    if blank_row_ratio_max is not None and blank_row_ratio > blank_row_ratio_max:
        errors.append(f"blank_row_ratio={blank_row_ratio:.4f} exceeds blank_row_ratio_max={blank_row_ratio_max:.4f}")
    sheet_row_count_min = _to_int(rules.get("sheet_row_count_min"))
    if sheet_row_count_min is not None:
        for frame in frames:
            if (_to_int(frame.get("row_count")) or 0) < sheet_row_count_min:
                errors.append(
                    f"sheet_row_count[{frame.get('sheet_name')}]={_to_int(frame.get('row_count')) or 0} below sheet_row_count_min={sheet_row_count_min}"
                )
    coverage_min = _to_float(rules.get("cross_sheet_required_coverage_min"))
    coverage_ratio = 1.0 - required_missing_ratio
    if coverage_min is not None and coverage_ratio < coverage_min:
        errors.append(
            f"cross_sheet_required_coverage={coverage_ratio:.4f} below cross_sheet_required_coverage_min={coverage_min:.4f}"
        )
    if not rows:
        errors.append("xlsx extraction produced no rows")
    errors.extend(_validate_with_pandera(rows, required_columns, spec))

    return {
        "modality": "xlsx",
        "ok": len(errors) == 0,
        "blocked": len(errors) > 0,
        "errors": errors,
        "metrics": {
            "sheet_count": len(frames),
            "row_count": len(rows),
            "header_confidence": round(avg_header_confidence, 6),
            "numeric_parse_rate": round(numeric_parse_rate, 6),
            "date_parse_rate": round(date_parse_rate, 6),
            "duplicate_key_ratio": round(duplicate_key_ratio, 6),
            "blank_row_ratio": round(blank_row_ratio, 6),
            "cross_sheet_required_coverage": round(coverage_ratio, 6),
            "required_field_missing": required_missing,
            "required_missing_ratio": round(required_missing_ratio, 6),
        },
    }


def enforce_quality_contract(report: Mapping[str, Any]) -> None:
    if bool(report.get("ok", False)):
        return
    errors = [str(item) for item in (report.get("errors") or []) if str(item).strip()]
    if not errors:
        errors = [f"{report.get('modality') or 'input'} quality blocked"]
    raise RuntimeError("; ".join(errors))

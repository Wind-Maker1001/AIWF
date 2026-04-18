from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Mapping

from aiwf.cleaning_spec_v2 import resolve_canonical_profile_name
from aiwf.quality_contract import normalize_value_for_field


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _normalize_account_no(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = re.sub(r"[^0-9A-Za-z]", "", text).upper()
    return normalized or text


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def _normalize_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _semantic_profile_is_bank(
    params_effective: Mapping[str, Any],
    profile_analysis: Mapping[str, Any] | None = None,
) -> bool:
    quality_rules = _as_dict(params_effective.get("quality_rules"))
    template_meta = _as_dict(params_effective.get("_resolved_cleaning_template"))
    for candidate in [
        params_effective.get("canonical_profile"),
        params_effective.get("template_expected_profile"),
        quality_rules.get("canonical_profile"),
        template_meta.get("template_expected_profile"),
        template_meta.get("canonical_profile"),
        _as_dict(profile_analysis).get("requested_profile"),
        _as_dict(profile_analysis).get("recommended_profile"),
    ]:
        if resolve_canonical_profile_name(candidate) == "bank_statement":
            return True
    return False


def semantic_rules_from_params(
    params_effective: Mapping[str, Any],
    *,
    profile_analysis: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    quality_rules = _as_dict(params_effective.get("quality_rules"))
    spec_quality = _as_dict(_as_dict(params_effective.get("cleaning_spec_v2")).get("quality"))
    source = _as_dict(quality_rules.get("advanced_rules"))
    if not source:
        source = _as_dict(spec_quality.get("advanced_rules"))
    semantic_cfg = _as_dict(source.get("bank_statement_semantics"))
    if not semantic_cfg and not _semantic_profile_is_bank(params_effective, profile_analysis):
        return {}
    return {
        "enabled": True,
        "signed_amount_conflict_tolerance": abs(float(semantic_cfg.get("signed_amount_conflict_tolerance", 0.01) or 0.01)),
        "balance_continuity_tolerance": abs(float(semantic_cfg.get("balance_continuity_tolerance", 0.05) or 0.05)),
        "block_on_semantic_conflicts": bool(semantic_cfg.get("block_on_semantic_conflicts", False)),
        "direction_field": str(semantic_cfg.get("direction_field") or "txn_type").strip() or "txn_type",
        "debit_tokens": [
            str(item).strip()
            for item in (semantic_cfg.get("debit_tokens") or ["借", "借方", "debit", "dr", "out", "支出"])
            if str(item).strip()
        ],
        "credit_tokens": [
            str(item).strip()
            for item in (semantic_cfg.get("credit_tokens") or ["贷", "贷方", "credit", "cr", "in", "收入"])
            if str(item).strip()
        ],
    }


def _resolve_direction_sign(direction: Any, cfg: Mapping[str, Any]) -> float | None:
    text = _normalize_token(direction)
    if not text:
        return None
    debit_tokens = [_normalize_token(item) for item in _as_list(cfg.get("debit_tokens")) if _normalize_token(item)]
    credit_tokens = [_normalize_token(item) for item in _as_list(cfg.get("credit_tokens")) if _normalize_token(item)]
    if any(token and token in text for token in debit_tokens):
        return -1.0
    if any(token and token in text for token in credit_tokens):
        return 1.0
    return None


def _normalize_row_for_semantics(row: Mapping[str, Any], rename_map: Mapping[str, Any]) -> Dict[str, Any]:
    source = dict(row or {})
    normalized_source = dict(source)
    for old_key, new_key in rename_map.items():
        old_text = str(old_key or "").strip()
        new_text = str(new_key or "").strip()
        if not old_text or not new_text:
            continue
        if old_text in normalized_source and new_text not in normalized_source:
            normalized_source[new_text] = normalized_source.get(old_text)
    out = {
        "account_no": _normalize_account_no(normalized_source.get("account_no")),
        "txn_date": normalize_value_for_field(normalized_source.get("txn_date"), "txn_date"),
        "debit_amount": normalize_value_for_field(normalized_source.get("debit_amount"), "debit_amount"),
        "credit_amount": normalize_value_for_field(normalized_source.get("credit_amount"), "credit_amount"),
        "amount": normalize_value_for_field(normalized_source.get("amount"), "amount"),
        "balance": normalize_value_for_field(normalized_source.get("balance"), "balance"),
        "txn_type": str(normalized_source.get("txn_type") or normalized_source.get("direction") or "").strip(),
        "ref_no": str(normalized_source.get("ref_no") or "").strip(),
        "counterparty_name": str(normalized_source.get("counterparty_name") or "").strip(),
        "row_index": normalized_source.get("row_index", normalized_source.get("_row_index")),
        "sheet_name": str(normalized_source.get("sheet_name") or "").strip(),
    }
    return out


def _expected_signed_amount(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> float | None:
    debit = _to_float(row.get("debit_amount"))
    credit = _to_float(row.get("credit_amount"))
    reported = _to_float(row.get("amount"))
    if debit is not None or credit is not None:
        return float(credit or 0.0) - float(debit or 0.0)
    if reported is None:
        return None
    sign = _resolve_direction_sign(row.get(str(cfg.get("direction_field") or "txn_type")), cfg)
    if sign is None:
        return reported
    return abs(reported) * sign


def evaluate_bank_statement_semantics(
    *,
    rows: List[Dict[str, Any]],
    params_effective: Mapping[str, Any],
    profile_analysis: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = semantic_rules_from_params(params_effective, profile_analysis=profile_analysis)
    if not cfg:
        return {
            "enabled": False,
            "passed": True,
            "blocked": False,
            "report_only": True,
            "items": [],
            "summary": {"conflict_count": 0, "counts": {}},
            "blocking_reason_codes": [],
            "rules": {},
        }
    rules = _as_dict(params_effective.get("rules"))
    rename_map = _as_dict(rules.get("rename_map"))
    normalized_rows = [_normalize_row_for_semantics(dict(item or {}), rename_map) for item in rows if isinstance(item, dict)]
    items: List[Dict[str, Any]] = []
    counts = {"signed_amount_conflict": 0, "balance_gap": 0}
    signed_tolerance = float(cfg["signed_amount_conflict_tolerance"])
    balance_tolerance = float(cfg["balance_continuity_tolerance"])

    previous_balance_by_account: Dict[str, float] = {}
    for ordinal, row in enumerate(normalized_rows, start=1):
        account_key = str(row.get("account_no") or "__global__")
        row_index = int(row.get("row_index") or ordinal)
        reported_amount = _to_float(row.get("amount"))
        expected_amount = _expected_signed_amount(row, cfg)
        if reported_amount is not None and expected_amount is not None:
            delta = abs(reported_amount - expected_amount)
            if delta > signed_tolerance:
                counts["signed_amount_conflict"] += 1
                items.append(
                    {
                        "kind": "signed_amount_conflict",
                        "row_index": row_index,
                        "account_no": str(row.get("account_no") or ""),
                        "txn_date": str(row.get("txn_date") or ""),
                        "reported_amount": reported_amount,
                        "expected_amount": expected_amount,
                        "delta": round(delta, 6),
                        "tolerance": signed_tolerance,
                        "message": "reported amount conflicts with debit/credit or direction semantics",
                    }
                )
        current_balance = _to_float(row.get("balance"))
        effective_amount = expected_amount if expected_amount is not None else reported_amount
        previous_balance = previous_balance_by_account.get(account_key)
        if previous_balance is not None and current_balance is not None and effective_amount is not None:
            expected_balance = previous_balance + effective_amount
            delta = abs(current_balance - expected_balance)
            if delta > balance_tolerance:
                counts["balance_gap"] += 1
                items.append(
                    {
                        "kind": "balance_gap",
                        "row_index": row_index,
                        "account_no": str(row.get("account_no") or ""),
                        "txn_date": str(row.get("txn_date") or ""),
                        "previous_balance": previous_balance,
                        "current_balance": current_balance,
                        "expected_balance": round(expected_balance, 6),
                        "amount": effective_amount,
                        "delta": round(delta, 6),
                        "tolerance": balance_tolerance,
                        "message": "balance continuity check failed against previous row",
                    }
                )
        if current_balance is not None:
            previous_balance_by_account[account_key] = current_balance

    passed = len(items) == 0
    block_on_conflicts = bool(cfg["block_on_semantic_conflicts"])
    blocking_reason_codes = sorted({str(item.get("kind") or "").strip() for item in items if str(item.get("kind") or "").strip()})
    return {
        "enabled": True,
        "passed": passed,
        "blocked": block_on_conflicts and not passed,
        "report_only": not block_on_conflicts,
        "items": items,
        "summary": {
            "conflict_count": len(items),
            "counts": counts,
        },
        "blocking_reason_codes": blocking_reason_codes,
        "rules": cfg,
    }

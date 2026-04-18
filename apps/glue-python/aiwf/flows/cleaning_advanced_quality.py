from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping

from aiwf.accel_client import quality_check_v4_operator
from aiwf.flows.cleaning_bank_semantics import evaluate_bank_statement_semantics


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        parsed = float(text)
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def _collect_numeric_values(rows: List[Dict[str, Any]], field: str) -> List[float]:
    values: List[float] = []
    for row in rows:
        parsed = _to_float(row.get(field))
        if parsed is not None:
            values.append(parsed)
    return values


def _quantile(sorted_values: List[float], ratio: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = ratio * (len(sorted_values) - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return sorted_values[lower]
    weight = pos - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _normalize_advanced_rules(params_effective: Mapping[str, Any]) -> Dict[str, Any]:
    quality_rules = _as_dict(params_effective.get("quality_rules"))
    spec_quality = _as_dict(_as_dict(params_effective.get("cleaning_spec_v2")).get("quality"))
    source = _as_dict(quality_rules.get("advanced_rules"))
    if not source:
        source = _as_dict(spec_quality.get("advanced_rules"))
    out: Dict[str, Any] = {}
    outlier = source.get("outlier_zscore")
    if isinstance(outlier, dict):
        out["outlier_zscore"] = dict(outlier)
    elif isinstance(outlier, list):
        outliers = [dict(item) for item in outlier if isinstance(item, dict)]
        if outliers:
            out["outlier_zscore"] = outliers
    anomaly = source.get("anomaly_iqr")
    if isinstance(anomaly, dict):
        out["anomaly_iqr"] = [dict(anomaly)]
    elif isinstance(anomaly, list):
        out["anomaly_iqr"] = [dict(item) for item in anomaly if isinstance(item, dict)]
    semantic = source.get("bank_statement_semantics")
    if isinstance(semantic, dict):
        out["bank_statement_semantics"] = dict(semantic)
    out["block_on_advanced_rules"] = bool(source.get("block_on_advanced_rules", False))
    return out


def _build_fallback_report(rows: List[Dict[str, Any]], rules: Dict[str, Any]) -> Dict[str, Any]:
    violations: List[Dict[str, Any]] = []
    raw_outlier = rules.get("outlier_zscore")
    outlier_cfgs = (
        [dict(raw_outlier)]
        if isinstance(raw_outlier, dict)
        else [dict(item) for item in _as_list(raw_outlier) if isinstance(item, dict)]
    )
    outlier_details: List[Dict[str, Any]] = []
    for outlier_cfg in outlier_cfgs:
        field = str(outlier_cfg.get("field") or "").strip()
        max_z = abs(float(outlier_cfg.get("max_z", 4.0) or 4.0))
        values = _collect_numeric_values(rows, field)
        if field and len(values) >= 3:
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / len(values)
            stddev = math.sqrt(variance)
            if stddev > 0:
                outliers = sum(1 for value in values if abs((value - mean) / stddev) > max_z)
                if outliers > 0:
                    outlier_details.append({"field": field, "outliers": outliers, "max_z": max_z})
    if outlier_details:
        violations.append({"rule": "outlier_zscore", "details": outlier_details})
    anomaly_cfg = _as_list(rules.get("anomaly_iqr"))
    if anomaly_cfg:
        details: List[Dict[str, Any]] = []
        for item in anomaly_cfg:
            cfg = _as_dict(item)
            field = str(cfg.get("field") or "").strip()
            max_ratio = float(cfg.get("max_ratio", 0.10) or 0.10)
            values = sorted(_collect_numeric_values(rows, field))
            if not field or len(values) < 4:
                continue
            q1 = _quantile(values, 0.25)
            q3 = _quantile(values, 0.75)
            if q1 is None or q3 is None:
                continue
            iqr = max(0.0, q3 - q1)
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            outliers = sum(1 for value in values if value < lower or value > upper)
            ratio = (outliers / len(values)) if values else 0.0
            if ratio > max_ratio:
                details.append(
                    {
                        "field": field,
                        "q1": q1,
                        "q3": q3,
                        "iqr": iqr,
                        "lower": lower,
                        "upper": upper,
                        "outliers": outliers,
                        "ratio": ratio,
                        "max_ratio": max_ratio,
                    }
                )
        if details:
            violations.append({"rule": "anomaly_iqr", "details": details})
    return {
        "rows": len(rows),
        "violations": violations,
        "rule_count": len([key for key in ("outlier_zscore", "anomaly_iqr") if rules.get(key)]),
    }


def evaluate_advanced_quality(
    *,
    rows: List[Dict[str, Any]],
    params_effective: Mapping[str, Any],
    semantic_rows: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    advanced_rules = _normalize_advanced_rules(params_effective)
    semantic_checks = evaluate_bank_statement_semantics(
        rows=list(semantic_rows or rows),
        params_effective=params_effective,
    )
    operator_rules = {
        key: value
        for key, value in advanced_rules.items()
        if key in {"outlier_zscore", "anomaly_iqr"} and value
    }
    if not operator_rules and not semantic_checks.get("enabled"):
        return {
            "enabled": False,
            "operator": "none",
            "report_only": True,
            "blocked": False,
            "passed": True,
            "report": {"rows": len(rows), "violations": [], "rule_count": 0},
            "rules": {},
        }

    if operator_rules:
        result = quality_check_v4_operator(
            rows=rows,
            params=dict(params_effective),
            rules=operator_rules,
            metrics={},
        )
        if result.get("ok"):
            report = dict(result.get("report") or {})
            passed = bool(result.get("passed", True))
            operator = "quality_check_v4"
            fallback_used = False
        else:
            report = _build_fallback_report(rows, operator_rules)
            passed = not bool(report.get("violations"))
            operator = "python_fallback"
            fallback_used = True
    else:
        result = {"ok": True}
        report = {"rows": len(rows), "violations": [], "rule_count": 0}
        passed = True
        operator = "none"
        fallback_used = False

    block_on_advanced_rules = bool(advanced_rules.get("block_on_advanced_rules", False))
    semantic_summary = dict(semantic_checks.get("summary") or {})
    semantic_items = [dict(item) for item in semantic_checks.get("items") or [] if isinstance(item, dict)]
    if semantic_checks.get("enabled"):
        report = dict(report)
        violations = list(report.get("violations") or [])
        if semantic_items:
            violations.append(
                {
                    "rule": "bank_statement_semantics",
                    "details": semantic_items,
                    "summary": semantic_summary,
                }
            )
        report["violations"] = violations
        report["bank_statement_semantics"] = {
            "summary": semantic_summary,
            "items": semantic_items,
            "block_on_semantic_conflicts": bool(semantic_checks.get("rules", {}).get("block_on_semantic_conflicts", False)),
        }
        report["rule_count"] = int(report.get("rule_count", 0) or 0) + 1
        passed = passed and bool(semantic_checks.get("passed", True))
    blocked = (block_on_advanced_rules and not passed) or bool(semantic_checks.get("blocked", False))
    report_only = not block_on_advanced_rules and not bool(semantic_checks.get("rules", {}).get("block_on_semantic_conflicts", False))
    return {
        "enabled": True,
        "operator": operator,
        "report_only": report_only,
        "blocked": blocked,
        "passed": passed,
        "report": report,
        "rules": {**operator_rules, **({"bank_statement_semantics": dict(advanced_rules.get("bank_statement_semantics") or {})} if advanced_rules.get("bank_statement_semantics") else {})},
        "semantic_checks": semantic_checks,
        "fallback_used": fallback_used,
        "error": "" if result.get("ok") else str(result.get("error") or ""),
    }

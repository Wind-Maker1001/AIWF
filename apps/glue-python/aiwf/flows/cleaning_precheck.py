from __future__ import annotations

from typing import Any, Dict, List

from aiwf.cleaning_spec_v2 import recommended_template_id_for_profile, resolve_canonical_profile_name
from aiwf.flows.cleaning import _clean_rows, _prepare_cleaning_params, _rule_param, _to_bool
from aiwf.flows.cleaning_review_support import build_review_analysis


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _as_reason_codes(value: Any) -> List[str]:
    return sorted({str(item).strip() for item in _as_list(value) if str(item).strip()})


def _recommended_candidate(candidate_profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
    return next(
        (
            item
            for item in candidate_profiles
            if isinstance(item, dict) and bool(item.get("recommended")) and str(item.get("profile") or "").strip()
        ),
        {},
    )


def _candidate_for_profile(candidate_profiles: List[Dict[str, Any]], profile: str) -> Dict[str, Any]:
    normalized = str(profile or "").strip().lower()
    if not normalized:
        return {}
    return next(
        (
            dict(item)
            for item in candidate_profiles
            if isinstance(item, dict) and str(item.get("profile") or "").strip().lower() == normalized
        ),
        {},
    )


def _precheck_action(
    *,
    recommendation_available: bool,
    quality_blocked: bool,
    profile_blocked: bool,
    predicted_zero_output_unexpected: bool,
    review_required: bool,
) -> str:
    if quality_blocked or profile_blocked or predicted_zero_output_unexpected:
        return "block"
    if review_required or not recommendation_available:
        return "warn"
    return "allow"


def _build_issues_and_suggestions(
    *,
    requested_profile: str,
    recommended_profile: str,
    recommended_template_id: str,
    recommendation_available: bool,
    profile_mismatch: bool,
    predicted_zero_output_unexpected: bool,
    blocking_reason_codes: List[str],
) -> tuple[List[str], List[str]]:
    issues: List[str] = []
    suggestions: List[str] = []

    if profile_mismatch:
        issues.append(
            "template profile mismatch: "
            f"requested={requested_profile or '<none>'}, recommended={recommended_profile or '<none>'}"
        )
    if predicted_zero_output_unexpected:
        issues.append("predicted cleaned output will be empty while blank output is not expected")
    if not recommendation_available:
        issues.append("recommendation signal unavailable; runtime guardrails may still block execution")
    if blocking_reason_codes:
        issues.append("blocking reason codes: " + ", ".join(blocking_reason_codes))

    if recommended_template_id:
        suggestions.append(f"recommended_template_id={recommended_template_id}")
    if profile_mismatch:
        suggestions.append("align cleaning_template or canonical_profile with the recommended profile")
    if predicted_zero_output_unexpected:
        suggestions.append("review required fields, filters, and blank_output_expected before running")

    return issues, suggestions


def run_cleaning_precheck(*, params: Dict[str, Any], extract_payload: Dict[str, Any]) -> Dict[str, Any]:
    params_effective = _prepare_cleaning_params(dict(params or {}))
    rows = [dict(item) for item in _as_list(extract_payload.get("rows")) if isinstance(item, dict)]
    header_mapping = [dict(item) for item in _as_list(extract_payload.get("header_mapping")) if isinstance(item, dict)]
    candidate_profiles = [dict(item) for item in _as_list(extract_payload.get("candidate_profiles")) if isinstance(item, dict)]
    quality_decisions = [dict(item) for item in _as_list(extract_payload.get("quality_decisions")) if isinstance(item, dict)]
    sample_rows = [dict(item) for item in _as_list(extract_payload.get("sample_rows")) if isinstance(item, dict)]

    requested_profile = resolve_canonical_profile_name(
        params_effective.get("template_expected_profile")
        or params_effective.get("canonical_profile")
        or _as_dict(params_effective.get("quality_rules")).get("canonical_profile")
    )
    blank_output_expected = _to_bool(params_effective.get("blank_output_expected"), default=False)
    recommended_candidate = _recommended_candidate(candidate_profiles)
    requested_candidate = _candidate_for_profile(candidate_profiles, requested_profile)

    recommended_profile = str(recommended_candidate.get("profile") or "").strip().lower()
    recommended_template_id = str(
        recommended_candidate.get("recommended_template_id")
        or recommended_template_id_for_profile(recommended_profile)
        or ""
    ).strip()
    profile_confidence = round(float(recommended_candidate.get("score") or 0.0), 6)
    requested_coverage = round(float(requested_candidate.get("required_coverage") or 0.0), 6)
    recommended_coverage = round(float(recommended_candidate.get("required_coverage") or 0.0), 6)
    required_field_coverage = requested_coverage if requested_profile else recommended_coverage
    profile_mismatch = bool(requested_profile and recommended_profile and requested_profile != recommended_profile)
    mismatch_action = str(params_effective.get("profile_mismatch_action") or "warn").strip().lower()
    if mismatch_action not in {"warn", "block"}:
        mismatch_action = "warn"

    recommendation_available = bool(recommended_profile)
    profile_blocked = bool(
        recommendation_available
        and mismatch_action == "block"
        and profile_mismatch
        and profile_confidence >= 0.85
        and recommended_coverage >= 0.75
        and requested_coverage <= 0.25
    )

    predicted_zero_output_unexpected = False
    predicted_quality: Dict[str, Any] = {}
    predicted_execution_audit: Dict[str, Any] = {}
    if rows:
        prediction_params = dict(params_effective)
        prediction_params["profile_mismatch_action"] = "warn"
        cleaned = _clean_rows(rows, prediction_params)
        predicted_quality = dict(cleaned.get("quality") or {})
        predicted_execution_audit = dict(cleaned.get("execution_audit") or {})
        allow_empty_output = _to_bool(
            _rule_param(
                params_effective,
                "allow_empty_output",
                params_effective.get("blank_output_expected", True),
            ),
            default=bool(params_effective.get("blank_output_expected", True)),
        )
        predicted_zero_output_unexpected = bool(
            int(predicted_quality.get("output_rows", 0) or 0) <= 0 and not allow_empty_output
        )

    blocking_reason_codes = set(_as_reason_codes(extract_payload.get("blocked_reason_codes")))
    if profile_blocked:
        blocking_reason_codes.add("profile_mismatch")
        blocking_reason_codes.add("profile_mismatch_blocked")
    if predicted_zero_output_unexpected:
        blocking_reason_codes.add("zero_output_unexpected")
    blocking_reason_codes_list = sorted(blocking_reason_codes)

    review_analysis = build_review_analysis(
        header_mapping=header_mapping,
        profile_analysis={
            "requested_profile": requested_profile,
            "recommended_profile": recommended_profile,
            "required_field_coverage": required_field_coverage,
            "zero_output_unexpected": predicted_zero_output_unexpected,
        },
        quality=predicted_quality,
        execution_audit=predicted_execution_audit,
        blocking_reason_codes=blocking_reason_codes_list,
    )

    quality_blocked = bool(extract_payload.get("quality_blocked")) and bool(blocking_reason_codes_list)
    precheck_action = _precheck_action(
        recommendation_available=recommendation_available,
        quality_blocked=quality_blocked,
        profile_blocked=profile_blocked,
        predicted_zero_output_unexpected=predicted_zero_output_unexpected,
        review_required=bool(review_analysis.get("review_required")),
    )
    issues, suggestions = _build_issues_and_suggestions(
        requested_profile=requested_profile,
        recommended_profile=recommended_profile,
        recommended_template_id=recommended_template_id,
        recommendation_available=recommendation_available,
        profile_mismatch=profile_mismatch,
        predicted_zero_output_unexpected=predicted_zero_output_unexpected,
        blocking_reason_codes=blocking_reason_codes_list,
    )
    for suggestion in [str(item).strip() for item in _as_list(review_analysis.get("suggested_repairs")) if str(item).strip()]:
        if suggestion not in suggestions:
            suggestions.append(suggestion)

    return {
        "ok": precheck_action != "block",
        "precheck_action": precheck_action,
        "requested_profile": requested_profile,
        "recommended_profile": recommended_profile,
        "recommended_template_id": recommended_template_id,
        "profile_confidence": profile_confidence,
        "profile_mismatch": profile_mismatch,
        "predicted_zero_output_unexpected": predicted_zero_output_unexpected,
        "blank_output_expected": blank_output_expected,
        "blocking_reason_codes": blocking_reason_codes_list,
        "issues": issues,
        "suggestions": suggestions,
        "header_mapping": header_mapping,
        "candidate_profiles": candidate_profiles,
        "quality_decisions": quality_decisions,
        "sample_rows": sample_rows,
        "quality_blocked": quality_blocked,
        "prediction_quality": predicted_quality,
        "issue_summary": dict(review_analysis.get("issue_summary") or {}),
        "suggested_repairs": list(review_analysis.get("suggested_repairs") or []),
        "header_ambiguities": list(review_analysis.get("header_ambiguities") or []),
        "duplicate_key_risk": dict(review_analysis.get("duplicate_key_risk") or {}),
        "review_required": bool(review_analysis.get("review_required", False)),
        "review_items": list(review_analysis.get("review_items") or []),
        "template_id": str(
            _as_dict(params_effective.get("_resolved_cleaning_template")).get("id")
            or params_effective.get("cleaning_template")
            or ""
        ).strip().lower(),
    }

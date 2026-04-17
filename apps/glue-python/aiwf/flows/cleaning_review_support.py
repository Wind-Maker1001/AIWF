from __future__ import annotations

from typing import Any, Dict, List, Mapping


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def build_header_ambiguities(
    header_mapping: List[Dict[str, Any]],
    *,
    confidence_gap_max: float = 0.08,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for mapping in header_mapping:
        if not isinstance(mapping, dict):
            continue
        raw_header = str(mapping.get("raw_header") or "").strip()
        alternatives = [dict(item) for item in _as_list(mapping.get("alternatives")) if isinstance(item, dict)]
        if not raw_header or len(alternatives) < 2:
            continue
        best = alternatives[0]
        second = alternatives[1]
        best_confidence = float(best.get("confidence") or 0.0)
        second_confidence = float(second.get("confidence") or 0.0)
        confidence_gap = round(best_confidence - second_confidence, 6)
        unresolved = not str(mapping.get("canonical_field") or "").strip()
        if unresolved or confidence_gap <= confidence_gap_max:
            items.append(
                {
                    "kind": "header_ambiguity",
                    "header": raw_header,
                    "canonical_field": str(mapping.get("canonical_field") or ""),
                    "match_strategy": str(mapping.get("match_strategy") or "unresolved"),
                    "confidence": round(float(mapping.get("confidence") or 0.0), 6),
                    "confidence_gap": confidence_gap,
                    "alternatives": alternatives[:3],
                    "message": (
                        f"header {raw_header} has close mapping candidates"
                        if not unresolved
                        else f"header {raw_header} is unresolved with multiple plausible candidates"
                    ),
                }
            )
    return items


def build_duplicate_key_risk(
    *,
    quality: Mapping[str, Any],
    execution_audit: Mapping[str, Any],
) -> Dict[str, Any]:
    duplicate_rows_removed = int(quality.get("duplicate_rows_removed", 0) or 0)
    duplicate_review_required_count = int(quality.get("duplicate_review_required_count", 0) or 0)
    reason_samples = _as_dict(execution_audit.get("reason_samples"))
    duplicate_samples = [dict(item) for item in _as_list(reason_samples.get("duplicate_removed")) if isinstance(item, dict)]
    risky_items: List[Dict[str, Any]] = []
    for sample in duplicate_samples:
        decision_basis = [str(item) for item in _as_list(sample.get("decision_basis")) if str(item).strip()]
        needs_review = (
            not decision_basis
            or any("tie" in item for item in decision_basis)
            or not sample.get("winner_row_id")
            or not sample.get("loser_row_id")
        )
        if not needs_review:
            continue
        risky_items.append(
            {
                "kind": "duplicate_key_risk",
                "key": list(sample.get("key") or []),
                "winner_row_id": int(sample.get("winner_row_id") or 0),
                "loser_row_id": int(sample.get("loser_row_id") or 0),
                "decision_basis": decision_basis,
                "message": "duplicate key conflict needs manual confirmation",
            }
        )
    unsampled_risky = max(0, duplicate_review_required_count - len(risky_items))
    if unsampled_risky:
        risky_items.append(
            {
                "kind": "duplicate_key_risk",
                "key": [],
                "winner_row_id": 0,
                "loser_row_id": 0,
                "decision_basis": ["unsampled_duplicate_conflicts"],
                "unsampled_count": unsampled_risky,
                "message": f"{unsampled_risky} duplicate key conflict(s) need manual confirmation outside the audit sample",
            }
        )
    return {
        "duplicate_rows_removed": duplicate_rows_removed,
        "review_required": bool(risky_items),
        "items": risky_items,
    }


def build_review_analysis(
    *,
    header_mapping: List[Dict[str, Any]],
    profile_analysis: Mapping[str, Any],
    quality: Mapping[str, Any],
    execution_audit: Mapping[str, Any],
    blocking_reason_codes: List[str],
) -> Dict[str, Any]:
    ambiguities = build_header_ambiguities(header_mapping)
    duplicate_key_risk = build_duplicate_key_risk(
        quality=quality,
        execution_audit=execution_audit,
    )
    coverage = float(profile_analysis.get("required_field_coverage") or 0.0)
    predicted_zero_output_unexpected = bool(profile_analysis.get("zero_output_unexpected", False))
    review_items: List[Dict[str, Any]] = list(ambiguities) + list(duplicate_key_risk.get("items") or [])
    if coverage > 0.0 and coverage < 0.75 and not blocking_reason_codes:
        review_items.append(
            {
                "kind": "required_field_coverage",
                "required_field_coverage": coverage,
                "message": "required field coverage is low and should be reviewed before cleaning",
            }
        )
    review_required = bool(ambiguities) or bool(duplicate_key_risk.get("review_required")) or (
        coverage > 0.0 and coverage < 0.75 and not blocking_reason_codes
    )

    suggested_repairs: List[str] = []
    if ambiguities:
        suggested_repairs.append("review ambiguous header mappings and pin rename_map or canonical_profile")
    if duplicate_key_risk.get("review_required"):
        suggested_repairs.append("review duplicate keys and configure survivorship preferences for the conflicting rows")
    if coverage > 0.0 and coverage < 1.0:
        suggested_repairs.append("improve required field coverage or relax the required_fields gate for this template")
    if int(quality.get("filtered_rows", 0) or 0) > 0:
        suggested_repairs.append("review row filters because some rows are predicted to be filtered out")
    if int(quality.get("invalid_rows", 0) or 0) > 0:
        suggested_repairs.append("review casts and field normalization because some rows are predicted to become invalid")

    issue_summary = {
        "header_ambiguity_count": len(ambiguities),
        "duplicate_conflict_count": len(duplicate_key_risk.get("items") or []),
        "predicted_invalid_rows": int(quality.get("invalid_rows", 0) or 0),
        "predicted_filtered_rows": int(quality.get("filtered_rows", 0) or 0),
        "predicted_duplicate_rows_removed": int(quality.get("duplicate_rows_removed", 0) or 0),
        "predicted_zero_output_unexpected": predicted_zero_output_unexpected,
        "required_field_coverage": coverage,
        "review_item_count": len(review_items),
    }
    return {
        "header_ambiguities": ambiguities,
        "duplicate_key_risk": duplicate_key_risk,
        "review_required": review_required,
        "review_items": review_items,
        "issue_summary": issue_summary,
        "suggested_repairs": suggested_repairs,
    }

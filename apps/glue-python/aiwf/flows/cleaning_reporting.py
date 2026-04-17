from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence


QUALITY_SUMMARY_SCHEMA_VERSION = "cleaning_quality_summary.v1"


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _normalize_reason_samples(value: Any) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    source = value if isinstance(value, dict) else {}
    for key, items in source.items():
        bucket: List[Dict[str, Any]] = []
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict):
                bucket.append(dict(item))
            else:
                bucket.append({"value": item})
        out[str(key)] = bucket
    return out


def flatten_rejection_records(reason_samples: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    normalized = _normalize_reason_samples(reason_samples)
    for reason_category in sorted(normalized.keys()):
        for sample_index, sample in enumerate(normalized[reason_category]):
            record = {
                "reason_category": reason_category,
                "sample_index": sample_index,
                "reason_code": str(sample.get("reason_code") or reason_category),
                "rule_id": str(sample.get("rule_id") or ""),
                "source_sheet": str(sample.get("source_sheet") or ""),
                "row_index": sample.get("row_index"),
                "sample": dict(sample),
            }
            out.append(record)
    return out


def build_quality_summary(
    *,
    params_effective: Dict[str, Any],
    transform_quality: Optional[Mapping[str, Any]],
    quality_gate: Optional[Mapping[str, Any]],
    execution_report: Optional[Mapping[str, Any]],
    preprocess_result: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    execution = _as_dict(execution_report)
    execution_audit = _as_dict(execution.get("execution_audit"))
    shadow_compare = _as_dict(execution.get("shadow_compare"))
    reason_counts = {
        str(key): int(value or 0)
        for key, value in _as_dict(execution_audit.get("reason_counts")).items()
    }
    reason_samples = _normalize_reason_samples(execution_audit.get("reason_samples"))
    preprocess = _as_dict(preprocess_result)
    blocked_inputs = [str(item) for item in _as_list(preprocess.get("blocked_inputs")) if str(item).strip()]
    skipped_files = [str(item) for item in _as_list(preprocess.get("skipped_files")) if str(item).strip()]
    failed_files = [str(item) for item in _as_list(preprocess.get("failed_files")) if str(item).strip()]
    file_results = _as_list(preprocess.get("file_results"))
    provenance = _as_dict(params_effective.get("_quality_rule_set_provenance"))
    stage_provenance = execution.get("stage_provenance")
    if not isinstance(stage_provenance, list):
        stage_provenance = execution_audit.get("stage_provenance")
    stage_provenance = list(stage_provenance) if isinstance(stage_provenance, list) else []
    advanced_quality = _as_dict(execution.get("advanced_quality"))
    row_samples = _as_dict(execution.get("row_samples"))
    review_analysis = _as_dict(execution.get("review_analysis") or execution_audit.get("review_analysis"))
    manual_review_queue = _as_dict(execution.get("manual_review_queue"))
    row_transform_engine = str(
        execution.get("row_transform_engine")
        or execution_audit.get("operator")
        or ("python" if str(execution.get("execution_mode") or "") == "python_legacy" else "")
    )
    materialization_engine = str(
        execution.get("materialization_engine")
        or ("legacy_accel_cleaning" if str(execution.get("execution_mode") or "") == "accel_operator" else "python")
    )
    postprocess_engine = str(execution.get("postprocess_engine") or "none")
    quality_gate_engine = str(execution.get("quality_gate_engine") or "none")
    profile_analysis = _as_dict(execution.get("profile_analysis"))
    gate_result = _as_dict(quality_gate)
    blank_output_expected = profile_analysis.get("blank_output_expected")
    if blank_output_expected is None:
        blank_output_expected = gate_result.get("blank_output_expected")
    if blank_output_expected is None:
        blank_output_expected = params_effective.get("blank_output_expected")
    blank_output_expected = bool(blank_output_expected) if blank_output_expected is not None else False
    output_rows = int(_as_dict(transform_quality).get("output_rows", 0) or 0)
    zero_output_unexpected = bool(gate_result.get("zero_output_unexpected")) or (output_rows <= 0 and not blank_output_expected)
    blocking_reason_codes = [
        str(item).strip()
        for item in _as_list(profile_analysis.get("blocking_reason_codes"))
        if str(item).strip()
    ]
    if zero_output_unexpected and "zero_output_unexpected" not in blocking_reason_codes:
        blocking_reason_codes.append("zero_output_unexpected")
    if _as_dict(preprocess.get("summary")).get("blocked_reason_codes"):
        for item in _as_list(_as_dict(preprocess.get("summary")).get("blocked_reason_codes")):
            code = str(item).strip()
            if code and code not in blocking_reason_codes:
                blocking_reason_codes.append(code)

    return {
        "schema_version": QUALITY_SUMMARY_SCHEMA_VERSION,
        "quality_rule_set_id": str(params_effective.get("quality_rule_set_id") or ""),
        "requested_profile": str(profile_analysis.get("requested_profile") or ""),
        "recommended_profile": str(profile_analysis.get("recommended_profile") or ""),
        "profile_confidence": float(profile_analysis.get("profile_confidence") or 0.0),
        "profile_mismatch": bool(profile_analysis.get("profile_mismatch", False)),
        "required_field_coverage": float(profile_analysis.get("required_field_coverage") or 0.0),
        "blank_output_expected": blank_output_expected,
        "zero_output_unexpected": zero_output_unexpected,
        "blocking_reason_codes": blocking_reason_codes,
        "rule_set_provenance": provenance,
        "input_quality": {
            "mode": "preprocess" if preprocess else "direct",
            "blocked": bool(blocked_inputs),
            "blocked_inputs": blocked_inputs,
            "input_format": str(preprocess.get("input_format") or ""),
            "output_format": str(preprocess.get("output_format") or ""),
            "file_results_count": len(file_results),
            "skipped_files": skipped_files,
            "failed_files": failed_files,
            "quality_report_path": str(preprocess.get("quality_report_path") or ""),
            "summary": _as_dict(preprocess.get("summary")),
            "profile_analysis": profile_analysis,
        },
        "transform_quality": _as_dict(transform_quality),
        "gate_result": gate_result,
        "advanced_quality": advanced_quality,
        "review_analysis": review_analysis,
        "manual_review_queue": manual_review_queue,
        "reason_counts": reason_counts,
        "reason_sample_counts": {
            str(key): len(items)
            for key, items in reason_samples.items()
        },
        "engine_path": {
            "execution_mode": str(execution.get("execution_mode") or ""),
            "eligibility_reason": str(execution.get("eligibility_reason") or ""),
            "execution_plan": str(execution.get("execution_plan") or ""),
            "row_transform_engine": row_transform_engine,
            "materialization_engine": materialization_engine,
            "postprocess_engine": postprocess_engine,
            "quality_gate_engine": quality_gate_engine,
            "legacy_cleaning_operator_used": bool(execution.get("legacy_cleaning_operator_used", False)),
            "stage_provenance": stage_provenance,
            "requested_rust_v2_mode": str(execution.get("requested_rust_v2_mode") or ""),
            "effective_rust_v2_mode": str(execution.get("effective_rust_v2_mode") or ""),
            "verify_on_default": bool(execution.get("verify_on_default", False)),
            "shadow_compare_status": str(shadow_compare.get("status") or ""),
            "audit_schema": str(execution_audit.get("schema") or ""),
        },
        "shadow_compare": shadow_compare,
        "rejections": {
            "sample_limit": _as_dict(execution_audit.get("limits")).get("sample_limit"),
            "sampled_record_count": sum(len(items) for items in reason_samples.values()),
            "sampled": True,
        },
        "row_samples": {
            "before": _as_list(row_samples.get("before"))[:5],
            "after": _as_list(row_samples.get("after"))[:5],
        },
    }

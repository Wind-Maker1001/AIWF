from __future__ import annotations

import copy
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from aiwf.canonical_profiles import get_profile_registry, resolve_profile_name


CLEANING_SPEC_V2_VERSION = "cleaning_spec.v2"
CLEANING_SPEC_V2_CONTRACT = "contracts/glue/cleaning_spec.v2.schema.json"
HEADER_MAPPING_MODE_VALUES = ["strict", "auto"]
DEFAULT_HEADER_MAPPING_MODE = "strict"
_PROFILE_TEMPLATE_RECOMMENDATIONS = {
    "finance_statement": "finance_report_v1",
    "bank_statement": "bank_statement_v1",
    "customer_contact": "customer_contact_v1",
    "customer_ledger": "customer_ledger_v1",
    "debate_evidence": "debate_evidence_v1",
}

CANONICAL_PROFILE_REGISTRY = get_profile_registry()

_QUALITY_GATE_KEYS = [
    "max_invalid_rows",
    "max_filtered_rows",
    "min_output_rows",
    "max_invalid_ratio",
    "max_required_missing_ratio",
    "max_duplicate_rows_removed",
    "allow_empty_output",
    "blank_output_expected",
    "numeric_parse_rate_min",
    "date_parse_rate_min",
    "duplicate_key_ratio_max",
    "blank_row_ratio_max",
]

_SUPPORTED_PREPROCESS_FIELD_OPS = {
    "trim",
    "lower",
    "upper",
    "collapse_whitespace",
    "remove_urls",
    "remove_emails",
    "regex_replace",
    "parse_number",
    "round_number",
    "parse_date",
    "extract_regex",
    "scale_number",
    "map_value",
}


def _normalize_advanced_rules(value: Any) -> dict[str, Any]:
    source = _as_dict(value)
    out: dict[str, Any] = {}
    outlier = source.get("outlier_zscore")
    if isinstance(outlier, dict):
        out["outlier_zscore"] = dict(outlier)
    elif isinstance(outlier, list):
        out["outlier_zscore"] = [dict(item) for item in outlier if isinstance(item, dict)]
    anomaly = source.get("anomaly_iqr")
    if isinstance(anomaly, dict):
        out["anomaly_iqr"] = [dict(anomaly)]
    elif isinstance(anomaly, list):
        out["anomaly_iqr"] = [dict(item) for item in anomaly if isinstance(item, dict)]
    if "block_on_advanced_rules" in source:
        out["block_on_advanced_rules"] = bool(source.get("block_on_advanced_rules"))
    semantic = source.get("bank_statement_semantics")
    if isinstance(semantic, dict):
        out["bank_statement_semantics"] = dict(semantic)
    return out


def empty_cleaning_spec_v2() -> dict[str, Any]:
    return {
        "schema_version": CLEANING_SPEC_V2_VERSION,
        "ingest": {},
        "schema": {},
        "transform": {},
        "quality": {},
        "artifacts": {},
        "audit": {"enabled": True, "sample_limit": 5, "lineage": True, "warnings": []},
    }


def get_canonical_profile_registry() -> dict[str, dict[str, Any]]:
    return copy.deepcopy(CANONICAL_PROFILE_REGISTRY)


def resolve_canonical_profile_name(value: Any) -> str:
    return resolve_profile_name(value)


def resolve_canonical_profile_spec(value: Any) -> dict[str, Any]:
    name = resolve_canonical_profile_name(value)
    return copy.deepcopy(CANONICAL_PROFILE_REGISTRY.get(name, {}))


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _merge_unique_strings(*groups: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for item in group:
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
    return out


def _normalize_preprocess_header(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s\-\/]+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "col"


def _normalize_lower_key(value: str) -> str:
    return str(value or "").strip().lower()


def _quality_gates_from_sources(*sources: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for source in sources:
        for key in _QUALITY_GATE_KEYS:
            if key in source:
                out[key] = source.get(key)
    if "allow_empty_output" not in out and "blank_output_expected" in out:
        out["allow_empty_output"] = out.get("blank_output_expected")
    return out


def _canonical_profile_from_params(params: Mapping[str, Any]) -> str:
    rules = _as_dict(params.get("rules"))
    quality_rules = _as_dict(params.get("quality_rules"))
    value = (
        params.get("canonical_profile")
        or rules.get("canonical_profile")
        or quality_rules.get("canonical_profile")
        or params.get("profile")
        or rules.get("profile")
    )
    return resolve_canonical_profile_name(value)


def compile_cleaning_params_to_spec(params: Mapping[str, Any]) -> dict[str, Any]:
    spec = empty_cleaning_spec_v2()
    rules = _as_dict(params.get("rules"))
    quality_rules = _as_dict(params.get("quality_rules"))
    image_rules = _as_dict(params.get("image_rules"))
    xlsx_rules = _as_dict(params.get("xlsx_rules"))
    sheet_profiles = _as_dict(params.get("sheet_profiles"))
    profile_name = _canonical_profile_from_params(params)
    profile_spec = resolve_canonical_profile_spec(profile_name)
    advanced_rules = _normalize_advanced_rules(
        params.get("advanced_rules")
        or quality_rules.get("advanced_rules")
        or rules.get("advanced_rules")
    )

    spec["ingest"] = {
        "input_format": str(params.get("input_format") or "").strip().lower(),
        "input_files": _as_str_list(params.get("input_files")),
        "input_path": str(params.get("input_path") or params.get("input_csv_path") or "").strip(),
        "header_mapping_mode": str(params.get("header_mapping_mode") or DEFAULT_HEADER_MAPPING_MODE).strip().lower() or DEFAULT_HEADER_MAPPING_MODE,
        "text_split_by_line": bool(params.get("text_split_by_line", False)),
        "ocr_enabled": bool(params.get("ocr_enabled", True)),
        "ocr_lang": str(params.get("ocr_lang") or "").strip(),
        "ocr_config": str(params.get("ocr_config") or "").strip(),
        "ocr_preprocess": str(params.get("ocr_preprocess") or "").strip(),
        "xlsx_all_sheets": bool(params.get("xlsx_all_sheets", True)),
        "include_hidden_sheets": bool(params.get("include_hidden_sheets", False)),
        "sheet_allowlist": _as_str_list(params.get("sheet_allowlist")),
        "sheet_profiles": sheet_profiles,
        "on_file_error": str(params.get("on_file_error") or "").strip().lower(),
    }

    transform: dict[str, Any] = {
        "rename_map": _as_dict(rules.get("rename_map")),
        "casts": _as_dict(rules.get("casts")),
        "required_fields": _as_str_list(rules.get("required_fields")),
        "default_values": _as_dict(rules.get("default_values")),
        "include_fields": _as_str_list(rules.get("include_fields")),
        "exclude_fields": _as_str_list(rules.get("exclude_fields")),
        "filters": list(rules.get("filters")) if isinstance(rules.get("filters"), list) else [],
        "deduplicate_by": _as_str_list(rules.get("deduplicate_by")),
        "deduplicate_keep": str(rules.get("deduplicate_keep") or "last").strip().lower() or "last",
        "sort_by": list(rules.get("sort_by")) if isinstance(rules.get("sort_by"), list) else [],
        "aggregate": _as_dict(rules.get("aggregate")),
        "null_values": _as_str_list(rules.get("null_values")),
        "trim_strings": bool(rules.get("trim_strings", True)),
        "computed_fields": _as_dict(rules.get("computed_fields")),
        "string_ops": list(rules.get("string_ops")) if isinstance(rules.get("string_ops"), list) else [],
        "date_ops": list(rules.get("date_ops")) if isinstance(rules.get("date_ops"), list) else [],
        "field_ops": list(rules.get("field_ops")) if isinstance(rules.get("field_ops"), list) else [],
        "survivorship": _as_dict(rules.get("survivorship")),
    }

    is_generic = str(rules.get("platform_mode") or "").strip().lower() == "generic" or any(
        key in rules
        for key in (
            "rename_map",
            "casts",
            "filters",
            "required_fields",
            "default_values",
            "include_fields",
            "exclude_fields",
            "deduplicate_by",
            "sort_by",
            "null_values",
            "string_ops",
            "date_ops",
            "field_ops",
            "computed_fields",
            "survivorship",
            "advanced_rules",
        )
    )
    if not is_generic:
        id_field = str(rules.get("id_field") or params.get("id_field") or "id").strip() or "id"
        amount_field = str(rules.get("amount_field") or params.get("amount_field") or "amount").strip() or "amount"
        if id_field != "id":
            transform["rename_map"][id_field] = "id"
        if amount_field != "amount":
            transform["rename_map"][amount_field] = "amount"
        transform["casts"].setdefault("id", "int")
        transform["casts"].setdefault("amount", "float")
        transform["required_fields"] = _merge_unique_strings(transform["required_fields"], ["id", "amount"])
        digits = params.get("amount_round_digits", rules.get("amount_round_digits", 2))
        transform["field_ops"].append({"field": "amount", "op": "round_number", "digits": int(digits)})
        if bool(params.get("drop_negative_amount", rules.get("drop_negative_amount", False))):
            transform["filters"].append({"field": "amount", "op": "gte", "value": 0})
        if (min_amount := params.get("min_amount", rules.get("min_amount"))) is not None:
            transform["filters"].append({"field": "amount", "op": "gte", "value": min_amount})
        if (max_amount := params.get("max_amount", rules.get("max_amount"))) is not None:
            transform["filters"].append({"field": "amount", "op": "lte", "value": max_amount})
        if bool(params.get("deduplicate_by_id", rules.get("deduplicate_by_id", True))):
            transform["deduplicate_by"] = ["id"]
        if bool(params.get("sort_by_id", rules.get("sort_by_id", True))):
            transform["sort_by"] = [{"field": "id", "order": "asc"}]
        spec["schema"]["auto_normalize_headers"] = True
        spec["schema"]["header_normalizer"] = "lower"
    else:
        spec["schema"]["auto_normalize_headers"] = False
        spec["schema"]["header_normalizer"] = "none"

    if profile_name:
        transform["default_values"] = {**profile_spec.get("defaults", {}), **transform["default_values"]}

    spec["transform"] = transform
    spec["schema"] = {
        **spec["schema"],
        "canonical_profile": profile_name,
        "template_expected_profile": str(params.get("template_expected_profile") or profile_name or "").strip().lower(),
        "fields": profile_spec,
        "defaults": copy.deepcopy(profile_spec.get("defaults", {})),
        "unique_keys": _merge_unique_strings(profile_spec.get("unique_keys", []), transform.get("deduplicate_by", [])),
        "header_aliases": copy.deepcopy(profile_spec.get("header_aliases", {})),
    }
    spec["quality"] = {
        "required_fields": _merge_unique_strings(
            _as_str_list(quality_rules.get("required_fields")),
            _as_str_list(rules.get("required_fields")),
            profile_spec.get("required_fields", []),
        ),
        "gates": _quality_gates_from_sources(params, rules, quality_rules),
        "advanced_rules": advanced_rules,
        "image_rules": image_rules,
        "xlsx_rules": xlsx_rules,
    }
    spec["artifacts"] = {
        "output_format": str(params.get("output_format") or "").strip().lower(),
        "generate_quality_report": bool(params.get("generate_quality_report", False)),
        "quality_report_path": str(params.get("quality_report_path") or "").strip(),
        "export_canonical_bundle": bool(params.get("export_canonical_bundle", False)),
        "canonical_bundle_dir": str(params.get("canonical_bundle_dir") or "").strip(),
    }
    spec["audit"] = {
        "enabled": True,
        "sample_limit": int(params.get("audit_sample_limit", 5) or 5),
        "lineage": True,
        "legacy_source": "cleaning.params",
        "warnings": [],
    }
    return spec


def _compile_preprocess_field_transform(item: Mapping[str, Any]) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    field = str(item.get("field") or "").strip()
    op = str(item.get("op") or "").strip().lower()
    if not field or not op:
        return None, "missing field/op"
    if op not in _SUPPORTED_PREPROCESS_FIELD_OPS:
        return None, f"unsupported field transform: {op}"
    payload: dict[str, Any] = {"field": field, "op": op}
    if "digits" in item:
        payload["digits"] = int(item.get("digits") or 0)
    if "pattern" in item:
        payload["pattern"] = str(item.get("pattern") or "")
    if "replace" in item:
        payload["replace"] = str(item.get("replace") or "")
    if "group" in item:
        payload["group"] = int(item.get("group") or 0)
    if "output_format" in item:
        payload["output_format"] = str(item.get("output_format") or "")
    if "input_formats" in item and isinstance(item.get("input_formats"), list):
        payload["input_formats"] = [str(value) for value in item["input_formats"]]
    if "multiplier" in item:
        payload["multiplier"] = item.get("multiplier")
    if "mapping" in item and isinstance(item.get("mapping"), dict):
        payload["mapping"] = dict(item.get("mapping") or {})
    return payload, None


def compile_preprocess_spec_to_spec(spec_obj: Mapping[str, Any]) -> dict[str, Any]:
    spec = empty_cleaning_spec_v2()
    quality_rules = _as_dict(spec_obj.get("quality_rules"))
    image_rules = _as_dict(spec_obj.get("image_rules"))
    xlsx_rules = _as_dict(spec_obj.get("xlsx_rules"))
    profile_name = resolve_canonical_profile_name(
        spec_obj.get("canonical_profile") or quality_rules.get("canonical_profile")
    )
    profile_spec = resolve_canonical_profile_spec(profile_name)
    field_ops: list[dict[str, Any]] = []
    warnings: list[str] = []

    if "amount_fields" in spec_obj:
        for field in _as_str_list(spec_obj.get("amount_fields")):
            field_ops.append({"field": field, "op": "parse_number"})
            field_ops.append(
                {
                    "field": field,
                    "op": "round_number",
                    "digits": int(spec_obj.get("amount_round_digits", 2) or 2),
                }
            )
    for field in _as_str_list(spec_obj.get("date_fields")):
        field_ops.append(
            {
                "field": field,
                "op": "parse_date",
                "output_format": str(spec_obj.get("date_output_format") or "%Y-%m-%d"),
                "input_formats": [str(value) for value in (spec_obj.get("date_input_formats") or [])],
            }
        )
    for item in spec_obj.get("field_transforms") if isinstance(spec_obj.get("field_transforms"), list) else []:
        if not isinstance(item, dict):
            warnings.append("ignored non-object field transform")
            continue
        compiled, error = _compile_preprocess_field_transform(item)
        if compiled is None:
            warnings.append(error or "ignored unsupported field transform")
            continue
        field_ops.append(compiled)

    spec["ingest"] = {
        "input_format": str(spec_obj.get("input_format") or "").strip().lower(),
        "input_files": _as_str_list(spec_obj.get("input_files")),
        "input_path": str(spec_obj.get("input_path") or "").strip(),
        "header_mapping_mode": str(spec_obj.get("header_mapping_mode") or DEFAULT_HEADER_MAPPING_MODE).strip().lower() or DEFAULT_HEADER_MAPPING_MODE,
        "text_split_by_line": bool(spec_obj.get("text_split_by_line", False)),
        "ocr_enabled": bool(spec_obj.get("ocr_enabled", True)),
        "ocr_lang": str(spec_obj.get("ocr_lang") or "").strip(),
        "ocr_config": str(spec_obj.get("ocr_config") or "").strip(),
        "ocr_preprocess": str(spec_obj.get("ocr_preprocess") or "").strip(),
        "xlsx_all_sheets": bool(spec_obj.get("xlsx_all_sheets", True)),
        "include_hidden_sheets": bool(spec_obj.get("include_hidden_sheets", False)),
        "sheet_allowlist": _as_str_list(spec_obj.get("sheet_allowlist")),
        "sheet_profiles": _as_dict(spec_obj.get("sheet_profiles")),
        "on_file_error": str(spec_obj.get("on_file_error") or "skip").strip().lower(),
    }
    spec["schema"] = {
        "canonical_profile": profile_name,
        "template_expected_profile": str(spec_obj.get("template_expected_profile") or profile_name or "").strip().lower(),
        "fields": profile_spec,
        "defaults": copy.deepcopy(profile_spec.get("defaults", {})),
        "unique_keys": _merge_unique_strings(
            profile_spec.get("unique_keys", []),
            _as_str_list(spec_obj.get("deduplicate_by")),
        ),
        "header_aliases": copy.deepcopy(profile_spec.get("header_aliases", {})),
        "auto_normalize_headers": True,
        "header_normalizer": "preprocess",
    }
    spec["transform"] = {
        "rename_map": _as_dict(spec_obj.get("header_map")),
        "casts": {},
        "required_fields": [],
        "default_values": _as_dict(spec_obj.get("default_values")),
        "include_fields": _as_str_list(spec_obj.get("include_fields")),
        "exclude_fields": _as_str_list(spec_obj.get("exclude_fields")),
        "filters": list(spec_obj.get("row_filters")) if isinstance(spec_obj.get("row_filters"), list) else [],
        "deduplicate_by": _as_str_list(spec_obj.get("deduplicate_by")),
        "deduplicate_keep": str(spec_obj.get("deduplicate_keep") or "first").strip().lower() or "first",
        "sort_by": [],
        "aggregate": {},
        "null_values": _as_str_list(spec_obj.get("null_values") or ["null", "none", "na", "n/a"]),
        "trim_strings": bool(spec_obj.get("trim_strings", True)),
        "computed_fields": {},
        "string_ops": [],
        "date_ops": [],
        "field_ops": field_ops,
        "survivorship": _as_dict(spec_obj.get("survivorship")),
        "postprocess": {
            "standardize_evidence": bool(spec_obj.get("standardize_evidence", False)),
            "evidence_schema": _as_dict(spec_obj.get("evidence_schema")),
            "chunk_mode": str(spec_obj.get("chunk_mode") or "none").strip().lower(),
            "chunk_field": str(spec_obj.get("chunk_field") or "").strip(),
            "chunk_max_chars": int(spec_obj.get("chunk_max_chars", 500) or 500),
            "detect_conflicts": bool(spec_obj.get("detect_conflicts", False)),
            "conflict_topic_field": str(spec_obj.get("conflict_topic_field") or "").strip(),
            "conflict_stance_field": str(spec_obj.get("conflict_stance_field") or "").strip(),
            "conflict_text_field": str(spec_obj.get("conflict_text_field") or "").strip(),
            "conflict_positive_words": _as_str_list(spec_obj.get("conflict_positive_words")),
            "conflict_negative_words": _as_str_list(spec_obj.get("conflict_negative_words")),
        },
    }
    spec["quality"] = {
        "required_fields": _merge_unique_strings(
            _as_str_list(spec_obj.get("quality_required_fields")),
            _as_str_list(quality_rules.get("required_fields")),
            profile_spec.get("required_fields", []),
        ),
        "gates": _quality_gates_from_sources(spec_obj, quality_rules),
        "advanced_rules": _normalize_advanced_rules(
            spec_obj.get("advanced_rules") or quality_rules.get("advanced_rules")
        ),
        "image_rules": image_rules,
        "xlsx_rules": xlsx_rules,
    }
    spec["artifacts"] = {
        "output_format": str(spec_obj.get("output_format") or "").strip().lower(),
        "generate_quality_report": bool(spec_obj.get("generate_quality_report", False)),
        "quality_report_path": str(spec_obj.get("quality_report_path") or "").strip(),
        "export_canonical_bundle": bool(spec_obj.get("export_canonical_bundle", False)),
        "canonical_bundle_dir": str(spec_obj.get("canonical_bundle_dir") or "").strip(),
    }
    spec["audit"] = {
        "enabled": True,
        "sample_limit": int(spec_obj.get("audit_sample_limit", 5) or 5),
        "lineage": True,
        "legacy_source": "preprocess.spec",
        "warnings": warnings,
    }
    return spec


def cleaning_spec_to_transform_components(
    spec: Mapping[str, Any],
    *,
    input_rows: Optional[Sequence[Mapping[str, Any]]] = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    transform = _as_dict(spec.get("transform"))
    schema = _as_dict(spec.get("schema"))
    quality = _as_dict(spec.get("quality"))
    rename_map = dict(transform.get("rename_map") or {})
    normalizer = str(schema.get("header_normalizer") or "none").strip().lower()
    auto_normalize = bool(schema.get("auto_normalize_headers", False))
    if auto_normalize and input_rows:
        seen_keys: set[str] = set()
        for row in input_rows:
            for raw_key in row.keys():
                key_text = str(raw_key)
                if key_text in seen_keys or key_text in rename_map:
                    continue
                seen_keys.add(key_text)
                normalized = ""
                if normalizer == "preprocess":
                    normalized = _normalize_preprocess_header(key_text)
                elif normalizer == "lower":
                    normalized = _normalize_lower_key(key_text)
                if normalized and normalized != key_text:
                    rename_map[key_text] = normalized

    rules = {
        "rename_map": rename_map,
        "casts": _as_dict(transform.get("casts")),
        "required_fields": _as_str_list(transform.get("required_fields")),
        "default_values": _as_dict(transform.get("default_values")),
        "include_fields": _as_str_list(transform.get("include_fields")),
        "exclude_fields": _as_str_list(transform.get("exclude_fields")),
        "filters": list(transform.get("filters")) if isinstance(transform.get("filters"), list) else [],
        "deduplicate_by": _as_str_list(transform.get("deduplicate_by")),
        "deduplicate_keep": str(transform.get("deduplicate_keep") or "last").strip().lower() or "last",
        "sort_by": list(transform.get("sort_by")) if isinstance(transform.get("sort_by"), list) else [],
        "aggregate": _as_dict(transform.get("aggregate")),
        "null_values": _as_str_list(transform.get("null_values")),
        "trim_strings": bool(transform.get("trim_strings", True)),
        "computed_fields": _as_dict(transform.get("computed_fields")),
        "string_ops": list(transform.get("string_ops")) if isinstance(transform.get("string_ops"), list) else [],
        "date_ops": list(transform.get("date_ops")) if isinstance(transform.get("date_ops"), list) else [],
        "field_ops": list(transform.get("field_ops")) if isinstance(transform.get("field_ops"), list) else [],
        "survivorship": _as_dict(transform.get("survivorship")),
    }
    quality_gates = {
        **_as_dict(quality.get("gates")),
        "required_fields": _merge_unique_strings(_as_str_list(quality.get("required_fields"))),
    }
    schema_hint = {
        "schema_version": CLEANING_SPEC_V2_VERSION,
        "contract": CLEANING_SPEC_V2_CONTRACT,
        "canonical_profile": str(schema.get("canonical_profile") or "").strip().lower(),
        "template_expected_profile": str(schema.get("template_expected_profile") or "").strip().lower(),
        "fields": _as_dict(schema.get("fields")),
        "defaults": _as_dict(schema.get("defaults")),
        "unique_keys": _as_str_list(schema.get("unique_keys")),
        "audit": _as_dict(spec.get("audit")),
    }
    return rules, quality_gates, schema_hint


def candidate_profiles_from_headers(
    headers: Sequence[str],
    *,
    sheet_profiles: Optional[Mapping[str, Any]] = None,
    header_mapping_mode: str = DEFAULT_HEADER_MAPPING_MODE,
    sample_values_by_header: Optional[Mapping[str, Sequence[Any]]] = None,
    signal_source: str = "headers",
    limit: int = 3,
) -> list[dict[str, Any]]:
    from aiwf.quality_contract import analyze_header_mapping

    raw_headers = [str(item).strip() for item in headers if str(item).strip()]
    if not raw_headers:
        return []
    candidates: list[dict[str, Any]] = []
    for profile_name, profile in CANONICAL_PROFILE_REGISTRY.items():
        field_universe = {
            str(item)
            for group in (
                profile.get("required_fields", []),
                profile.get("string_fields", []),
                profile.get("numeric_fields", []),
                profile.get("date_fields", []),
            )
            for item in group
            if str(item).strip()
        }
        matched_fields: dict[str, float] = {}
        unresolved_required_ambiguity = 0
        for header in raw_headers:
            details = analyze_header_mapping(
                header,
                {
                    "canonical_profile": profile_name,
                    "sheet_profiles": dict(sheet_profiles or {}),
                    "header_mapping_mode": header_mapping_mode,
                },
                sample_values=list((sample_values_by_header or {}).get(header) or []),
            )
            field = str(details.get("canonical_field") or "")
            confidence = float(details.get("confidence") or 0.0)
            if field and field not in field_universe:
                field = ""
            if not field or field.startswith("col_"):
                alternatives = details.get("alternatives") if isinstance(details.get("alternatives"), list) else []
                best_alt = alternatives[0] if alternatives else {}
                if str(best_alt.get("field") or "").strip() in {
                    str(item) for item in profile.get("required_fields", []) if str(item).strip()
                }:
                    unresolved_required_ambiguity += 1
                continue
            previous = matched_fields.get(field)
            if previous is None or confidence > previous:
                matched_fields[field] = confidence
        required_fields = [str(item) for item in profile.get("required_fields", [])]
        required_hits = sum(1 for field in required_fields if field in matched_fields)
        if not matched_fields:
            continue
        avg_confidence = sum(matched_fields.values()) / len(matched_fields)
        coverage = required_hits / max(1, len(required_fields))
        matched_field_ratio = len(matched_fields) / max(1, len(field_universe))
        score = round(
            coverage * 0.55
            + matched_field_ratio * 0.25
            + avg_confidence * 0.20
            - (0.08 if unresolved_required_ambiguity > 0 else 0.0),
            6,
        )
        recommended = coverage == 1.0 or (required_hits > 0 and score >= 0.78)
        candidates.append(
            {
                "profile": profile_name,
                "score": score,
                "required_hits": required_hits,
                "required_total": len(required_fields),
                "avg_confidence": round(avg_confidence, 6),
                "required_coverage": round(coverage, 6),
                "recommended": recommended,
                "recommended_template_id": _PROFILE_TEMPLATE_RECOMMENDATIONS.get(profile_name, "") if recommended else "",
                "signal_source": str(signal_source or "headers"),
                "matched_fields": sorted(matched_fields.keys()),
            }
        )
    contact_candidate = next((item for item in candidates if item.get("profile") == "customer_contact"), None)
    ledger_candidate = next((item for item in candidates if item.get("profile") == "customer_ledger"), None)
    if isinstance(contact_candidate, dict) and isinstance(ledger_candidate, dict):
        ledger_fields = {str(item) for item in ledger_candidate.get("matched_fields", []) if str(item).strip()}
        if (
            float(ledger_candidate.get("required_coverage") or 0.0) >= 1.0
            and {"amount", "biz_date"}.issubset(ledger_fields)
        ):
            ledger_candidate["score"] = round(float(ledger_candidate.get("score") or 0.0) + 0.03, 6)
            ledger_candidate["recommended"] = True
            ledger_candidate["recommended_template_id"] = _PROFILE_TEMPLATE_RECOMMENDATIONS.get("customer_ledger", "")
            contact_candidate["recommended"] = False
            contact_candidate["recommended_template_id"] = ""
    candidates.sort(key=lambda item: (-float(item.get("score", 0.0)), -int(item.get("required_hits", 0)), item["profile"]))
    return candidates[: max(1, int(limit or 3))]


def recommended_template_id_for_profile(profile_name: str) -> str:
    return str(_PROFILE_TEMPLATE_RECOMMENDATIONS.get(str(profile_name or "").strip().lower(), "") or "")


def build_header_mapping(
    headers: Sequence[str],
    *,
    canonical_profile: str = "",
    sheet_profiles: Optional[Mapping[str, Any]] = None,
    header_mapping_mode: str = DEFAULT_HEADER_MAPPING_MODE,
    sample_values_by_header: Optional[Mapping[str, Sequence[Any]]] = None,
) -> list[dict[str, Any]]:
    from aiwf.quality_contract import analyze_header_mapping

    spec = {
        "canonical_profile": resolve_canonical_profile_name(canonical_profile),
        "sheet_profiles": dict(sheet_profiles or {}),
        "header_mapping_mode": str(header_mapping_mode or DEFAULT_HEADER_MAPPING_MODE).strip().lower() or DEFAULT_HEADER_MAPPING_MODE,
    }
    out: list[dict[str, Any]] = []
    for header in headers:
        raw_header = str(header).strip()
        if not raw_header:
            continue
        details = analyze_header_mapping(
            raw_header,
            spec,
            sample_values=list((sample_values_by_header or {}).get(raw_header) or []),
        )
        out.append(
            {
                "raw_header": raw_header,
                "canonical_field": str(details.get("canonical_field") or ""),
                "confidence": round(float(details.get("confidence") or 0.0), 6),
                "matched_token": str(details.get("matched_token") or ""),
                "match_strategy": str(details.get("match_strategy") or "unresolved"),
                "alternatives": list(details.get("alternatives") or []),
            }
        )
    return out


def reason_codes_from_quality_errors(errors: Sequence[Any]) -> list[str]:
    codes: list[str] = []
    for item in errors:
        text = str(item or "").strip().lower()
        if not text:
            continue
        if "header_confidence" in text:
            codes.append("header_low_confidence")
        elif "ocr_confidence" in text or "low_confidence_block_ratio" in text:
            codes.append("ocr_low_confidence")
        elif "required_columns missing" in text:
            codes.append("required_fields_missing")
        elif "required_missing_ratio" in text:
            codes.append("required_missing")
        elif "numeric_parse_rate" in text:
            codes.append("numeric_parse_low")
        elif "date_parse_rate" in text:
            codes.append("date_parse_low")
        elif "duplicate_key_ratio" in text or "duplicate_line_ratio" in text:
            codes.append("duplicate_excess")
        elif "blank_row_ratio" in text:
            codes.append("blank_rows_excess")
        elif "empty_text_block_ratio" in text:
            codes.append("empty_text_excess")
        elif "produced no rows" in text:
            codes.append("no_rows_extracted")
        else:
            codes.append("quality_blocked")
    return _merge_unique_strings(codes)


def build_quality_decisions(
    *,
    quality_report: Optional[Mapping[str, Any]],
    quality_blocked: bool,
) -> list[dict[str, Any]]:
    report = dict(quality_report or {})
    errors = report.get("errors") if isinstance(report.get("errors"), list) else []
    metrics = report.get("metrics") if isinstance(report.get("metrics"), dict) else {}
    return [
        {
            "scope": "input_quality",
            "blocked": bool(quality_blocked),
            "reason_codes": reason_codes_from_quality_errors(errors),
            "metrics": metrics,
            "errors": [str(item) for item in errors if str(item).strip()],
        }
    ]

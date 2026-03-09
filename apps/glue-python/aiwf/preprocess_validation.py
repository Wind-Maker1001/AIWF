from __future__ import annotations

from typing import Any, Callable, Dict, List


def validate_preprocess_spec_impl(
    spec: Dict[str, Any],
    *,
    field_transform_ops: List[str],
    row_filter_specs: Dict[str, bool],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(spec, dict):
        return {"ok": False, "errors": ["preprocess spec must be object"], "warnings": []}

    for key in ["header_map", "default_values"]:
        if key in spec and not isinstance(spec.get(key), dict):
            errors.append(f"{key} must be an object")
    for key in [
        "amount_fields",
        "date_fields",
        "null_values",
        "include_fields",
        "exclude_fields",
        "field_transforms",
        "row_filters",
        "input_files",
        "quality_required_fields",
        "conflict_positive_words",
        "conflict_negative_words",
    ]:
        if key in spec and not isinstance(spec.get(key), list):
            errors.append(f"{key} must be an array")
    if "ocr_enabled" in spec and not isinstance(spec.get("ocr_enabled"), bool):
        errors.append("ocr_enabled must be boolean")
    if "ocr_lang" in spec and not isinstance(spec.get("ocr_lang"), str):
        errors.append("ocr_lang must be string")
    if "ocr_config" in spec and not isinstance(spec.get("ocr_config"), str):
        errors.append("ocr_config must be string")
    if "ocr_preprocess" in spec and not isinstance(spec.get("ocr_preprocess"), str):
        errors.append("ocr_preprocess must be string")
    if "ocr_preprocess" in spec:
        val = str(spec.get("ocr_preprocess") or "").strip().lower()
        if val and val not in {"adaptive", "gray", "none", "off"}:
            errors.append("ocr_preprocess must be adaptive|gray|none|off")
    if "xlsx_all_sheets" in spec and not isinstance(spec.get("xlsx_all_sheets"), bool):
        errors.append("xlsx_all_sheets must be boolean")
    if "standardize_evidence" in spec and not isinstance(spec.get("standardize_evidence"), bool):
        errors.append("standardize_evidence must be boolean")
    if "generate_quality_report" in spec and not isinstance(spec.get("generate_quality_report"), bool):
        errors.append("generate_quality_report must be boolean")
    if "quality_report_path" in spec and not isinstance(spec.get("quality_report_path"), str):
        errors.append("quality_report_path must be string")
    if "export_canonical_bundle" in spec and not isinstance(spec.get("export_canonical_bundle"), bool):
        errors.append("export_canonical_bundle must be boolean")
    if "canonical_bundle_dir" in spec and not isinstance(spec.get("canonical_bundle_dir"), str):
        errors.append("canonical_bundle_dir must be string")
    if "canonical_title" in spec and not isinstance(spec.get("canonical_title"), str):
        errors.append("canonical_title must be string")
    if "evidence_schema" in spec and not isinstance(spec.get("evidence_schema"), dict):
        errors.append("evidence_schema must be object")
    if "detect_conflicts" in spec and not isinstance(spec.get("detect_conflicts"), bool):
        errors.append("detect_conflicts must be boolean")
    if "chunk_mode" in spec and str(spec.get("chunk_mode")).strip().lower() not in {"none", "off", "paragraph", "sentence", "fixed"}:
        errors.append("chunk_mode must be one of none/off/paragraph/sentence/fixed")
    if "chunk_max_chars" in spec:
        try:
            if int(spec.get("chunk_max_chars")) <= 0:
                errors.append("chunk_max_chars must be > 0")
        except Exception:
            errors.append("chunk_max_chars must be integer")
    if "max_retries" in spec:
        try:
            if int(spec.get("max_retries")) < 0:
                errors.append("max_retries must be >= 0")
        except Exception:
            errors.append("max_retries must be integer")
    if "on_file_error" in spec:
        if str(spec.get("on_file_error")).strip().lower() not in {"skip", "raise"}:
            errors.append("on_file_error must be 'skip' or 'raise'")
    if "amount_round_digits" in spec:
        try:
            d = int(spec.get("amount_round_digits"))
            if d < 0 or d > 6:
                errors.append("amount_round_digits must be [0..6]")
        except Exception:
            errors.append("amount_round_digits must be integer")
    if "deduplicate_keep" in spec and str(spec.get("deduplicate_keep")).strip().lower() not in {"first", "last"}:
        errors.append("deduplicate_keep must be 'first' or 'last'")
    if "deduplicate_by" in spec and not isinstance(spec.get("deduplicate_by"), list):
        errors.append("deduplicate_by must be an array")

    allowed_input = {"", "csv", "json", "jsonl"}
    if str(spec.get("input_format") or "").strip().lower() not in allowed_input:
        errors.append("input_format must be one of csv/json/jsonl")
    allowed_output = {"", "csv", "json", "jsonl"}
    if str(spec.get("output_format") or "").strip().lower() not in allowed_output:
        errors.append("output_format must be one of csv/json/jsonl")

    if isinstance(spec.get("field_transforms"), list):
        for i, t in enumerate(spec["field_transforms"]):
            if not isinstance(t, dict):
                errors.append(f"field_transforms[{i}] must be an object")
                continue
            if "field" not in t or "op" not in t:
                errors.append(f"field_transforms[{i}] requires field and op")
                continue
            op = str(t.get("op") or "").strip().lower()
            if op and op not in field_transform_ops:
                errors.append(f"field_transforms[{i}].op must be one of {field_transform_ops}")

    if isinstance(spec.get("row_filters"), list):
        for i, f in enumerate(spec["row_filters"]):
            if not isinstance(f, dict):
                errors.append(f"row_filters[{i}] must be an object")
                continue
            if "op" not in f:
                errors.append(f"row_filters[{i}] requires op")
                continue
            op = str(f.get("op") or "").strip().lower()
            requires_field = row_filter_specs.get(op)
            if requires_field is None:
                errors.append(f"row_filters[{i}].op must be one of {sorted(row_filter_specs.keys())}")
                continue
            if requires_field and "field" not in f:
                errors.append(f"row_filters[{i}] requires field")

    known = {
        "pipeline",
        "input_format",
        "output_format",
        "input_files",
        "text_split_by_line",
        "ocr_enabled",
        "ocr_lang",
        "ocr_config",
        "ocr_preprocess",
        "xlsx_all_sheets",
        "max_retries",
        "on_file_error",
        "standardize_evidence",
        "evidence_schema",
        "generate_quality_report",
        "quality_report_path",
        "export_canonical_bundle",
        "canonical_bundle_dir",
        "canonical_title",
        "quality_required_fields",
        "chunk_mode",
        "chunk_field",
        "chunk_max_chars",
        "detect_conflicts",
        "conflict_topic_field",
        "conflict_stance_field",
        "conflict_text_field",
        "conflict_positive_words",
        "conflict_negative_words",
        "delimiter",
        "header_map",
        "null_values",
        "amount_fields",
        "date_fields",
        "amount_round_digits",
        "trim_strings",
        "drop_empty_rows",
        "date_output_format",
        "date_input_formats",
        "default_values",
        "include_fields",
        "exclude_fields",
        "field_transforms",
        "row_filters",
        "deduplicate_by",
        "deduplicate_keep",
    }
    unknown = [k for k in spec.keys() if k not in known]
    if unknown:
        warnings.append(f"unknown preprocess keys: {', '.join(sorted(unknown))}")

    pipeline = spec.get("pipeline")
    if pipeline is not None:
        if not isinstance(pipeline, dict):
            errors.append("pipeline must be object")
        else:
            stages = pipeline.get("stages")
            if not isinstance(stages, list) or not stages:
                errors.append("pipeline.stages must be a non-empty array")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def validate_preprocess_pipeline_impl(
    pipeline: Dict[str, Any],
    *,
    list_pipeline_stages: Callable[[], List[str]],
    get_pipeline_registration: Callable[[str], Any],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    if not isinstance(pipeline, dict):
        return {"ok": False, "errors": ["pipeline must be object"], "warnings": []}

    stages = pipeline.get("stages")
    if not isinstance(stages, list) or not stages:
        errors.append("pipeline.stages must be a non-empty array")
        return {"ok": False, "errors": errors, "warnings": warnings}

    for i, stage in enumerate(stages):
        if not isinstance(stage, dict):
            errors.append(f"pipeline.stages[{i}] must be object")
            continue
        name = str(stage.get("name") or "").strip().lower()
        try:
            registration = get_pipeline_registration(name)
        except KeyError:
            errors.append(f"pipeline.stages[{i}].name must be one of {list_pipeline_stages()}")
            continue
        cfg = stage.get("config") if isinstance(stage.get("config"), dict) else {}
        vr = registration.validator(cfg)
        if not vr.get("ok"):
            errors.extend([f"pipeline.stages[{i}]: {x}" for x in vr.get("errors", [])])
        warnings.extend([f"pipeline.stages[{i}]: {x}" for x in vr.get("warnings", [])])

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

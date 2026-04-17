from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from aiwf.cleaning_spec_v2 import CLEANING_SPEC_V2_VERSION, compile_cleaning_params_to_spec


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _templates_dir() -> Path:
    raw = str(os.getenv("AIWF_CLEANING_TEMPLATE_DIR") or "").strip()
    if raw:
        return Path(raw)
    return _repo_root() / "rules" / "templates"


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value or {}) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_str_list(value: Any) -> list[str]:
    return [str(item).strip() for item in _as_list(value) if str(item).strip()]


def normalize_cleaning_spec(raw: Any) -> Optional[Dict[str, Any]]:
    source = raw
    if isinstance(raw, dict) and isinstance(raw.get("cleaning_spec_v2"), dict):
        source = raw.get("cleaning_spec_v2")
    if not isinstance(source, dict):
        return None
    if str(source.get("schema_version") or "").strip() != CLEANING_SPEC_V2_VERSION:
        return None
    return {
        "schema_version": CLEANING_SPEC_V2_VERSION,
        "ingest": _as_dict(source.get("ingest")),
        "schema": _as_dict(source.get("schema")),
        "transform": _as_dict(source.get("transform")),
        "quality": _as_dict(source.get("quality")),
        "artifacts": _as_dict(source.get("artifacts")),
        "audit": _as_dict(source.get("audit")),
    }


def merge_cleaning_spec(base: Any, override: Any) -> Dict[str, Any]:
    left = normalize_cleaning_spec(base) or normalize_cleaning_spec(
        {
            "schema_version": CLEANING_SPEC_V2_VERSION,
            "ingest": {},
            "schema": {},
            "transform": {},
            "quality": {},
            "artifacts": {},
            "audit": {},
        }
    )
    right = normalize_cleaning_spec(override)
    if left is None:
        raise ValueError("internal error: default cleaning spec normalization failed")
    if right is None:
        return left
    return {
        "schema_version": CLEANING_SPEC_V2_VERSION,
        "ingest": {**_as_dict(left.get("ingest")), **_as_dict(right.get("ingest"))},
        "schema": {**_as_dict(left.get("schema")), **_as_dict(right.get("schema"))},
        "transform": {**_as_dict(left.get("transform")), **_as_dict(right.get("transform"))},
        "quality": {**_as_dict(left.get("quality")), **_as_dict(right.get("quality"))},
        "artifacts": {**_as_dict(left.get("artifacts")), **_as_dict(right.get("artifacts"))},
        "audit": {**_as_dict(left.get("audit")), **_as_dict(right.get("audit"))},
    }


def derive_legacy_rules_from_cleaning_spec(spec: Any) -> Dict[str, Any]:
    normalized = normalize_cleaning_spec(spec)
    if normalized is None:
        return {}
    transform = _as_dict(normalized.get("transform"))
    quality = _as_dict(normalized.get("quality"))
    gates = _as_dict(quality.get("gates"))
    schema = _as_dict(normalized.get("schema"))
    rules: Dict[str, Any] = {
        "rename_map": _as_dict(transform.get("rename_map")),
        "casts": _as_dict(transform.get("casts")),
        "required_fields": _as_str_list(transform.get("required_fields")),
        "default_values": _as_dict(transform.get("default_values")),
        "include_fields": _as_list(transform.get("include_fields")),
        "exclude_fields": _as_list(transform.get("exclude_fields")),
        "filters": _as_list(transform.get("filters")),
        "deduplicate_by": _as_str_list(transform.get("deduplicate_by")),
        "deduplicate_keep": str(transform.get("deduplicate_keep") or "last").strip().lower() or "last",
        "sort_by": _as_list(transform.get("sort_by")),
        "aggregate": _as_dict(transform.get("aggregate")),
        "null_values": _as_str_list(transform.get("null_values")),
        "trim_strings": bool(transform.get("trim_strings", True)),
        "computed_fields": _as_dict(transform.get("computed_fields")),
        "string_ops": _as_list(transform.get("string_ops")),
        "date_ops": _as_list(transform.get("date_ops")),
        "field_ops": _as_list(transform.get("field_ops")),
        "survivorship": _as_dict(transform.get("survivorship")),
    }
    required_fields = _as_str_list(quality.get("required_fields"))
    if required_fields and not rules["required_fields"]:
        rules["required_fields"] = required_fields
    rules.update(gates)
    canonical_profile = str(schema.get("canonical_profile") or "").strip().lower()
    if canonical_profile:
        rules["canonical_profile"] = canonical_profile
    blank_output_expected = gates.get("blank_output_expected")
    if blank_output_expected is not None:
        rules["blank_output_expected"] = blank_output_expected
    return rules


def compile_legacy_rules_to_cleaning_spec(rules: Dict[str, Any]) -> Dict[str, Any]:
    params = {"rules": dict(rules or {})}
    return compile_cleaning_params_to_spec(params)


def normalize_cleaning_template_payload(raw: Any) -> Optional[Dict[str, Any]]:
    source = copy.deepcopy(raw) if isinstance(raw, dict) else {}
    params_schema = _as_dict(source.get("params_schema"))
    metadata = {
        "template_expected_profile": str(source.get("template_expected_profile") or "").strip().lower(),
        "blank_output_expected": source.get("blank_output_expected"),
    }
    cleaning_spec = normalize_cleaning_spec(source)
    if cleaning_spec is not None:
        return {
            "template_format": "cleaning_spec_v2",
            "cleaning_spec_v2": cleaning_spec,
            "rules": derive_legacy_rules_from_cleaning_spec(cleaning_spec),
            "params_schema": params_schema,
            "metadata": metadata,
        }
    if isinstance(source.get("rules"), dict):
        compiled = compile_legacy_rules_to_cleaning_spec(_as_dict(source.get("rules")))
        return {
            "template_format": "legacy_rules",
            "cleaning_spec_v2": compiled,
            "rules": _as_dict(source.get("rules")),
            "params_schema": params_schema,
            "metadata": metadata,
        }
    return None


def _read_json_file(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8").lstrip("\ufeff")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"template file must contain an object: {path}")
    return payload


def _normalize_registry_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(entry.get("id") or "").strip().lower(),
        "file": str(entry.get("file") or "").strip(),
        "label": str(entry.get("label") or "").strip(),
        "description": str(entry.get("description") or "").strip(),
        "template_expected_profile": str(entry.get("template_expected_profile") or "").strip().lower(),
        "blank_output_expected": entry.get("blank_output_expected"),
        "template_format": str(entry.get("template_format") or "").strip(),
        "cleaning_spec_v2": _as_dict(entry.get("cleaning_spec_v2")),
        "rules": _as_dict(entry.get("rules")),
    }


def load_cleaning_template(template_id: str) -> Dict[str, Any]:
    normalized_id = str(template_id or "").strip().lower()
    if not normalized_id or normalized_id == "default":
        return {}
    templates_dir = _templates_dir()
    registry_path = templates_dir / "cleaning_templates_desktop.json"
    if not registry_path.exists():
        raise ValueError(f"cleaning template registry not found: {registry_path}")
    registry = _read_json_file(registry_path)
    templates = registry.get("templates")
    if not isinstance(templates, list):
        raise ValueError(f"cleaning template registry invalid: {registry_path}")
    matched = next(
        (
            _normalize_registry_entry(item)
            for item in templates
            if isinstance(item, dict) and str(item.get("id") or "").strip().lower() == normalized_id
        ),
        {},
    )
    if not matched:
        raise ValueError(f"unknown cleaning_template: {normalized_id}")

    payload_source: Dict[str, Any]
    if matched["file"]:
        template_path = templates_dir / matched["file"]
        if not template_path.exists():
            raise ValueError(f"cleaning template file not found: {template_path}")
        payload_source = _read_json_file(template_path)
    elif matched["cleaning_spec_v2"]:
        payload_source = {"cleaning_spec_v2": matched["cleaning_spec_v2"]}
    elif matched["rules"]:
        payload_source = {"rules": matched["rules"]}
    else:
        raise ValueError(f"cleaning template has no payload: {normalized_id}")

    payload = normalize_cleaning_template_payload(payload_source)
    if payload is None:
        raise ValueError(f"cleaning template payload invalid: {normalized_id}")

    metadata = dict(payload.get("metadata") or {})
    if matched["template_expected_profile"] and not metadata.get("template_expected_profile"):
        metadata["template_expected_profile"] = matched["template_expected_profile"]
    if matched["blank_output_expected"] is not None and metadata.get("blank_output_expected") is None:
        metadata["blank_output_expected"] = matched["blank_output_expected"]
    payload["metadata"] = metadata
    payload["template"] = {
        "id": normalized_id,
        "label": matched["label"],
        "description": matched["description"],
        "template_format": str(payload.get("template_format") or matched["template_format"] or ""),
        "file": matched["file"],
        **metadata,
    }
    return payload


def apply_cleaning_spec_to_params(
    params: Dict[str, Any],
    spec: Any,
    *,
    template_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized = normalize_cleaning_spec(spec)
    if normalized is None:
        return dict(params or {})
    next_params = dict(params or {})
    next_params["cleaning_spec_v2"] = merge_cleaning_spec(next_params.get("cleaning_spec_v2"), normalized)

    derived_rules = derive_legacy_rules_from_cleaning_spec(normalized)
    existing_rules = _as_dict(next_params.get("rules"))
    next_params["rules"] = {**derived_rules, **existing_rules}

    quality = _as_dict(normalized.get("quality"))
    gates = _as_dict(quality.get("gates"))
    quality_rules = {
        **gates,
        **(_as_dict(next_params.get("quality_rules"))),
    }
    advanced_rules = _as_dict(quality.get("advanced_rules"))
    if advanced_rules and "advanced_rules" not in _as_dict(next_params.get("quality_rules")):
        quality_rules["advanced_rules"] = advanced_rules
    required_fields = _as_str_list(quality.get("required_fields"))
    if required_fields and "required_fields" not in _as_dict(next_params.get("quality_rules")):
        quality_rules["required_fields"] = required_fields
    next_params["quality_rules"] = quality_rules
    next_params["image_rules"] = {
        **_as_dict(quality.get("image_rules")),
        **_as_dict(next_params.get("image_rules")),
    }
    next_params["xlsx_rules"] = {
        **_as_dict(quality.get("xlsx_rules")),
        **_as_dict(next_params.get("xlsx_rules")),
    }
    ingest = _as_dict(normalized.get("ingest"))
    next_params["sheet_profiles"] = {
        **_as_dict(ingest.get("sheet_profiles")),
        **_as_dict(next_params.get("sheet_profiles")),
    }
    if ingest.get("sheet_allowlist") and "sheet_allowlist" not in next_params:
        next_params["sheet_allowlist"] = _as_list(ingest.get("sheet_allowlist"))
    if "header_mapping_mode" not in next_params and str(ingest.get("header_mapping_mode") or "").strip():
        next_params["header_mapping_mode"] = str(ingest.get("header_mapping_mode") or "").strip().lower()

    canonical_profile = str(_as_dict(normalized.get("schema")).get("canonical_profile") or "").strip().lower()
    if canonical_profile and not str(next_params.get("canonical_profile") or "").strip():
        next_params["canonical_profile"] = canonical_profile

    metadata = dict(template_metadata or {})
    if metadata.get("template_expected_profile"):
        next_params["template_expected_profile"] = str(metadata["template_expected_profile"]).strip().lower()
    elif canonical_profile and not str(next_params.get("template_expected_profile") or "").strip():
        next_params["template_expected_profile"] = canonical_profile

    if metadata.get("blank_output_expected") is not None and "blank_output_expected" not in next_params:
        next_params["blank_output_expected"] = metadata.get("blank_output_expected")
    elif gates.get("blank_output_expected") is not None and "blank_output_expected" not in next_params:
        next_params["blank_output_expected"] = gates.get("blank_output_expected")

    if template_metadata:
        next_params["_resolved_cleaning_template"] = dict(template_metadata)
    return next_params


def resolve_cleaning_template_params(params: Dict[str, Any]) -> Dict[str, Any]:
    next_params = dict(params or {})
    direct_spec = next_params.get("cleaning_spec_v2")
    if isinstance(direct_spec, dict):
        next_params = apply_cleaning_spec_to_params(
            next_params,
            direct_spec,
            template_metadata={
                "id": "direct_spec",
                "template_format": "cleaning_spec_v2",
                "template_expected_profile": str(next_params.get("template_expected_profile") or "").strip().lower(),
                "blank_output_expected": next_params.get("blank_output_expected"),
            },
        )

    template_id = str(next_params.get("cleaning_template") or "").strip().lower()
    if not template_id or template_id == "default":
        return next_params

    payload = load_cleaning_template(template_id)
    template_metadata = dict(payload.get("template") or {})
    next_params = apply_cleaning_spec_to_params(
        next_params,
        payload.get("cleaning_spec_v2"),
        template_metadata=template_metadata,
    )
    template_rules = _as_dict(payload.get("rules"))
    if template_rules:
        next_params["rules"] = {
            **template_rules,
            **_as_dict(next_params.get("rules")),
        }
    return next_params

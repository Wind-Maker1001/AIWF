from __future__ import annotations

from typing import Any, Dict, List, Optional


def _to_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "on"}:
        return True
    if s in {"0", "false", "no", "off"}:
        return False
    return default


def _first_list(*values: Any) -> List[Any]:
    for value in values:
        if isinstance(value, list):
            return list(value)
    return []


def _section(root: Dict[str, Any], key: str) -> Dict[str, Any]:
    value = root.get(key)
    return value if isinstance(value, dict) else {}


def normalize_artifact_selection(params_effective: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    root = params_effective.get("artifact_selection")
    selection = root if isinstance(root, dict) else {}
    office_cfg = _section(selection, "office")
    core_cfg = _section(selection, "core")

    return {
        "office": {
            "enabled": _to_bool(office_cfg.get("enabled"), _to_bool(params_effective.get("office_outputs_enabled"), True)),
            "include": _first_list(
                office_cfg.get("include"),
                office_cfg.get("enabled_artifacts"),
                params_effective.get("enabled_office_artifacts"),
            ),
            "exclude": _first_list(
                office_cfg.get("exclude"),
                office_cfg.get("disabled_artifacts"),
                params_effective.get("disabled_office_artifacts"),
            ),
        },
        "core": {
            "enabled": _to_bool(core_cfg.get("enabled"), True),
            "include": _first_list(
                core_cfg.get("include"),
                core_cfg.get("enabled_artifacts"),
                params_effective.get("enabled_core_artifacts"),
            ),
            "exclude": _first_list(
                core_cfg.get("exclude"),
                core_cfg.get("disabled_artifacts"),
                params_effective.get("disabled_core_artifacts"),
            ),
        },
    }


def validate_artifact_selection_config(params: Dict[str, Any]) -> Dict[str, Any]:
    return validate_artifact_selection_config_with_tokens(params)


def validate_artifact_selection_config_with_tokens(
    params: Dict[str, Any],
    *,
    allowed_core_tokens: Optional[List[str]] = None,
    allowed_office_tokens: Optional[List[str]] = None,
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    selection = params.get("artifact_selection")
    if selection is None:
        return {"ok": True, "errors": errors, "warnings": warnings}
    if not isinstance(selection, dict):
        return {"ok": False, "errors": ["artifact_selection must be an object"], "warnings": warnings}

    known_root = {"office", "core"}
    unknown_root = [k for k in selection.keys() if k not in known_root]
    if unknown_root:
        warnings.append(f"unknown artifact_selection keys: {', '.join(sorted(unknown_root))}")

    for section_name in ["office", "core"]:
        section = selection.get(section_name)
        if section is None:
            continue
        if not isinstance(section, dict):
            errors.append(f"artifact_selection.{section_name} must be an object")
            continue
        known_section = {"enabled", "include", "exclude", "enabled_artifacts", "disabled_artifacts"}
        unknown_section = [k for k in section.keys() if k not in known_section]
        if unknown_section:
            warnings.append(
                f"unknown artifact_selection.{section_name} keys: {', '.join(sorted(unknown_section))}"
            )
        if "enabled" in section and not isinstance(section.get("enabled"), bool):
            errors.append(f"artifact_selection.{section_name}.enabled must be boolean")
        for key in ["include", "exclude", "enabled_artifacts", "disabled_artifacts"]:
            if key in section and not isinstance(section.get(key), list):
                errors.append(f"artifact_selection.{section_name}.{key} must be an array")

    normalized = normalize_artifact_selection(params)
    token_map = {
        "office": {str(x).strip().lower() for x in (allowed_office_tokens or []) if str(x).strip()},
        "core": {str(x).strip().lower() for x in (allowed_core_tokens or []) if str(x).strip()},
    }
    for section_name in ["office", "core"]:
        allowed_tokens = token_map[section_name]
        if not allowed_tokens:
            continue
        section = normalized[section_name]
        for key in ["include", "exclude"]:
            values = section.get(key) if isinstance(section.get(key), list) else []
            unknown = sorted({str(v).strip().lower() for v in values if str(v).strip().lower() not in allowed_tokens})
            if unknown:
                errors.append(
                    f"artifact_selection.{section_name}.{key} contains unknown artifacts: {', '.join(unknown)}"
                )

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

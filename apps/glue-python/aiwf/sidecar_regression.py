from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi.testclient import TestClient

from aiwf import rust_client
from aiwf.paths import repo_root
from aiwf.quality_contract import normalize_value_for_field


_PROFILE_NUMERIC_FIELDS = {
    "finance_statement": ["id", "amount"],
    "customer_contact": [],
    "debate_evidence": ["confidence"],
}

_PROFILE_DATE_FIELDS = {
    "finance_statement": ["biz_date", "published_at"],
    "customer_contact": [],
    "debate_evidence": ["published_at"],
}

_PROFILE_REQUIRED_FIELDS = {
    "finance_statement": ["id", "amount"],
    "customer_contact": ["customer_name", "phone"],
    "debate_evidence": ["claim_text"],
}


def _load_glue_app():
    project_root = Path(repo_root())
    module_name = "aiwf_sidecar_regression_glue_app"
    module_path = project_root / "apps" / "glue-python" / "app.py"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load glue app from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def load_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    text = Path(path).read_text(encoding="utf-8-sig")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            out.append(payload)
    return out


def load_sidecar_dataset(dataset_dir: str | Path) -> List[Dict[str, Any]]:
    dataset_root = Path(dataset_dir)
    manifest = load_json(dataset_root / "expectations.json")
    scenarios = manifest.get("scenarios") if isinstance(manifest.get("scenarios"), list) else []
    out: List[Dict[str, Any]] = []
    for item in scenarios:
        if not isinstance(item, dict):
            continue
        rel_dir = str(item.get("dir") or item.get("id") or "").strip()
        if not rel_dir:
            continue
        scenario_dir = dataset_root / rel_dir
        scenario = load_json(scenario_dir / "scenario.json")
        expected_quality = load_json(scenario_dir / "expected_quality.json")
        expected_rows = load_jsonl(scenario_dir / "expected_rows.jsonl")
        out.append(
            {
                "id": str(item.get("id") or scenario.get("id") or rel_dir),
                "dir": str(scenario_dir),
                "scenario": scenario,
                "expected_quality": expected_quality,
                "expected_rows": expected_rows,
            }
        )
    return out


def validate_ingest_extract_contract(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_top = [
        "rows",
        "file_results",
        "image_blocks",
        "table_cells",
        "sheet_frames",
        "quality_blocked",
        "blocked_inputs",
        "engine_trace",
        "header_mapping",
        "candidate_profiles",
        "quality_decisions",
        "blocked_reason_codes",
        "sample_rows",
        "contract",
    ]
    for field in required_top:
        if field not in payload:
            errors.append(f"missing top-level field: {field}")
    file_results = payload.get("file_results")
    if not isinstance(file_results, list):
        errors.append("file_results must be array")
        return errors
    for index, item in enumerate(file_results):
        if not isinstance(item, dict):
            errors.append(f"file_results[{index}] must be object")
            continue
        for field in [
            "path",
            "ok",
            "input_format",
            "rows",
            "row_count",
            "quality_blocked",
            "quality_report",
            "quality_metrics",
            "image_blocks",
            "table_cells",
            "sheet_frames",
            "engine_trace",
            "header_mapping",
            "candidate_profiles",
            "quality_decisions",
            "blocked_reason_codes",
            "sample_rows",
        ]:
            if field not in item:
                errors.append(f"missing file_result field: file_results[{index}].{field}")
    return errors


def _pick_quality_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    file_results = payload.get("file_results") if isinstance(payload.get("file_results"), list) else []
    for item in file_results:
        if isinstance(item, dict) and isinstance(item.get("quality_metrics"), dict):
            return dict(item.get("quality_metrics") or {})
    quality_metrics = payload.get("quality_metrics")
    if isinstance(quality_metrics, list):
        for item in quality_metrics:
            if isinstance(item, dict):
                return dict(item)
    return {}


def _scenario_input_paths(scenario: Dict[str, Any], scenario_dir: Path) -> List[str]:
    files = scenario.get("input_files") if isinstance(scenario.get("input_files"), list) else []
    return [str((scenario_dir / str(item)).resolve()) for item in files if str(item).strip()]


def run_sidecar_extract_for_scenario(scenario_dir: str | Path, scenario: Dict[str, Any]) -> Dict[str, Any]:
    glue_app = _load_glue_app()
    client = TestClient(glue_app.app, raise_server_exceptions=False)
    scenario_path = Path(scenario_dir)
    body = {
        "input_files": _scenario_input_paths(scenario, scenario_path),
        "ocr_enabled": bool(scenario.get("ocr_enabled", True)),
        "ocr_lang": scenario.get("ocr_lang"),
        "ocr_config": scenario.get("ocr_config"),
        "ocr_preprocess": scenario.get("ocr_preprocess"),
        "xlsx_all_sheets": bool(scenario.get("xlsx_all_sheets", True)),
        "include_hidden_sheets": bool(scenario.get("include_hidden_sheets", False)),
        "sheet_allowlist": list(scenario.get("sheet_allowlist") or []),
        "quality_rules": dict(scenario.get("quality_rules") or {}),
        "image_rules": dict(scenario.get("image_rules") or {}),
        "xlsx_rules": dict(scenario.get("xlsx_rules") or {}),
        "sheet_profiles": dict(scenario.get("sheet_profiles") or {}),
        "canonical_profile": str(scenario.get("canonical_profile") or ""),
        "on_file_error": "raise",
    }
    response = client.post("/ingest/extract", json=body)
    payload = response.json()
    return {
        "status_code": response.status_code,
        "payload": payload,
    }


def normalize_rows_for_compare(rows: Iterable[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append({field: row.get(field) for field in fields})
    return sorted(normalized, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))


def compare_expected_rows(actual_rows: List[Dict[str, Any]], expected_rows: List[Dict[str, Any]], scenario: Dict[str, Any]) -> List[str]:
    fields = scenario.get("expected_row_fields") if isinstance(scenario.get("expected_row_fields"), list) else []
    compare_fields = [str(item) for item in fields if str(item).strip()]
    if not compare_fields:
        compare_fields = sorted({key for row in expected_rows for key in row.keys()})
    actual = normalize_rows_for_compare(actual_rows, compare_fields)
    expected = normalize_rows_for_compare(expected_rows, compare_fields)
    if actual != expected:
        return [f"rows mismatch for fields: {', '.join(compare_fields)}"]
    return []


def compare_expected_quality(
    payload: Dict[str, Any],
    expected_quality: Dict[str, Any],
    scenario: Optional[Dict[str, Any]] = None,
) -> List[str]:
    metrics = _pick_quality_metrics(payload)
    errors: List[str] = []
    scenario_obj = scenario or {}
    expected_blocked = expected_quality.get("quality_blocked")
    if expected_blocked is not None and bool(payload.get("quality_blocked")) != bool(expected_blocked):
        errors.append(
            f"quality_blocked={bool(payload.get('quality_blocked'))} != expected {bool(expected_blocked)}"
        )
    engine_trace = payload.get("engine_trace") if isinstance(payload.get("engine_trace"), list) else []
    engine_ok_any_of = scenario_obj.get("engine_ok_any_of") if isinstance(scenario_obj.get("engine_ok_any_of"), list) else []
    if engine_ok_any_of:
        ok_engines = {
            str(item.get("engine") or "").strip()
            for item in engine_trace
            if isinstance(item, dict) and bool(item.get("ok"))
        }
        expected_engines = {str(item) for item in engine_ok_any_of if str(item).strip()}
        if not ok_engines.intersection(expected_engines):
            errors.append(
                f"engine_ok_any_of not satisfied: expected one of {sorted(expected_engines)}, got {sorted(ok_engines)}"
            )
    if "required_fields" in expected_quality and isinstance(expected_quality.get("required_fields"), list):
        required_missing = metrics.get("required_field_missing") if isinstance(metrics.get("required_field_missing"), dict) else {}
        for field in expected_quality["required_fields"]:
            if str(field) not in required_missing:
                errors.append(f"required_field_missing missing key: {field}")
    for key, value in expected_quality.items():
        if key == "quality_blocked" or key == "required_fields":
            continue
        if key == "table_cells_min":
            if len(payload.get("table_cells") or []) < int(value):
                errors.append(f"table_cells {len(payload.get('table_cells') or [])} < {int(value)}")
            continue
        if key == "sheet_frames_min":
            if len(payload.get("sheet_frames") or []) < int(value):
                errors.append(f"sheet_frames {len(payload.get('sheet_frames') or [])} < {int(value)}")
            continue
        metric_key = key
        comparator = None
        if key.endswith("_min"):
            metric_key = key[: -len("_min")]
            comparator = "min"
        elif key.endswith("_max"):
            metric_key = key[: -len("_max")]
            comparator = "max"
        actual_value = metrics.get(metric_key)
        if actual_value is None:
            errors.append(f"quality metric missing: {metric_key}")
            continue
        try:
            actual_number = float(actual_value)
            expected_number = float(value)
        except Exception:
            if actual_value != value:
                errors.append(f"{metric_key}={actual_value} != {value}")
            continue
        if comparator == "min" and actual_number < expected_number:
            errors.append(f"{metric_key}={actual_number:.6f} < {expected_number:.6f}")
        elif comparator == "max" and actual_number > expected_number:
            errors.append(f"{metric_key}={actual_number:.6f} > {expected_number:.6f}")
        elif comparator is None and actual_number != expected_number:
            errors.append(f"{metric_key}={actual_number:.6f} != {expected_number:.6f}")
    return errors


def _resolve_required_fields(scenario: Dict[str, Any]) -> List[str]:
    quality_rules = scenario.get("quality_rules") if isinstance(scenario.get("quality_rules"), dict) else {}
    if isinstance(quality_rules.get("required_fields"), list):
        return [str(item) for item in quality_rules["required_fields"] if str(item).strip()]
    profile = str(scenario.get("canonical_profile") or "").strip().lower()
    return list(_PROFILE_REQUIRED_FIELDS.get(profile, []))


def _resolve_duplicate_fields(scenario: Dict[str, Any]) -> List[str]:
    quality_rules = scenario.get("quality_rules") if isinstance(scenario.get("quality_rules"), dict) else {}
    if isinstance(quality_rules.get("unique_keys"), list):
        return [str(item) for item in quality_rules["unique_keys"] if str(item).strip()]
    if isinstance(quality_rules.get("deduplicate_by"), list):
        return [str(item) for item in quality_rules["deduplicate_by"] if str(item).strip()]
    profile = str(scenario.get("canonical_profile") or "").strip().lower()
    if profile == "finance_statement":
        return ["id"]
    if profile == "customer_contact":
        return ["phone"]
    return []


def compute_common_python_metrics(rows: List[Dict[str, Any]], scenario: Dict[str, Any]) -> Dict[str, float]:
    profile = str(scenario.get("canonical_profile") or "").strip().lower()
    required_fields = _resolve_required_fields(scenario)
    duplicate_fields = _resolve_duplicate_fields(scenario)
    numeric_fields = scenario.get("numeric_fields") if isinstance(scenario.get("numeric_fields"), list) else _PROFILE_NUMERIC_FIELDS.get(profile, [])
    date_fields = scenario.get("date_fields") if isinstance(scenario.get("date_fields"), list) else _PROFILE_DATE_FIELDS.get(profile, [])

    required_missing_cells = 0
    for field in required_fields:
        for row in rows:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                required_missing_cells += 1
    required_total = len(rows) * len(required_fields) if required_fields else 0
    required_missing_ratio = (required_missing_cells / required_total) if required_total > 0 else 0.0

    numeric_total = 0
    numeric_parsed = 0
    for row in rows:
        for field in numeric_fields:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                continue
            numeric_total += 1
            normalized = normalize_value_for_field(value, field)
            try:
                float(str(normalized).replace(",", ""))
                numeric_parsed += 1
            except Exception:
                pass
    numeric_parse_rate = (numeric_parsed / numeric_total) if numeric_total > 0 else 1.0

    date_total = 0
    date_parsed = 0
    for row in rows:
        for field in date_fields:
            value = row.get(field)
            if value is None or str(value).strip() == "":
                continue
            date_total += 1
            normalized = normalize_value_for_field(value, field)
            if normalized not in {None, ""}:
                date_parsed += 1
    date_parse_rate = (date_parsed / date_total) if date_total > 0 else 1.0

    duplicate_key_ratio = 0.0
    if duplicate_fields:
        keys = [tuple(row.get(field) for field in duplicate_fields) for row in rows]
        keys = [item for item in keys if any(part not in {None, ""} for part in item)]
        if keys:
            duplicate_key_ratio = 1.0 - (len(set(keys)) / len(keys))

    return {
        "required_missing_ratio": round(required_missing_ratio, 6),
        "numeric_parse_rate": round(numeric_parse_rate, 6),
        "date_parse_rate": round(date_parse_rate, 6),
        "duplicate_key_ratio": round(duplicate_key_ratio, 6),
        "gate_pass": 1.0,
    }


def _rust_available(accel_url: str) -> bool:
    try:
        rust_client.health(base_url=accel_url, timeout=2.0)
        return True
    except Exception:
        return False


def _expected_reason_counts(rows: List[Dict[str, Any]], rust_rules: Dict[str, Any]) -> Dict[str, int]:
    counts = {
        "invalid_object": 0,
        "cast_failed": 0,
        "required_missing": 0,
        "filter_rejected": 0,
        "duplicate_removed": 0,
    }
    casts = rust_rules.get("casts") if isinstance(rust_rules.get("casts"), dict) else {}
    required_fields = [str(item) for item in (rust_rules.get("required_fields") or [])]
    filters = rust_rules.get("filters") if isinstance(rust_rules.get("filters"), list) else []
    deduplicate_by = [str(item) for item in (rust_rules.get("deduplicate_by") or [])]
    deduplicate_keep = str(rust_rules.get("deduplicate_keep") or "last").strip().lower()

    passed_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            counts["invalid_object"] += 1
            continue
        cast_failed = False
        normalized = dict(row)
        for field, kind in casts.items():
            if field not in normalized:
                continue
            value = normalized.get(field)
            normalized_value = normalize_value_for_field(value, field)
            kind_text = str(kind or "").strip().lower()
            if kind_text in {"int", "integer"}:
                try:
                    normalized[field] = int(float(str(normalized_value)))
                except Exception:
                    cast_failed = True
                    break
            elif kind_text in {"float", "double", "number", "decimal"}:
                try:
                    normalized[field] = float(str(normalized_value).replace(",", ""))
                except Exception:
                    cast_failed = True
                    break
        if cast_failed:
            counts["cast_failed"] += 1
            continue
        if required_fields and any(normalized.get(field) in {None, ""} for field in required_fields):
            counts["required_missing"] += 1
            continue
        if filters:
            filter_failed = False
            for item in filters:
                if not isinstance(item, dict):
                    continue
                field = str(item.get("field") or "").strip()
                op = str(item.get("op") or "").strip().lower()
                value = normalized.get(field)
                target = item.get("value")
                text_value = "" if value is None else str(value)
                if op == "exists":
                    ok = value not in {None, ""}
                elif op == "not_exists":
                    ok = value in {None, ""}
                elif op == "eq":
                    ok = text_value == str(target)
                elif op == "ne":
                    ok = text_value != str(target)
                elif op == "contains":
                    ok = str(target) in text_value
                elif op == "in":
                    ok = text_value in [str(x) for x in (target or [])]
                elif op == "not_in":
                    ok = text_value not in [str(x) for x in (target or [])]
                elif op == "regex":
                    ok = bool(re.search(str(target), text_value))
                elif op == "not_regex":
                    ok = not bool(re.search(str(target), text_value))
                elif op in {"gt", "gte", "lt", "lte"}:
                    try:
                        left = float(text_value)
                        right = float(target)
                    except Exception:
                        ok = False
                    else:
                        if op == "gt":
                            ok = left > right
                        elif op == "gte":
                            ok = left >= right
                        elif op == "lt":
                            ok = left < right
                        else:
                            ok = left <= right
                else:
                    ok = True
                if not ok:
                    filter_failed = True
                    break
            if filter_failed:
                counts["filter_rejected"] += 1
                continue
        passed_rows.append(normalized)

    if deduplicate_by:
        seen = {}
        if deduplicate_keep == "first":
            for row in passed_rows:
                key = tuple(row.get(field) for field in deduplicate_by)
                if key in seen:
                    counts["duplicate_removed"] += 1
                    continue
                seen[key] = row
        else:
            for row in passed_rows:
                key = tuple(row.get(field) for field in deduplicate_by)
                if key in seen:
                    counts["duplicate_removed"] += 1
                seen[key] = row
    return counts


def run_python_rust_consistency(scenario_dir: str | Path, scenario: Dict[str, Any], accel_url: str) -> Dict[str, Any]:
    result = run_sidecar_extract_for_scenario(scenario_dir, scenario)
    if result["status_code"] != 200:
        return {"ok": False, "status": "failed", "error": f"sidecar extract http {result['status_code']}"}
    payload = result["payload"]
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    if not _rust_available(accel_url):
        return {"ok": True, "status": "skipped", "reason": "rust service unavailable"}
    quality_rules = dict(scenario.get("quality_rules") or {})
    rust_rules = dict(scenario.get("rust_rules") or {})
    if not rust_rules:
        if quality_rules.get("canonical_profile") == "finance_statement":
            rust_rules = {
                "casts": {"id": "int", "amount": "float"},
                "deduplicate_by": ["id"],
                "deduplicate_keep": "last",
                "date_ops": [{"field": "biz_date", "op": "parse_ymd", "as": "biz_date_norm"}],
            }
    rust_gates = {}
    if "required_fields" in quality_rules:
        rust_gates["required_fields"] = quality_rules.get("required_fields")
    for key in [
        "max_required_missing_ratio",
        "max_duplicate_rows_removed",
        "allow_empty_output",
        "numeric_parse_rate_min",
        "date_parse_rate_min",
        "duplicate_key_ratio_max",
        "blank_row_ratio_max",
    ]:
        if key in quality_rules:
            rust_gates[key] = quality_rules[key]
    rust_out = rust_client.transform_rows_v2(
        rows=rows,
        rules=rust_rules,
        quality_gates=rust_gates,
        schema_hint={"source": "sidecar_regression", "audit": {"sample_limit": 5}},
        base_url=accel_url,
        timeout=10.0,
    )
    if not bool(rust_out.get("ok")):
        return {"ok": False, "status": "failed", "error": str(rust_out.get("error") or "rust transform failed")}
    python_metrics = compute_common_python_metrics(rows, scenario)
    rust_quality = rust_out.get("quality") if isinstance(rust_out.get("quality"), dict) else {}
    mismatches: List[str] = []
    for key in ["required_missing_ratio", "numeric_parse_rate", "date_parse_rate", "duplicate_key_ratio"]:
        if key not in rust_quality:
            mismatches.append(f"rust quality missing {key}")
            continue
        try:
            rust_value = round(float(rust_quality[key]), 6)
            python_value = round(float(python_metrics[key]), 6)
        except Exception:
            mismatches.append(f"cannot compare metric {key}")
            continue
        if abs(rust_value - python_value) > 0.000001:
            mismatches.append(f"{key}: python={python_value:.6f} rust={rust_value:.6f}")
    gate_pass = rust_out.get("gate_result", {}).get("passed") if isinstance(rust_out.get("gate_result"), dict) else None
    if gate_pass is not True:
        mismatches.append(f"rust gate failed: {rust_out.get('gate_result')}")
    rust_audit = rust_out.get("audit") if isinstance(rust_out.get("audit"), dict) else {}
    rust_reason_counts = rust_audit.get("reason_counts") if isinstance(rust_audit.get("reason_counts"), dict) else {}
    rust_reason_samples = rust_audit.get("reason_samples") if isinstance(rust_audit.get("reason_samples"), dict) else {}
    expected_reason_counts = _expected_reason_counts(rows, rust_rules)
    if set(rust_reason_counts.keys()) != set(expected_reason_counts.keys()):
        mismatches.append(
            f"reason_counts keys mismatch: python={sorted(expected_reason_counts.keys())} rust={sorted(rust_reason_counts.keys())}"
        )
    for key, python_count in expected_reason_counts.items():
        rust_count = rust_reason_counts.get(key)
        if rust_count is None:
            mismatches.append(f"rust reason_counts missing {key}")
            continue
        if int(rust_count) != int(python_count):
            mismatches.append(f"reason_counts[{key}]: python={python_count} rust={rust_count}")
        samples = rust_reason_samples.get(key) if isinstance(rust_reason_samples.get(key), list) else []
        if len(samples) > 5:
            mismatches.append(f"reason_samples[{key}] exceeds sample_limit")
        if int(rust_count) < len(samples):
            mismatches.append(f"reason_samples[{key}] larger than count")
    return {
        "ok": len(mismatches) == 0,
        "status": "passed" if len(mismatches) == 0 else "failed",
        "python_metrics": python_metrics,
        "rust_quality": rust_quality,
        "rust_audit": rust_audit,
        "rust_audit_reason_counts": rust_reason_counts,
        "expected_reason_counts": expected_reason_counts,
        "mismatches": mismatches,
    }


def evaluate_consistency_report(items: List[Dict[str, Any]], require_accel: bool = False) -> Dict[str, Any]:
    failed: List[str] = []
    skipped: List[str] = []
    for item in items:
        scenario_id = str(item.get("id") or "")
        status = str(item.get("status") or "")
        if status == "failed":
            failed.append(scenario_id)
        elif status == "skipped":
            skipped.append(scenario_id)
    ok = len(failed) == 0 and (not require_accel or len(skipped) == 0)
    return {
        "ok": ok,
        "failed": failed,
        "skipped": skipped,
    }

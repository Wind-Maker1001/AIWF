from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional


def resolve_base_url(s: Optional[Any], base: Optional[Any]) -> str:
    if s is not None and getattr(s, "base_url", None):
        return str(getattr(s, "base_url"))
    if base is not None and getattr(base, "base_url", None):
        return str(getattr(base, "base_url"))
    return "http://127.0.0.1:18080"


def prepare_job_layout(
    job_id: str,
    params: Dict[str, Any],
    *,
    ensure_dirs: Callable[..., Any],
) -> Dict[str, str]:
    bus_root = str(os.environ.get("AIWF_BUS", "")).strip() or os.path.join(os.path.expanduser("~"), "AIWF")
    job_root = params.get("job_root") or os.path.join(bus_root, "jobs", job_id)
    stage_dir = os.path.join(job_root, "stage")
    artifacts_dir = os.path.join(job_root, "artifacts")
    evidence_dir = os.path.join(job_root, "evidence")
    ensure_dirs(stage_dir, artifacts_dir, evidence_dir)
    input_uri = params.get("input_uri") or (job_root + "\\")
    output_uri = params.get("output_uri") or (job_root + "\\")
    return {
        "bus_root": bus_root,
        "job_root": job_root,
        "stage_dir": stage_dir,
        "artifacts_dir": artifacts_dir,
        "evidence_dir": evidence_dir,
        "input_uri": input_uri,
        "output_uri": output_uri,
    }


def prepare_local_clean_cache(
    params_effective: Dict[str, Any],
    job_root: str,
    *,
    load_raw_rows: Callable[..., Any],
    clean_rows: Callable[..., Any],
    rules_dict: Callable[..., Any],
) -> Dict[str, Any]:
    raw_rows, source = load_raw_rows(params_effective, job_root)
    cleaned_local = clean_rows(raw_rows, params_effective)
    local_rows = cleaned_local["rows"]
    local_quality = cleaned_local["quality"]
    params_for_accel = dict(params_effective)
    params_for_accel["rows"] = local_rows
    params_for_accel["rules"] = rules_dict(params_effective)
    return {
        "raw_rows": raw_rows,
        "source": source,
        "local_rows": local_rows,
        "local_quality": local_quality,
        "params_for_accel": params_for_accel,
    }


def prepare_accel_result(
    *,
    params_effective: Dict[str, Any],
    params_for_accel: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: str,
    output_uri: str,
    to_bool: Callable[..., bool],
    rule_param: Callable[..., Any],
    is_generic_rules_enabled: Callable[..., bool],
    try_accel_cleaning: Callable[..., Any],
    is_valid_parquet_file: Callable[..., bool],
) -> Dict[str, Any]:
    if to_bool(rule_param(params_effective, "force_local_cleaning", False), default=False) or is_generic_rules_enabled(params_effective):
        accel = {
            "attempted": False,
            "ok": False,
            "error": "accel skipped for generic/local-only cleaning mode",
        }
    else:
        accel = try_accel_cleaning(
            params=params_for_accel,
            job_id=job_id,
            step_id=step_id,
            actor=actor,
            ruleset_version=ruleset_version,
            input_uri=input_uri,
            output_uri=output_uri,
        )

    accel_resp = accel.get("response") if isinstance(accel, dict) else {}
    accel_outputs = accel_resp.get("outputs") if isinstance(accel_resp, dict) else {}
    accel_profile = accel_resp.get("profile") if isinstance(accel_resp, dict) else {}

    accel_parquet_path = ""
    if isinstance(accel_outputs, dict):
        parquet_obj = accel_outputs.get("cleaned_parquet") or {}
        if isinstance(parquet_obj, dict):
            accel_parquet_path = str(parquet_obj.get("path", ""))
    accel_parquet_valid = is_valid_parquet_file(accel_parquet_path)

    accel_validation_error = None
    if accel.get("ok") and isinstance(accel_outputs, dict) and isinstance(accel_outputs.get("cleaned_parquet"), dict):
        if not accel_parquet_valid:
            accel_validation_error = f"invalid parquet from accel output: {accel_parquet_path or '<empty path>'}"

    use_accel_outputs = (
        accel.get("ok")
        and isinstance(accel_outputs, dict)
        and isinstance(accel_outputs.get("cleaned_parquet"), dict)
        and accel_parquet_valid
    )

    return {
        "accel": accel,
        "accel_resp": accel_resp,
        "accel_outputs": accel_outputs,
        "accel_profile": accel_profile,
        "accel_validation_error": accel_validation_error,
        "use_accel_outputs": use_accel_outputs,
    }


def materialize_office_outputs(
    *,
    job_id: str,
    artifacts_dir: str,
    params_effective: Dict[str, Any],
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    profile_source: str,
    office_rows_subset: Callable[..., Any],
    build_profile: Callable[..., Any],
    write_profile_illustration_png: Callable[..., Any],
    write_fin_xlsx: Callable[..., Any],
    write_audit_docx: Callable[..., Any],
    write_deck_pptx: Callable[..., Any],
    sha256_file: Callable[..., str],
) -> Dict[str, Any]:
    xlsx_path = os.path.join(artifacts_dir, "fin.xlsx")
    docx_path = os.path.join(artifacts_dir, "audit.docx")
    pptx_path = os.path.join(artifacts_dir, "deck.pptx")
    illustration_path = os.path.join(artifacts_dir, "summary_visual.png")

    office_rows, office_truncated = office_rows_subset(rows, params_effective)
    office_profile = build_profile(office_rows, quality, profile_source)
    office_profile["office_rows_truncated"] = office_truncated
    office_profile["office_rows_used"] = len(office_rows)

    write_profile_illustration_png(illustration_path, office_profile, params_effective)
    write_fin_xlsx(xlsx_path, office_rows, illustration_path, params_effective)
    write_audit_docx(docx_path, job_id, office_profile, illustration_path, params_effective)
    write_deck_pptx(pptx_path, job_id, office_profile, illustration_path, params_effective)

    return {
        "xlsx_path": xlsx_path,
        "docx_path": docx_path,
        "pptx_path": pptx_path,
        "sha_xlsx": sha256_file(xlsx_path),
        "sha_docx": sha256_file(docx_path),
        "sha_pptx": sha256_file(pptx_path),
        "office_profile": office_profile,
    }


def materialize_accel_outputs(
    *,
    job_id: str,
    artifacts_dir: str,
    params_effective: Dict[str, Any],
    local_rows: List[Dict[str, Any]],
    local_quality: Dict[str, Any],
    accel_outputs: Dict[str, Any],
    accel_profile: Dict[str, Any],
    sha256_file: Callable[..., str],
    materialize_office_outputs_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    csv_obj = accel_outputs.get("cleaned_csv") or {}
    parquet_obj = accel_outputs.get("cleaned_parquet") or {}
    profile_obj = accel_outputs.get("profile_json") or {}

    cleaned_csv = str(csv_obj.get("path", ""))
    cleaned_parquet = str(parquet_obj.get("path", ""))
    profile_json = str(profile_obj.get("path", ""))

    out = {
        "cleaned_csv": cleaned_csv,
        "cleaned_parquet": cleaned_parquet,
        "profile_json": profile_json,
        "sha_csv": str(csv_obj.get("sha256") or (sha256_file(cleaned_csv) if cleaned_csv else "")),
        "sha_parquet": str(parquet_obj.get("sha256") or (sha256_file(cleaned_parquet) if cleaned_parquet else "")),
        "sha_profile": str(profile_obj.get("sha256") or (sha256_file(profile_json) if profile_json else "")),
        "profile": {
            "rows": int((accel_profile or {}).get("rows", 0)),
            "cols": int((accel_profile or {}).get("cols", 2)),
            "source": "accel",
        },
    }
    out.update(
        materialize_office_outputs_fn(
            job_id=job_id,
            artifacts_dir=artifacts_dir,
            params_effective=params_effective,
            rows=local_rows,
            quality=local_quality,
            profile_source="accel+local_office",
        )
    )
    return out


def materialize_local_outputs(
    *,
    job_id: str,
    stage_dir: str,
    artifacts_dir: str,
    evidence_dir: str,
    params_effective: Dict[str, Any],
    rows: List[Dict[str, Any]],
    quality: Dict[str, Any],
    source: str,
    preprocess_result: Optional[Dict[str, Any]],
    apply_quality_gates: Callable[..., Any],
    to_bool: Callable[..., bool],
    rule_param: Callable[..., Any],
    require_local_parquet_dependencies: Callable[..., Any],
    write_cleaned_csv: Callable[..., Any],
    write_cleaned_parquet: Callable[..., Any],
    is_valid_parquet_file: Callable[..., bool],
    local_parquet_strict_enabled: Callable[..., bool],
    build_profile: Callable[..., Any],
    write_profile_json: Callable[..., Any],
    sha256_file: Callable[..., str],
    materialize_office_outputs_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    quality_gate = apply_quality_gates(quality, params_effective)
    if not rows and not to_bool(rule_param(params_effective, "allow_empty_output", True), default=True):
        raise RuntimeError("cleaning produced empty result")

    cleaned_csv = os.path.join(stage_dir, "cleaned.csv")
    cleaned_parquet = os.path.join(stage_dir, "cleaned.parquet")
    profile_json = os.path.join(evidence_dir, "profile.json")

    require_local_parquet_dependencies(params_effective)
    write_cleaned_csv(cleaned_csv, rows)
    write_cleaned_parquet(cleaned_parquet, rows)
    parquet_valid_local = is_valid_parquet_file(cleaned_parquet)
    if not parquet_valid_local and local_parquet_strict_enabled(params_effective):
        raise RuntimeError(
            "local parquet generation invalid; strict mode enabled (set local_parquet_strict=false to bypass)"
        )

    profile = build_profile(rows, quality, source)
    profile["quality_gate"] = quality_gate
    profile["preprocess"] = preprocess_result
    write_profile_json(profile_json, profile, params_effective)

    out = {
        "cleaned_csv": cleaned_csv,
        "cleaned_parquet": cleaned_parquet,
        "profile_json": profile_json,
        "sha_csv": sha256_file(cleaned_csv),
        "sha_parquet": sha256_file(cleaned_parquet),
        "sha_profile": sha256_file(profile_json),
        "profile": profile,
    }
    out.update(
        materialize_office_outputs_fn(
            job_id=job_id,
            artifacts_dir=artifacts_dir,
            params_effective=params_effective,
            rows=rows,
            quality=quality,
            profile_source=source,
        )
    )
    return out

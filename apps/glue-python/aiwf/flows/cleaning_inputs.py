from __future__ import annotations

import csv
import io
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from aiwf.paths import resolve_path_within_root


def local_parquet_strict_enabled_impl(
    params: Dict[str, Any],
    *,
    rules_dict: Callable[[Dict[str, Any]], Dict[str, Any]],
    to_bool: Callable[[Any, bool], bool],
    rule_param: Callable[[Dict[str, Any], str, Any], Any],
) -> bool:
    if "local_parquet_strict" in params or "local_parquet_strict" in rules_dict(params):
        return to_bool(rule_param(params, "local_parquet_strict", True), default=True)
    return to_bool(os.getenv("AIWF_GLUE_LOCAL_PARQUET_STRICT", "true"), default=True)


def require_local_parquet_dependencies_impl(
    params: Dict[str, Any],
    *,
    local_parquet_strict_enabled: Callable[[Dict[str, Any]], bool],
) -> None:
    if not local_parquet_strict_enabled(params):
        return
    try:
        import pandas as _  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"local parquet strict mode: missing pandas ({exc})")
    try:
        import pyarrow as _  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"local parquet strict mode: missing pyarrow ({exc})")


def resolve_csv_source_path_impl(
    params: Dict[str, Any],
    job_root: Optional[str],
    *,
    resolve_path: Callable[[str, str, bool], str],
) -> Optional[str]:
    candidate = (
        params.get("input_csv_path")
        or params.get("source_csv_path")
        or params.get("csv_path")
        or params.get("input_uri")
    )
    if not candidate:
        return None
    path = str(candidate).strip()
    if not path:
        return None
    if job_root:
        return resolve_path(job_root, path, True)
    if path.startswith("file://"):
        path = path[len("file://") :]
    return os.path.normpath(os.path.abspath(path)) if os.path.isabs(path) else path


def parse_rows_from_csv_text_impl(csv_text: str) -> List[Dict[str, Any]]:
    if not csv_text or not csv_text.strip():
        return []
    out: List[Dict[str, Any]] = []
    reader = csv.DictReader(csv_text.strip().splitlines())
    for row in reader:
        out.append(dict(row))
    return out


def read_text_file_with_fallback_impl(path: str, encodings: Optional[List[str]] = None) -> str:
    tried = encodings or ["utf-8-sig", "utf-8", "gb18030", "gbk"]
    last_err: Optional[Exception] = None
    for enc in tried:
        try:
            with open(path, "r", encoding=enc, newline="") as file:
                return file.read()
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"failed to decode text file with fallback encodings: {path}, last_err={last_err}")


def load_raw_rows_impl(
    params: Dict[str, Any],
    job_root: Optional[str],
    *,
    resolve_csv_source_path: Callable[[Dict[str, Any], Optional[str]], Optional[str]],
    parse_rows_from_csv_text: Callable[[str], List[Dict[str, Any]]],
    read_text_file_with_fallback: Callable[[str, Optional[List[str]]], str],
) -> Tuple[List[Dict[str, Any]], str]:
    if isinstance(params.get("rows"), list):
        if params["rows"]:
            return list(params["rows"]), "params.rows"
        raise RuntimeError("params.rows is empty")

    if "csv_text" in params:
        csv_text = params.get("csv_text")
        if not isinstance(csv_text, str) or not csv_text.strip():
            raise RuntimeError("params.csv_text is empty")
        rows = parse_rows_from_csv_text(csv_text)
        if rows:
            return rows, "params.csv_text"
        raise RuntimeError("params.csv_text does not contain any data rows")

    source_path = resolve_csv_source_path(params, job_root)
    if source_path:
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"input csv file not found: {source_path}")
        csv_text_file = read_text_file_with_fallback(source_path, None)
        with io.StringIO(csv_text_file) as file:
            reader = csv.DictReader(file)
            rows = [dict(row) for row in reader]
        if rows:
            return rows, source_path
        raise RuntimeError(f"input csv file has no data rows: {source_path}")

    raise RuntimeError("no input rows provided; expected params.rows, params.csv_text, or input_csv_path")


def maybe_preprocess_input_impl(
    params: Dict[str, Any],
    job_root: str,
    stage_dir: str,
    *,
    to_bool: Callable[[Any, bool], bool],
    resolve_path: Callable[[str, str, bool], str],
    preprocess_csv_file: Callable[[str, str, Dict[str, Any]], Dict[str, Any]],
    run_preprocess_pipeline: Callable[..., Dict[str, Any]],
    validate_preprocess_pipeline: Callable[[Dict[str, Any]], Dict[str, Any]],
    validate_preprocess_spec: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    preprocess_cfg = params.get("preprocess") if isinstance(params.get("preprocess"), dict) else {}
    enabled = to_bool(preprocess_cfg.get("enabled"), default=False)
    input_csv = preprocess_cfg.get("input_path") or params.get("input_csv_path")
    if not enabled or not input_csv:
        return params, None

    input_path = resolve_path(job_root, str(input_csv), True)
    output_path = resolve_path_within_root(
        job_root,
        str(preprocess_cfg.get("output_path") or os.path.join(stage_dir, "preprocessed_input.csv")),
    )

    pipeline_cfg = preprocess_cfg.get("pipeline") if isinstance(preprocess_cfg.get("pipeline"), dict) else None
    if pipeline_cfg and pipeline_cfg.get("enabled", True):
        pipeline = dict(pipeline_cfg)
        pipeline.pop("enabled", None)
        validation = validate_preprocess_pipeline(pipeline)
        if not validation.get("ok"):
            raise RuntimeError(f"preprocess pipeline invalid: {validation.get('errors')}")
        result = run_preprocess_pipeline(
            pipeline=pipeline,
            job_root=job_root,
            stage_dir=stage_dir,
            input_path=input_path,
            final_output_path=output_path,
        )
        output_path = result.get("output_path", output_path)
    else:
        spec = dict(preprocess_cfg)
        spec.pop("enabled", None)
        spec.pop("input_path", None)
        spec.pop("output_path", None)
        validation = validate_preprocess_spec(spec)
        if not validation.get("ok"):
            raise RuntimeError(f"preprocess config invalid: {validation.get('errors')}")
        result = preprocess_csv_file(input_path, output_path, spec)

    next_params = dict(params)
    next_params["input_csv_path"] = output_path
    return next_params, result

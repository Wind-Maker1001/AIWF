from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Tuple

from aiwf.paths import resolve_path_within_root


def preprocess_file_impl(
    input_path: str,
    output_path: str,
    spec: Dict[str, Any],
    *,
    read_rows: Callable[[str, Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]],
    preprocess_rows: Callable[[List[Dict[str, Any]], Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]],
    write_rows: Callable[[str, List[Dict[str, Any]], Dict[str, Any]], str],
    build_quality_report: Callable[[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    write_json: Callable[[str, List[Dict[str, Any]]], None],
    export_canonical_bundle: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    rows, meta = read_rows(input_path, spec)
    if bool(meta.get("quality_blocked")):
        raise RuntimeError(str(meta.get("quality_error") or "input quality blocked"))
    out_rows, summary = preprocess_rows(rows, spec)
    output_dir = resolve_path_within_root(
        os.path.dirname(output_path) or ".",
        ".",
    )
    report = build_quality_report(out_rows, summary, {**spec, "_input_meta": meta, "_input_rows": rows})
    quality_report_path = None
    if bool(spec.get("generate_quality_report", False)):
        quality_report_path = resolve_path_within_root(
            output_dir,
            str(spec.get("quality_report_path") or f"{os.path.basename(output_path)}.quality.json"),
        )
        write_json(quality_report_path, report)
    if bool(spec.get("generate_quality_report", False)) and bool(report.get("blocked")):
        errors = [str(item) for item in (report.get("errors") or []) if str(item).strip()]
        raise RuntimeError("; ".join(errors) or "preprocess quality blocked")
    out_fmt = write_rows(output_path, out_rows, spec)
    canonical_bundle = None
    if bool(spec.get("export_canonical_bundle", False)):
        canonical_bundle = export_canonical_bundle(
            rows=out_rows,
            summary=summary,
            meta=meta,
            output_path=output_path,
            spec=spec,
        )
    return {
        "input_path": input_path,
        "output_path": output_path,
        "input_format": meta.get("input_format"),
        "output_format": out_fmt,
        "delimiter": meta.get("delimiter"),
        "skipped_files": meta.get("skipped_files"),
        "failed_files": meta.get("failed_files"),
        "file_results": meta.get("file_results"),
        "blocked_inputs": meta.get("blocked_inputs"),
        "quality_report_path": quality_report_path,
        "canonical_bundle": canonical_bundle,
        "summary": summary,
    }

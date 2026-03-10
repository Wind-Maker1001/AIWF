from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


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
    out_rows, summary = preprocess_rows(rows, spec)
    out_fmt = write_rows(output_path, out_rows, spec)
    quality_report_path = None
    if bool(spec.get("generate_quality_report", False)):
        quality_report_path = str(spec.get("quality_report_path") or f"{output_path}.quality.json")
        report = build_quality_report(out_rows, summary, spec)
        write_json(quality_report_path, report)
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
        "quality_report_path": quality_report_path,
        "canonical_bundle": canonical_bundle,
        "summary": summary,
    }

from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from aiwf import ingest


def _attach_source_lineage(rows: List[Dict[str, Any]], *, path: str, input_format: str) -> List[Dict[str, Any]]:
    source_path = os.path.abspath(path)
    source_file = os.path.basename(source_path)
    enriched: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        payload = dict(row)
        payload.setdefault("source_path", source_path)
        payload.setdefault("source_file", source_file)
        payload.setdefault("source_type", input_format)
        payload.setdefault("row_index", index)
        enriched.append(payload)
    return enriched


def _detect_input_format(path: str, spec: Dict[str, Any]) -> str:
    fmt = str(spec.get("input_format") or "").strip().lower()
    if fmt in {"csv", "json", "jsonl"}:
        return fmt
    ext = os.path.splitext(path)[1].lower()
    if ext in {".json"}:
        return "json"
    if ext in {".jsonl", ".ndjson"}:
        return "jsonl"
    return "csv"


def _detect_output_format(path: str, spec: Dict[str, Any]) -> str:
    fmt = str(spec.get("output_format") or "").strip().lower()
    if fmt in {"csv", "json", "jsonl"}:
        return fmt
    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        return "json"
    if ext in {".jsonl", ".ndjson"}:
        return "jsonl"
    return "csv"


def _read_csv(path: str, delimiter: Optional[str] = None) -> Tuple[List[Dict[str, Any]], str]:
    if delimiter is None:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","
            reader = csv.DictReader(f, delimiter=delimiter)
            return [dict(r) for r in reader], delimiter

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [dict(r) for r in reader], delimiter


def _read_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            return [dict(x) for x in payload["rows"] if isinstance(x, dict)]
        return [payload]
    return []


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _read_rows(path: str, spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    input_files = spec.get("input_files") if isinstance(spec.get("input_files"), list) else []
    if input_files:
        abs_files = [str(x) for x in input_files]
        rows, meta = ingest.load_rows_from_files(
            abs_files,
            text_by_line=bool(spec.get("text_split_by_line", False)),
            ocr_enabled=bool(spec.get("ocr_enabled", True)),
            ocr_lang=str(spec.get("ocr_lang") or "").strip() or None,
            ocr_config=str(spec.get("ocr_config") or "").strip() or None,
            ocr_preprocess=str(spec.get("ocr_preprocess") or "").strip() or None,
            xlsx_all_sheets=bool(spec.get("xlsx_all_sheets", True)),
            max_retries=int(spec.get("max_retries", 0)),
            on_file_error=str(spec.get("on_file_error", "skip")).strip().lower(),
            extra_options=spec,
        )
        return rows, meta

    fmt = _detect_input_format(path, spec)
    if fmt == "csv":
        rows, delimiter = _read_csv(path, delimiter=spec.get("delimiter"))
        enriched = _attach_source_lineage(rows, path=path, input_format="csv")
        return enriched, {"input_format": "csv", "delimiter": delimiter}
    if fmt == "json":
        rows = _read_json(path)
        enriched = _attach_source_lineage(rows, path=path, input_format="json")
        return enriched, {"input_format": "json"}
    rows = _read_jsonl(path)
    enriched = _attach_source_lineage(rows, path=path, input_format="jsonl")
    return enriched, {"input_format": "jsonl"}


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    _ensure_parent_dir(path)
    if rows:
        fields = list(rows[0].keys())
        seen = set(fields)
        for r in rows[1:]:
            for k in r.keys():
                if k not in seen:
                    fields.append(k)
                    seen.add(k)
    else:
        fields = []

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _write_json(path: str, rows: List[Dict[str, Any]]) -> None:
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def _write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _write_rows(path: str, rows: List[Dict[str, Any]], spec: Dict[str, Any]) -> str:
    fmt = _detect_output_format(path, spec)
    if fmt == "json":
        _write_json(path, rows)
        return "json"
    if fmt == "jsonl":
        _write_jsonl(path, rows)
        return "jsonl"
    _write_csv(path, rows)
    return "csv"

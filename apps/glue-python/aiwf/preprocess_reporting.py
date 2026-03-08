from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from aiwf.preprocess_io import _detect_output_format


def _safe_filename(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or "").strip())
    return s.strip("._") or "artifact"


def _pick_markdown_text(row: Dict[str, Any]) -> str:
    for key in ("claim_text", "text", "content", "body", "paragraph"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    parts: List[str] = []
    for k, v in row.items():
        if v is None:
            continue
        sv = str(v).strip()
        if not sv:
            continue
        parts.append(f"{k}: {sv}")
    return " | ".join(parts)


def export_canonical_bundle(
    *,
    rows: List[Dict[str, Any]],
    summary: Dict[str, Any],
    meta: Dict[str, Any],
    output_path: str,
    spec: Dict[str, Any],
) -> Dict[str, Any]:
    bundle_dir = str(spec.get("canonical_bundle_dir") or f"{output_path}.bundle")
    os.makedirs(bundle_dir, exist_ok=True)
    title = str(spec.get("canonical_title") or "AIWF Canonical Corpus").strip()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    md_lines: List[str] = []
    md_lines.append(f"# {title}")
    md_lines.append("")
    md_lines.append(f"- generated_at: {now}")
    md_lines.append(f"- output_rows: {int(summary.get('output_rows', len(rows)))}")
    md_lines.append(f"- input_format: {meta.get('input_format')}")
    md_lines.append("")
    md_lines.append("## Content")
    md_lines.append("")
    for i, row in enumerate(rows):
        text = _pick_markdown_text(row)
        if not text:
            continue
        md_lines.append(f"### Item {i + 1}")
        md_lines.append("")
        md_lines.append(text)
        md_lines.append("")
    md_path = os.path.join(bundle_dir, f"{_safe_filename(title)}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines).rstrip() + "\n")

    source_counts: Dict[str, int] = {}
    for row in rows:
        src = str(row.get("source_file") or row.get("source_path") or "unknown").strip()
        source_counts[src] = source_counts.get(src, 0) + 1

    metadata = {
        "title": title,
        "generated_at": now,
        "input_format": meta.get("input_format"),
        "file_count": meta.get("file_count"),
        "summary": summary,
        "row_count": len(rows),
        "source_counts": source_counts,
    }
    metadata_path = os.path.join(bundle_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    lineage = {
        "generated_at": now,
        "output_path": output_path,
        "output_format": _detect_output_format(output_path, spec),
        "input_files": spec.get("input_files") if isinstance(spec.get("input_files"), list) else [],
        "skipped_files": meta.get("skipped_files") or [],
        "failed_files": meta.get("failed_files") or [],
        "steps": [
            "ingest",
            "normalize",
            "filter",
            "deduplicate",
            "export_markdown_bundle",
        ],
    }
    lineage_path = os.path.join(bundle_dir, "lineage.json")
    with open(lineage_path, "w", encoding="utf-8") as f:
        json.dump(lineage, f, ensure_ascii=False, indent=2)

    return {
        "bundle_dir": bundle_dir,
        "markdown_path": md_path,
        "metadata_path": metadata_path,
        "lineage_path": lineage_path,
    }


def _build_quality_report(rows: List[Dict[str, Any]], summary: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    row_count = len(rows)
    all_fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                all_fields.append(key)

    non_null_counts: Dict[str, int] = {}
    for field in all_fields:
        count = 0
        for row in rows:
            value = row.get(field)
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            count += 1
        non_null_counts[field] = count

    coverage = {
        field: {
            "non_null": non_null_counts[field],
            "ratio": (float(non_null_counts[field]) / float(row_count)) if row_count > 0 else 0.0,
        }
        for field in all_fields
    }

    source_type_counts: Dict[str, int] = {}
    for row in rows:
        source_type = str(row.get("source_type") or "unknown")
        source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1

    claim_lengths: List[int] = []
    for row in rows:
        value = row.get("claim_text")
        if value is None:
            continue
        s = str(value).strip()
        if s:
            claim_lengths.append(len(s))
    claim_stats = {
        "count": len(claim_lengths),
        "min": min(claim_lengths) if claim_lengths else 0,
        "max": max(claim_lengths) if claim_lengths else 0,
        "avg": (sum(claim_lengths) / len(claim_lengths)) if claim_lengths else 0.0,
    }

    required = [str(x) for x in (spec.get("quality_required_fields") or [])]
    if not required and bool(spec.get("standardize_evidence", False)):
        required = ["claim_text", "source_path"]
    required_missing: Dict[str, int] = {}
    for field in required:
        missing = 0
        for row in rows:
            value = row.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ""):
                missing += 1
        required_missing[field] = missing

    return {
        "rows": row_count,
        "fields": len(all_fields),
        "summary": summary,
        "source_types": source_type_counts,
        "field_coverage": coverage,
        "required_field_missing": required_missing,
        "claim_length": claim_stats,
    }

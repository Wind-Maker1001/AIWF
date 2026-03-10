from __future__ import annotations

import hashlib
from typing import Any, Dict, List


def _first_non_empty(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def _to_canonical_evidence_row(row: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    def _aliases(name: str, defaults: List[str]) -> List[str]:
        value = schema.get(name)
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(item) for item in value]
        return defaults

    claim = _first_non_empty(row, _aliases("claim_text", ["claim_text", "text", "content"]))
    speaker = _first_non_empty(row, _aliases("speaker", ["speaker", "author", "name"]))
    source_url = _first_non_empty(row, _aliases("source_url", ["source_url", "url", "link"]))
    source_title = _first_non_empty(row, _aliases("source_title", ["source_title", "title", "source_name"]))
    published_at = _first_non_empty(row, _aliases("published_at", ["published_at", "publish_date", "date"]))
    stance = _first_non_empty(row, _aliases("stance", ["stance", "position"]))
    confidence = _first_non_empty(row, _aliases("confidence", ["confidence", "score"]))

    source_path = row.get("source_path")
    source_file = row.get("source_file")
    source_type = row.get("source_type")
    chunk_index = row.get("chunk_index")
    page = row.get("page")
    sheet_name = row.get("sheet_name")
    row_index = row.get("row_index")

    key_text = "|".join(
        [
            str(source_path or ""),
            str(page or ""),
            str(sheet_name or ""),
            str(row_index or chunk_index or ""),
            str(claim or ""),
        ]
    )
    evidence_id = hashlib.sha1(key_text.encode("utf-8")).hexdigest()[:16]

    return {
        "evidence_id": evidence_id,
        "claim_text": claim,
        "speaker": speaker,
        "source_title": source_title,
        "source_url": source_url,
        "published_at": published_at,
        "stance": stance,
        "confidence": confidence,
        "source_file": source_file,
        "source_path": source_path,
        "source_type": source_type,
        "page": page,
        "sheet_name": sheet_name,
        "row_index": row_index,
        "chunk_index": chunk_index,
    }

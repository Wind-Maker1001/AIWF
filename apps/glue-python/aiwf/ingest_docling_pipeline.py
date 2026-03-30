from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class ExtractionResult:
    rows: List[Dict[str, Any]] = field(default_factory=list)
    image_blocks: List[Dict[str, Any]] = field(default_factory=list)
    table_cells: List[Dict[str, Any]] = field(default_factory=list)
    sheet_frames: List[Dict[str, Any]] = field(default_factory=list)
    engine_trace: List[Dict[str, Any]] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rows": self.rows,
            "image_blocks": self.image_blocks,
            "table_cells": self.table_cells,
            "sheet_frames": self.sheet_frames,
            "engine_trace": self.engine_trace,
            "payload": self.payload,
        }


def coerce_extraction_result(value: Any) -> Optional[ExtractionResult]:
    if value is None:
        return None
    if isinstance(value, ExtractionResult):
        return value
    if isinstance(value, dict):
        return ExtractionResult(
            rows=list(value.get("rows") or []),
            image_blocks=list(value.get("image_blocks") or []),
            table_cells=list(value.get("table_cells") or []),
            sheet_frames=list(value.get("sheet_frames") or []),
            engine_trace=list(value.get("engine_trace") or []),
            payload=dict(value.get("payload") or {}),
        )
    return None


def _docling_runtime_enabled() -> bool:
    return not bool(os.environ.get("PYTEST_CURRENT_TEST"))


def docling_available() -> bool:
    if not _docling_runtime_enabled():
        return False
    try:
        from docling.document_converter import DocumentConverter  # type: ignore

        return DocumentConverter is not None
    except Exception:
        return False


def _to_plain_object(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _to_plain_object(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain_object(item) for item in value]
    for attr in ("model_dump", "to_dict", "dict"):
        fn = getattr(value, attr, None)
        if callable(fn):
            try:
                result = fn()
                return _to_plain_object(result)
            except Exception:
                continue
    try:
        return _to_plain_object(vars(value))
    except Exception:
        return str(value)


def _iter_nested_nodes(value: Any) -> Iterable[Tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from _iter_nested_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield "", item
            yield from _iter_nested_nodes(item)


def _bbox_from_mapping(item: Dict[str, Any]) -> list[int]:
    for key in ("bbox", "box", "bounds"):
        value = item.get(key)
        if isinstance(value, list) and len(value) == 4:
            try:
                return [int(float(v)) for v in value]
            except Exception:
                continue
        if isinstance(value, dict):
            left = value.get("l") if "l" in value else value.get("left")
            top = value.get("t") if "t" in value else value.get("top")
            right = value.get("r") if "r" in value else value.get("right")
            bottom = value.get("b") if "b" in value else value.get("bottom")
            try:
                return [int(float(left)), int(float(top)), int(float(right)), int(float(bottom))]
            except Exception:
                continue
    return [0, 0, 0, 0]


def _extract_text_like_nodes(payload: Dict[str, Any], path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocks: list[dict[str, Any]] = []
    table_cells: list[dict[str, Any]] = []
    line_no = 0
    for key, node in _iter_nested_nodes(payload):
        if not isinstance(node, dict):
            continue
        label = str(node.get("label") or node.get("type") or key or "").lower()
        text = str(node.get("text") or node.get("content") or node.get("raw_text") or "").strip()
        if any(token in label for token in ("table_cell", "cell")):
            table_cells.append(
                {
                    "cell_id": str(node.get("id") or f"doc_cell_{len(table_cells) + 1:04d}"),
                    "row": int(node.get("row") or node.get("row_index") or len(table_cells)),
                    "col": int(node.get("col") or node.get("col_index") or 0),
                    "text": text,
                    "bbox": _bbox_from_mapping(node),
                    "source_path": path,
                }
            )
            continue
        if not text:
            continue
        if not any(token in label for token in ("text", "paragraph", "title", "heading", "line", "table", "caption")):
            continue
        line_no += 1
        blocks.append(
            {
                "block_id": str(node.get("id") or f"doc_blk_{line_no:04d}"),
                "block_type": "table" if "table" in label else "text",
                "bbox": _bbox_from_mapping(node),
                "text": text,
                "confidence": float(node.get("confidence") or node.get("score") or 1.0),
                "line_no": line_no,
                "page_no": int(node.get("page_no") or node.get("page") or 1),
                "source_path": path,
            }
        )
    return blocks, table_cells


def _extract_sheet_like_rows(payload: Dict[str, Any], path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    frames: list[dict[str, Any]] = []
    table_cells: list[dict[str, Any]] = []
    workbook_name = os.path.basename(path)
    for key, node in _iter_nested_nodes(payload):
        if not isinstance(node, dict):
            continue
        label = str(node.get("label") or node.get("type") or key or "").lower()
        if not any(token in label for token in ("sheet", "table", "worksheet")):
            continue
        columns = node.get("columns")
        data_rows = node.get("rows")
        if not isinstance(columns, list) or not isinstance(data_rows, list):
            continue
        sheet_name = str(node.get("sheet_name") or node.get("name") or f"sheet_{len(frames)+1}")
        canonical_columns = [str(item) for item in columns]
        frame_rows: list[dict[str, Any]] = []
        for row_index, raw_row in enumerate(data_rows, start=1):
            if isinstance(raw_row, dict):
                item = {
                    "source_file": workbook_name,
                    "source_path": path,
                    "source_type": "xlsx",
                    "workbook_name": workbook_name,
                    "sheet_name": sheet_name,
                    "sheet_index": len(frames),
                    "row_index": row_index,
                }
                for column in canonical_columns:
                    item[column] = raw_row.get(column)
                rows.append(item)
                frame_rows.append(item)
            elif isinstance(raw_row, list):
                item = {
                    "source_file": workbook_name,
                    "source_path": path,
                    "source_type": "xlsx",
                    "workbook_name": workbook_name,
                    "sheet_name": sheet_name,
                    "sheet_index": len(frames),
                    "row_index": row_index,
                }
                for column_index, column in enumerate(canonical_columns):
                    value = raw_row[column_index] if column_index < len(raw_row) else None
                    item[column] = value
                    table_cells.append(
                        {
                            "cell_id": f"{sheet_name}_{row_index}_{column_index + 1}",
                            "row": row_index,
                            "col": column_index + 1,
                            "text": str(value or ""),
                            "bbox": [0, 0, 0, 0],
                            "sheet_name": sheet_name,
                            "source_path": path,
                        }
                    )
                rows.append(item)
                frame_rows.append(item)
        frames.append(
            {
                "workbook_name": workbook_name,
                "sheet_name": sheet_name,
                "sheet_index": len(frames),
                "header_row_span": [1, 1],
                "header_confidence": 1.0,
                "table_name": str(node.get("table_name") or ""),
                "columns": canonical_columns,
                "row_count": len(frame_rows),
                "blank_rows": 0,
                "numeric_cells_total": 0,
                "numeric_cells_parsed": 0,
                "date_cells_total": 0,
                "date_cells_parsed": 0,
                "formula_cells": 0,
                "formula_mismatches": 0,
                "hidden": False,
                "source_path": path,
            }
        )
    return rows, frames, table_cells


def extract_with_docling(path: str) -> Optional[ExtractionResult]:
    if not _docling_runtime_enabled():
        return None
    if not docling_available():
        return None
    try:
        from docling.document_converter import DocumentConverter  # type: ignore
    except Exception:
        return None

    try:
        converter = DocumentConverter()
        result = converter.convert(path)
    except Exception:
        return None

    document = getattr(result, "document", result)
    payload = _to_plain_object(document)
    if not isinstance(payload, dict):
        return None
    blocks, table_cells = _extract_text_like_nodes(payload, path)
    rows, frames, sheet_table_cells = _extract_sheet_like_rows(payload, path)
    engine_trace = [
        {
            "engine": "docling",
            "ok": True,
            "document_keys": sorted(payload.keys()),
        }
    ]
    return ExtractionResult(
        payload=payload,
        image_blocks=blocks,
        table_cells=table_cells + sheet_table_cells,
        rows=rows,
        sheet_frames=frames,
        engine_trace=engine_trace,
    )


def dump_docling_payload(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)

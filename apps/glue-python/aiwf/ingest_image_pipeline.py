from __future__ import annotations

import functools
import os
from typing import Any, Dict, List, Optional, Tuple

from aiwf.ingest_ocr import (
    ocr_try_modes,
    preprocess_image_for_ocr,
    resolve_tesseract_cmd,
)
from aiwf.ingest_docling_pipeline import coerce_extraction_result, extract_with_docling
from aiwf.quality_contract import build_image_quality_report


def _bbox_from_quad(points: Any) -> list[int]:
    coords = list(points or [])
    xs: list[float] = []
    ys: list[float] = []
    for point in coords:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            try:
                xs.append(float(point[0]))
                ys.append(float(point[1]))
            except Exception:
                continue
    if not xs or not ys:
        return [0, 0, 0, 0]
    return [int(min(xs)), int(min(ys)), int(max(xs)), int(max(ys))]


def _block_type_from_text(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return "unknown"
    if (
        normalized.count("|") >= 2
        or normalized.count("\t") >= 2
        or normalized.count("丨") >= 2
        or normalized.count("｜") >= 2
        or normalized.count("¦") >= 2
    ):
        return "table"
    return "text"


@functools.lru_cache(maxsize=4)
def _get_paddle_ocr(lang: str):
    from paddleocr import PaddleOCR  # type: ignore

    paddle_lang = "ch" if "chi" in lang or "zh" in lang or "ch" in lang else "en"
    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
    try:
        return PaddleOCR(use_angle_cls=True, lang=paddle_lang, show_log=False)
    except TypeError:
        return PaddleOCR(use_angle_cls=True, lang=paddle_lang)


def _extract_with_paddleocr(path: str, *, ocr_lang: Optional[str]) -> tuple[list[dict[str, Any]], str]:
    lang = str(ocr_lang or "chi_sim+eng").strip().lower()
    ocr = _get_paddle_ocr(lang)
    result = ocr.ocr(path, cls=True)
    blocks: list[dict[str, Any]] = []
    lines = result[0] if isinstance(result, list) and result else []
    for index, item in enumerate(lines):
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        quad = item[0]
        payload = item[1]
        text = ""
        confidence = 0.0
        if isinstance(payload, (list, tuple)) and payload:
            text = str(payload[0] or "")
            try:
                confidence = float(payload[1] or 0.0)
            except Exception:
                confidence = 0.0
        block = {
            "block_id": f"img_blk_{index + 1:04d}",
            "block_type": _block_type_from_text(text),
            "bbox": _bbox_from_quad(quad),
            "text": text.strip(),
            "confidence": confidence,
            "line_no": index + 1,
            "page_no": 1,
            "source_path": path,
        }
        blocks.append(block)
    return blocks, "paddleocr"


def _extract_with_tesseract(
    path: str,
    *,
    ocr_lang: Optional[str],
    ocr_config: Optional[str],
    ocr_preprocess: Optional[str],
) -> tuple[list[dict[str, Any]], str]:
    from PIL import Image  # type: ignore
    import pytesseract  # type: ignore

    try:
        from pytesseract import Output  # type: ignore
    except Exception:
        Output = None

    cmd = resolve_tesseract_cmd()
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd
    lang = str(ocr_lang or os.environ.get("AIWF_OCR_LANG") or "eng+chi_sim").strip()
    config = str(ocr_config or os.environ.get("AIWF_OCR_CONFIG") or "--oem 1 --psm 6").strip()

    best_blocks: list[dict[str, Any]] = []
    best_score = -1.0
    with Image.open(path) as image:
        for mode in ocr_try_modes(ocr_preprocess):
            processed = preprocess_image_for_ocr(image, mode)
            if Output is None:
                text = pytesseract.image_to_string(processed, lang=lang, config=config)
                candidate = [
                    {
                        "block_id": "img_blk_0001",
                        "block_type": _block_type_from_text(text),
                        "bbox": [0, 0, int(image.width), int(image.height)],
                        "text": str(text or "").strip(),
                        "confidence": 0.0,
                        "line_no": 1,
                        "page_no": 1,
                        "source_path": path,
                    }
                ]
            else:
                data = pytesseract.image_to_data(processed, lang=lang, config=config, output_type=Output.DICT)
                grouped: dict[tuple[int, int, int], dict[str, Any]] = {}
                for idx in range(len(data.get("text", []))):
                    text = str((data.get("text") or [""])[idx] or "").strip()
                    if not text:
                        continue
                    block_num = int((data.get("block_num") or [0])[idx] or 0)
                    par_num = int((data.get("par_num") or [0])[idx] or 0)
                    line_num = int((data.get("line_num") or [0])[idx] or 0)
                    key = (block_num, par_num, line_num)
                    left = int((data.get("left") or [0])[idx] or 0)
                    top = int((data.get("top") or [0])[idx] or 0)
                    width = int((data.get("width") or [0])[idx] or 0)
                    height = int((data.get("height") or [0])[idx] or 0)
                    conf = float((data.get("conf") or [0])[idx] or 0.0)
                    current = grouped.setdefault(
                        key,
                        {
                            "texts": [],
                            "bbox": [left, top, left + width, top + height],
                            "confidences": [],
                        },
                    )
                    current["texts"].append(text)
                    current["confidences"].append(max(0.0, conf / 100.0))
                    current["bbox"][0] = min(current["bbox"][0], left)
                    current["bbox"][1] = min(current["bbox"][1], top)
                    current["bbox"][2] = max(current["bbox"][2], left + width)
                    current["bbox"][3] = max(current["bbox"][3], top + height)
                candidate = []
                for line_index, (_key, payload) in enumerate(sorted(grouped.items(), key=lambda item: item[0])):
                    joined = " ".join(payload["texts"]).strip()
                    confidences = list(payload["confidences"])
                    candidate.append(
                        {
                            "block_id": f"img_blk_{line_index + 1:04d}",
                            "block_type": _block_type_from_text(joined),
                            "bbox": payload["bbox"],
                            "text": joined,
                            "confidence": (sum(confidences) / len(confidences)) if confidences else 0.0,
                            "line_no": line_index + 1,
                            "page_no": 1,
                            "source_path": path,
                        }
                    )
            score = sum(float(item.get("confidence") or 0.0) for item in candidate) + len(candidate) * 0.01
            if score > best_score:
                best_score = score
                best_blocks = candidate
    return best_blocks, "tesseract"


def _recover_table_cells_from_layout(blocks: List[Dict[str, Any]], path: str) -> list[dict[str, Any]]:
    text_blocks = [
        block for block in blocks
        if str(block.get("text") or "").strip() and str(block.get("block_type") or "text") != "figure"
    ]
    if len(text_blocks) < 4:
        return []
    sorted_blocks = sorted(
        text_blocks,
        key=lambda block: (
            int((block.get("bbox") or [0, 0, 0, 0])[1]),
            int((block.get("bbox") or [0, 0, 0, 0])[0]),
        ),
    )
    row_clusters: list[list[Dict[str, Any]]] = []
    for block in sorted_blocks:
        bbox = list(block.get("bbox") or [0, 0, 0, 0])
        top = int(bbox[1]) if len(bbox) >= 2 else 0
        bottom = int(bbox[3]) if len(bbox) >= 4 else top
        height = max(1, bottom - top)
        matched_cluster: Optional[list[Dict[str, Any]]] = None
        for cluster in row_clusters:
            sample_bbox = list(cluster[0].get("bbox") or [0, 0, 0, 0])
            sample_top = int(sample_bbox[1]) if len(sample_bbox) >= 2 else 0
            sample_bottom = int(sample_bbox[3]) if len(sample_bbox) >= 4 else sample_top
            tolerance = max(12, int(max(height, sample_bottom - sample_top) * 0.7))
            if abs(top - sample_top) <= tolerance:
                matched_cluster = cluster
                break
        if matched_cluster is None:
            row_clusters.append([block])
        else:
            matched_cluster.append(block)

    candidate_rows = [sorted(cluster, key=lambda block: int((block.get("bbox") or [0, 0, 0, 0])[0])) for cluster in row_clusters]
    candidate_rows = [cluster for cluster in candidate_rows if len(cluster) >= 2]
    if len(candidate_rows) < 2:
        return []

    max_cols = max(len(cluster) for cluster in candidate_rows)
    if max_cols < 2:
        return []

    cells: list[dict[str, Any]] = []
    for row_index, cluster in enumerate(candidate_rows, start=1):
        if len(cluster) < 2:
            continue
        for col_index, block in enumerate(cluster, start=1):
            bbox = list(block.get("bbox") or [0, 0, 0, 0])
            cells.append(
                {
                    "cell_id": f"{block.get('block_id') or f'img_blk_{row_index}_{col_index}'}_{row_index}_{col_index}",
                    "row": row_index,
                    "col": col_index,
                    "text": str(block.get("text") or "").strip(),
                    "bbox": bbox,
                    "source_path": path,
                }
            )
    return cells if len(cells) >= 4 else []


def extract_image_rows(
    path: str,
    *,
    by_line: bool = False,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    spec: Optional[Dict[str, Any]] = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    del by_line
    spec_obj = dict(spec or {})

    blocks: list[dict[str, Any]] = []
    table_cells: list[dict[str, Any]] = []
    engine = "none"
    paddle_error: Optional[str] = None
    engine_trace: list[dict[str, Any]] = []

    docling = coerce_extraction_result(extract_with_docling(path))
    if docling is not None:
        blocks = list(docling.image_blocks or [])
        table_cells = list(docling.table_cells or [])
        engine = "docling"
        engine_trace.extend(list(docling.engine_trace or []))
        docling_text_blocks = [block for block in blocks if str(block.get("text") or "").strip()]
        if not table_cells and len(docling_text_blocks) < 3:
            blocks = []
            engine = "none"

    if not blocks:
        try:
            blocks, engine = _extract_with_paddleocr(path, ocr_lang=ocr_lang)
            engine_trace.append({"engine": "paddleocr", "ok": True, "block_count": len(blocks)})
        except Exception as exc:
            paddle_error = str(exc)
            engine_trace.append({"engine": "paddleocr", "ok": False, "error": paddle_error})

    if not blocks:
        try:
            blocks, engine = _extract_with_tesseract(
                path,
                ocr_lang=ocr_lang,
                ocr_config=ocr_config,
                ocr_preprocess=ocr_preprocess,
            )
            engine_trace.append({"engine": "tesseract", "ok": True, "block_count": len(blocks)})
        except Exception as exc:
            paddle_error = paddle_error or str(exc)
            blocks = []
            engine = "none"
            engine_trace.append({"engine": "tesseract", "ok": False, "error": paddle_error})

    if not table_cells:
        row_offset = 0
        for block in blocks:
            text = str(block.get("text") or "")
            if str(block.get("block_type") or "") != "table":
                continue
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            for row_index, line in enumerate(lines, start=1):
                effective_row = row_offset + row_index
                parts = [
                    part.strip()
                    for part in line.replace("\t", "|").replace("丨", "|").replace("｜", "|").replace("¦", "|").split("|")
                    if part.strip()
                ]
                for col_index, cell_text in enumerate(parts, start=1):
                    table_cells.append(
                        {
                            "cell_id": f"{block.get('block_id')}_{effective_row}_{col_index}",
                            "row": effective_row,
                            "col": col_index,
                            "text": cell_text,
                            "bbox": list(block.get("bbox") or [0, 0, 0, 0]),
                            "source_path": path,
                        }
                    )
            row_offset += len(lines)
    if not table_cells:
        table_cells = _recover_table_cells_from_layout(blocks, path)

    rows = [
        {
            "source_file": os.path.basename(path),
            "source_path": path,
            "source_type": "image",
            "page": 1,
            "row_index": index + 1,
            "line_no": block.get("line_no", index + 1),
            "text": str(block.get("text") or ""),
            "ocr_confidence": float(block.get("confidence") or 0.0),
            "image_block_id": block.get("block_id"),
            "image_block_type": block.get("block_type"),
            "bbox": list(block.get("bbox") or []),
        }
        for index, block in enumerate(blocks)
        if str(block.get("text") or "").strip()
    ]
    quality_report = build_image_quality_report(rows, blocks, spec_obj)
    meta = {
        "input_format": "image",
        "ocr_engine": engine,
        "image_blocks": blocks,
        "table_cells": table_cells,
        "quality_report": quality_report,
        "quality_metrics": quality_report.get("metrics") if isinstance(quality_report.get("metrics"), dict) else {},
        "engine_trace": engine_trace,
        "quality_blocked": bool(quality_report.get("blocked")),
        "quality_error": "; ".join(quality_report.get("errors") or []),
    }
    if paddle_error:
        meta["ocr_warning"] = paddle_error
    return rows, meta

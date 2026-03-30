from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from aiwf.ingest_image_pipeline import extract_image_rows
from aiwf.ingest_xlsx_pipeline import extract_xlsx_rows


def split_text_to_rows(text: str, path: str, source_type: str, by_line: bool = False) -> List[Dict[str, Any]]:
    if by_line:
        chunks = [line.strip() for line in text.splitlines() if line.strip()]
    else:
        chunks = [part.strip() for part in text.split("\n\n") if part.strip()]
    rows: List[Dict[str, Any]] = []
    for index, chunk in enumerate(chunks):
        rows.append(
            {
                "text": chunk,
                "source_file": os.path.basename(path),
                "source_path": path,
                "source_type": source_type,
                "chunk_index": index,
            }
        )
    return rows


def read_text_with_fallback(path: str) -> str:
    encodings = ["utf-8-sig", "utf-8", "gb18030", "gbk"]
    last_err: Optional[Exception] = None
    for encoding in encodings:
        try:
            with open(path, "r", encoding=encoding, errors="strict") as handle:
                return handle.read()
        except Exception as exc:
            last_err = exc
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        txt = handle.read()
    if txt:
        return txt
    raise RuntimeError(f"cannot decode text file: {path}, last_err={last_err}")


def read_txt(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    text = read_text_with_fallback(path)
    return split_text_to_rows(text, path, "txt", by_line=by_line)


def read_docx(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    try:
        from docx import Document  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"docx support requires python-docx: {exc}")
    doc = Document(path)
    text = "\n".join(paragraph.text for paragraph in doc.paragraphs if paragraph.text and paragraph.text.strip())
    return split_text_to_rows(text, path, "docx", by_line=by_line)


def read_pdf(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"pdf support requires pypdf: {exc}")
    reader = PdfReader(path)
    rows: List[Dict[str, Any]] = []
    for page_idx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        chunks = split_text_to_rows(text, path, "pdf", by_line=by_line)
        for chunk in chunks:
            chunk["page"] = page_idx + 1
        rows.extend(chunks)
    return rows


def read_image(
    path: str,
    *,
    by_line: bool = False,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    resolve_tesseract_cmd: Callable[[], Optional[str]],
    ocr_try_modes: Callable[[Optional[str]], List[str]],
    ocr_extract_text: Callable[[Any, Any, str, str, List[str]], str],
) -> List[Dict[str, Any]]:
    del resolve_tesseract_cmd
    del ocr_try_modes
    del ocr_extract_text
    rows, _meta = extract_image_rows(
        path,
        by_line=by_line,
        ocr_lang=ocr_lang,
        ocr_config=ocr_config,
        ocr_preprocess=ocr_preprocess,
    )
    return rows


def read_xlsx(path: str, *, include_all_sheets: bool = True, spec: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    rows, _meta = extract_xlsx_rows(path, include_all_sheets=include_all_sheets, spec=spec)
    return rows


def load_txt_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return read_txt(path, by_line=bool(options.get("text_by_line", False))), {"input_format": "txt"}


def load_docx_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return read_docx(path, by_line=bool(options.get("text_by_line", False))), {"input_format": "docx"}


def load_pdf_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return read_pdf(path, by_line=bool(options.get("text_by_line", False))), {"input_format": "pdf"}


def load_image_input(
    path: str,
    options: Dict[str, Any],
    *,
    read_image_fn: Callable[..., List[Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not bool(options.get("ocr_enabled", True)):
        return [], {"input_format": "image", "skipped": True, "reason": "ocr disabled"}
    rows, meta = extract_image_rows(
        path,
        by_line=bool(options.get("text_by_line", False)),
        ocr_lang=options.get("ocr_lang"),
        ocr_config=options.get("ocr_config"),
        ocr_preprocess=options.get("ocr_preprocess"),
        spec=options,
    )
    return rows, meta


def load_xlsx_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, meta = extract_xlsx_rows(
        path,
        include_all_sheets=bool(options.get("xlsx_all_sheets", True)),
        spec=options,
    )
    return rows, meta

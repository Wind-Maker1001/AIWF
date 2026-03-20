from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional, Tuple


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
    try:
        from PIL import Image  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"image support requires Pillow: {exc}")
    try:
        import pytesseract  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"image OCR support requires pytesseract: {exc}")
    cmd = resolve_tesseract_cmd()
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd
    lang = str(ocr_lang or os.environ.get("AIWF_OCR_LANG") or "eng+chi_sim").strip()
    config = str(ocr_config or os.environ.get("AIWF_OCR_CONFIG") or "--oem 1 --psm 6").strip()
    modes = ocr_try_modes(ocr_preprocess)
    with Image.open(path) as image:
        text = ocr_extract_text(pytesseract, image, lang, config, modes)
    return split_text_to_rows(text, path, "image", by_line=by_line)


def read_xlsx(path: str, *, include_all_sheets: bool = False) -> List[Dict[str, Any]]:
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"xlsx support requires openpyxl: {exc}")
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        out: List[Dict[str, Any]] = []
        sheet_names = workbook.sheetnames if include_all_sheets else [workbook.sheetnames[0]]
        for sheet_name in sheet_names:
            ws = workbook[sheet_name]
            rows_iter = ws.iter_rows(values_only=True)
            try:
                header_raw = next(rows_iter)
            except StopIteration:
                continue
            headers = [
                str(header).strip() if header is not None and str(header).strip() else f"col_{index + 1}"
                for index, header in enumerate(header_raw)
            ]
            for row_idx, row in enumerate(rows_iter, start=2):
                obj: Dict[str, Any] = {}
                non_empty = False
                for index, value in enumerate(row):
                    obj[headers[index]] = value
                    if value is not None and str(value).strip() != "":
                        non_empty = True
                if non_empty:
                    obj["source_file"] = os.path.basename(path)
                    obj["source_path"] = path
                    obj["source_type"] = "xlsx"
                    obj["sheet_name"] = sheet_name
                    obj["row_index"] = row_idx
                    out.append(obj)
        return out
    finally:
        workbook.close()


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
    return (
        read_image_fn(
            path,
            by_line=bool(options.get("text_by_line", False)),
            ocr_lang=options.get("ocr_lang"),
            ocr_config=options.get("ocr_config"),
            ocr_preprocess=options.get("ocr_preprocess"),
        ),
        {"input_format": "image"},
    )


def load_xlsx_input(path: str, options: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return (
        read_xlsx(path, include_all_sheets=bool(options.get("xlsx_all_sheets", False))),
        {"input_format": "xlsx"},
    )

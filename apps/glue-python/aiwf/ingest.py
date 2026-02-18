from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _ext(path: str) -> str:
    return Path(path).suffix.lower()


def _resolve_tesseract_cmd() -> Optional[str]:
    env_cmd = os.environ.get("TESSERACT_CMD")
    if env_cmd and os.path.exists(env_cmd):
        return env_cmd
    candidates = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        "/usr/bin/tesseract",
        "/usr/local/bin/tesseract",
        "/opt/homebrew/bin/tesseract",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def _ocr_preprocess_mode(value: Optional[str]) -> str:
    mode = str(value or os.environ.get("AIWF_OCR_PREPROCESS") or "adaptive").strip().lower()
    if mode not in {"none", "off", "gray", "adaptive"}:
        return "adaptive"
    return mode


def _ocr_try_modes(value: Optional[str]) -> List[str]:
    raw = str(value or os.environ.get("AIWF_OCR_TRY_MODES") or "").strip()
    if raw:
        out: List[str] = []
        for token in raw.split(","):
            m = _ocr_preprocess_mode(token)
            if m not in out:
                out.append(m)
        if out:
            return out
    mode = _ocr_preprocess_mode(value)
    if mode in {"none", "off"}:
        return ["none"]
    if mode == "gray":
        return ["gray", "none"]
    return ["adaptive", "gray", "none"]


def _preprocess_image_for_ocr(image: Any, mode: str) -> Any:
    if mode in {"none", "off"}:
        return image
    try:
        from PIL import ImageFilter, ImageOps  # type: ignore
    except Exception:
        return image
    gray = ImageOps.grayscale(image)
    if mode == "gray":
        return gray
    enhanced = ImageOps.autocontrast(gray)
    filtered = enhanced.filter(ImageFilter.MedianFilter(size=3))
    return filtered.point(lambda x: 255 if x > 160 else 0, mode="1")


def _ocr_text_score(text: str) -> int:
    if not text:
        return 0
    s = str(text).strip()
    if not s:
        return 0
    return len(re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", s))


def _ocr_extract_text(pytesseract: Any, image: Any, lang: str, config: str, modes: List[str]) -> str:
    best = ""
    best_score = -1
    for mode in modes:
        try:
            processed = _preprocess_image_for_ocr(image, mode)
            text = pytesseract.image_to_string(processed, lang=lang, config=config)
        except Exception:
            continue
        score = _ocr_text_score(text)
        if score > best_score:
            best = text
            best_score = score
    return best


def _split_text_to_rows(text: str, path: str, source_type: str, by_line: bool = False) -> List[Dict[str, Any]]:
    if by_line:
        chunks = [ln.strip() for ln in text.splitlines() if ln.strip()]
    else:
        chunks = [p.strip() for p in text.split("\n\n") if p.strip()]
    rows: List[Dict[str, Any]] = []
    for i, c in enumerate(chunks):
        rows.append(
            {
                "text": c,
                "source_file": os.path.basename(path),
                "source_path": path,
                "source_type": source_type,
                "chunk_index": i,
            }
        )
    return rows


def _read_text_with_fallback(path: str) -> str:
    encodings = ["utf-8-sig", "utf-8", "gb18030", "gbk"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read()
        except Exception as e:
            last_err = e
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        txt = f.read()
    if txt:
        return txt
    raise RuntimeError(f"cannot decode text file: {path}, last_err={last_err}")


def read_txt(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    text = _read_text_with_fallback(path)
    return _split_text_to_rows(text, path, "txt", by_line=by_line)


def read_docx(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    try:
        from docx import Document  # type: ignore
    except Exception as e:
        raise RuntimeError(f"docx support requires python-docx: {e}")
    doc = Document(path)
    text = "\n".join(p.text for p in doc.paragraphs if p.text and p.text.strip())
    return _split_text_to_rows(text, path, "docx", by_line=by_line)


def read_pdf(path: str, *, by_line: bool = False) -> List[Dict[str, Any]]:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as e:
        raise RuntimeError(f"pdf support requires pypdf: {e}")
    reader = PdfReader(path)
    rows: List[Dict[str, Any]] = []
    for page_idx, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        chunks = _split_text_to_rows(text, path, "pdf", by_line=by_line)
        for c in chunks:
            c["page"] = page_idx + 1
        rows.extend(chunks)
    return rows


def read_image(
    path: str,
    *,
    by_line: bool = False,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
) -> List[Dict[str, Any]]:
    try:
        from PIL import Image  # type: ignore
    except Exception as e:
        raise RuntimeError(f"image support requires Pillow: {e}")
    try:
        import pytesseract  # type: ignore
    except Exception as e:
        raise RuntimeError(f"image OCR support requires pytesseract: {e}")
    cmd = _resolve_tesseract_cmd()
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd
    lang = str(ocr_lang or os.environ.get("AIWF_OCR_LANG") or "eng+chi_sim").strip()
    config = str(ocr_config or os.environ.get("AIWF_OCR_CONFIG") or "--oem 1 --psm 6").strip()
    modes = _ocr_try_modes(ocr_preprocess)
    with Image.open(path) as image:
        text = _ocr_extract_text(pytesseract, image, lang, config, modes)
    return _split_text_to_rows(text, path, "image", by_line=by_line)


def read_xlsx(path: str, *, include_all_sheets: bool = False) -> List[Dict[str, Any]]:
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception as e:
        raise RuntimeError(f"xlsx support requires openpyxl: {e}")
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        out: List[Dict[str, Any]] = []
        sheet_names = wb.sheetnames if include_all_sheets else [wb.sheetnames[0]]
        for sheet_name in sheet_names:
            ws = wb[sheet_name]
            rows_iter = ws.iter_rows(values_only=True)
            try:
                header_raw = next(rows_iter)
            except StopIteration:
                continue
            headers = [str(h).strip() if h is not None and str(h).strip() else f"col_{i+1}" for i, h in enumerate(header_raw)]
            for row_idx, row in enumerate(rows_iter, start=2):
                obj: Dict[str, Any] = {}
                non_empty = False
                for i, v in enumerate(row):
                    obj[headers[i]] = v
                    if v is not None and str(v).strip() != "":
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
        wb.close()


def load_rows_from_file(
    path: str,
    *,
    text_by_line: bool = False,
    ocr_enabled: bool = True,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    xlsx_all_sheets: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    ext = _ext(path)
    if ext in {".txt"}:
        return read_txt(path, by_line=text_by_line), {"input_format": "txt"}
    if ext in {".docx"}:
        return read_docx(path, by_line=text_by_line), {"input_format": "docx"}
    if ext in {".pdf"}:
        return read_pdf(path, by_line=text_by_line), {"input_format": "pdf"}
    if ext in {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}:
        if not ocr_enabled:
            return [], {"input_format": "image", "skipped": True, "reason": "ocr disabled"}
        return (
            read_image(
                path,
                by_line=text_by_line,
                ocr_lang=ocr_lang,
                ocr_config=ocr_config,
                ocr_preprocess=ocr_preprocess,
            ),
            {"input_format": "image"},
        )
    if ext in {".xlsx", ".xlsm"}:
        return read_xlsx(path, include_all_sheets=xlsx_all_sheets), {"input_format": "xlsx"}
    raise RuntimeError(f"unsupported input file type: {path}")


def load_rows_from_files(
    paths: List[str],
    *,
    text_by_line: bool = False,
    ocr_enabled: bool = True,
    ocr_lang: Optional[str] = None,
    ocr_config: Optional[str] = None,
    ocr_preprocess: Optional[str] = None,
    xlsx_all_sheets: bool = False,
    max_retries: int = 0,
    on_file_error: str = "skip",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    formats: List[str] = []
    skipped_files: List[Dict[str, Any]] = []
    failed_files: List[Dict[str, Any]] = []
    for p in paths:
        last_err: Optional[str] = None
        loaded = False
        for _ in range(max_retries + 1):
            try:
                rows, meta = load_rows_from_file(
                    p,
                    text_by_line=text_by_line,
                    ocr_enabled=ocr_enabled,
                    ocr_lang=ocr_lang,
                    ocr_config=ocr_config,
                    ocr_preprocess=ocr_preprocess,
                    xlsx_all_sheets=xlsx_all_sheets,
                )
                fmt = str(meta.get("input_format"))
                formats.append(fmt)
                if meta.get("skipped"):
                    skipped_files.append({"path": p, "reason": str(meta.get("reason") or "skipped")})
                else:
                    all_rows.extend(rows)
                loaded = True
                break
            except Exception as e:
                last_err = str(e)
        if not loaded:
            failed_files.append({"path": p, "error": last_err or "unknown error"})
            if on_file_error == "raise":
                raise RuntimeError(f"failed to load file {p}: {last_err}")
    return all_rows, {
        "input_format": ",".join(formats),
        "file_count": len(paths),
        "skipped_files": skipped_files,
        "failed_files": failed_files,
    }

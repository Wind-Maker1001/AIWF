from __future__ import annotations

import os
import re
from typing import Any, List, Optional


def resolve_tesseract_cmd() -> Optional[str]:
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
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return None


def ocr_preprocess_mode(value: Optional[str]) -> str:
    mode = str(value or os.environ.get("AIWF_OCR_PREPROCESS") or "adaptive").strip().lower()
    if mode not in {"none", "off", "gray", "adaptive"}:
        return "adaptive"
    return mode


def ocr_try_modes(value: Optional[str]) -> List[str]:
    raw = str(value or os.environ.get("AIWF_OCR_TRY_MODES") or "").strip()
    if raw:
        out: List[str] = []
        for token in raw.split(","):
            mode = ocr_preprocess_mode(token)
            if mode not in out:
                out.append(mode)
        if out:
            return out
    mode = ocr_preprocess_mode(value)
    if mode in {"none", "off"}:
        return ["none"]
    if mode == "gray":
        return ["gray", "none"]
    return ["adaptive", "gray", "none"]


def preprocess_image_for_ocr(image: Any, mode: str) -> Any:
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
    return filtered.point(lambda value: 255 if value > 160 else 0, mode="1")


def ocr_text_score(text: str) -> int:
    if not text:
        return 0
    normalized = str(text).strip()
    if not normalized:
        return 0
    return len(re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", normalized))


def ocr_extract_text(pytesseract: Any, image: Any, lang: str, config: str, modes: List[str]) -> str:
    best = ""
    best_score = -1
    successful_modes = 0
    last_err: Optional[Exception] = None
    for mode in modes:
        try:
            processed = preprocess_image_for_ocr(image, mode)
            text = pytesseract.image_to_string(processed, lang=lang, config=config)
        except Exception as exc:
            last_err = exc
            continue
        successful_modes += 1
        score = ocr_text_score(text)
        if score > best_score:
            best = text
            best_score = score
    if successful_modes == 0 and last_err is not None:
        raise RuntimeError(f"OCR failed for all preprocess modes: {last_err}") from last_err
    return best

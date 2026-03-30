from __future__ import annotations

import importlib.util
import importlib
from typing import Any, Dict

from aiwf.ingest_ocr import resolve_tesseract_cmd


def _module_status(name: str) -> dict[str, Any]:
    spec = importlib.util.find_spec(name)
    status = {"installed": spec is not None, "usable": False}
    if spec is None:
        status["error"] = "module not installed"
        return status
    try:
        module = importlib.import_module(name)
        status["usable"] = module is not None
        status["version"] = str(getattr(module, "__version__", "") or "")
    except Exception as exc:
        status["error"] = str(exc)
    return status


def _docling_status() -> dict[str, Any]:
    status = _module_status("docling")
    return status


def _ocr_status() -> dict[str, Any]:
    tesseract_cmd = resolve_tesseract_cmd()
    return {
        "tesseract": {"installed": bool(tesseract_cmd), "command": tesseract_cmd or ""},
        "paddleocr": _module_status("paddleocr"),
    }


def dependency_status() -> Dict[str, Any]:
    return {
        "pandera": {**_module_status("pandera"), "required_for_quality_contract": True},
        "rapidfuzz": _module_status("rapidfuzz"),
        "dateparser": _module_status("dateparser"),
        "phonenumbers": _module_status("phonenumbers"),
        "python_calamine": _module_status("python_calamine"),
        "openpyxl": _module_status("openpyxl"),
        "docling": _docling_status(),
        "ocr": _ocr_status(),
    }

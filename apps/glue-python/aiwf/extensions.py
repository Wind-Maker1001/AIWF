from __future__ import annotations

import importlib
import os
import sys
from typing import Any, Dict, List


_LOADED_MODULES: List[str] = []
_FAILED_MODULES: Dict[str, str] = {}
_LOAD_ATTEMPTED = False
_LOADING = False


def configured_extension_modules() -> List[str]:
    raw = str(
        os.getenv("AIWF_EXT_MODULES")
        or os.getenv("AIWF_EXTENSION_MODULES")
        or ""
    ).strip()
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def load_extension_modules(*, force: bool = False) -> Dict[str, Any]:
    global _LOAD_ATTEMPTED, _LOADING

    if _LOADING:
        return extension_status()
    if _LOAD_ATTEMPTED and not force:
        return extension_status()

    _LOAD_ATTEMPTED = True
    _LOADING = True
    if force:
        _LOADED_MODULES.clear()
        _FAILED_MODULES.clear()
        importlib.invalidate_caches()

    try:
        for module_name in configured_extension_modules():
            if module_name in _LOADED_MODULES:
                continue
            try:
                existing_module = sys.modules.get(module_name)
                if force and existing_module is not None:
                    importlib.reload(existing_module)
                else:
                    importlib.import_module(module_name)
                _LOADED_MODULES.append(module_name)
                _FAILED_MODULES.pop(module_name, None)
            except Exception as exc:
                _FAILED_MODULES[module_name] = str(exc)
    finally:
        _LOADING = False

    return extension_status()


def extension_status() -> Dict[str, Any]:
    return {
        "configured": configured_extension_modules(),
        "loaded": list(_LOADED_MODULES),
        "failed": dict(_FAILED_MODULES),
        "load_attempted": _LOAD_ATTEMPTED,
    }


def reset_extension_state_for_tests() -> None:
    global _LOAD_ATTEMPTED, _LOADING
    _LOADED_MODULES.clear()
    _FAILED_MODULES.clear()
    _LOAD_ATTEMPTED = False
    _LOADING = False

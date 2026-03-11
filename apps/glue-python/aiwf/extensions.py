from __future__ import annotations

import importlib
import os
import sys
from typing import Any, Dict, List

from aiwf.runtime_state import get_runtime_state


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
    state = get_runtime_state()

    if state.loading:
        return extension_status()
    if state.load_attempted and not force:
        return extension_status()

    state.load_attempted = True
    state.loading = True
    if force:
        state.loaded_modules.clear()
        state.failed_modules.clear()
        importlib.invalidate_caches()

    try:
        for module_name in configured_extension_modules():
            if module_name in state.loaded_modules and not force:
                continue
            try:
                existing_module = sys.modules.get(module_name)
                if existing_module is not None:
                    importlib.reload(existing_module)
                else:
                    importlib.import_module(module_name)
                if module_name not in state.loaded_modules:
                    state.loaded_modules.append(module_name)
                state.failed_modules.pop(module_name, None)
            except Exception as exc:
                state.failed_modules[module_name] = str(exc)
    finally:
        state.loading = False

    return extension_status()


def extension_status() -> Dict[str, Any]:
    state = get_runtime_state()
    return {
        "configured": configured_extension_modules(),
        "loaded": list(state.loaded_modules),
        "failed": dict(state.failed_modules),
        "load_attempted": state.load_attempted,
    }


def reset_extension_state_for_tests() -> None:
    state = get_runtime_state()
    state.loaded_modules.clear()
    state.failed_modules.clear()
    state.load_attempted = False
    state.loading = False

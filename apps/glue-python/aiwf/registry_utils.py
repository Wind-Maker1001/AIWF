from __future__ import annotations

import inspect


def infer_caller_module() -> str:
    for frame_info in inspect.stack()[2:]:
        module_name = str(frame_info.frame.f_globals.get("__name__") or "").strip()
        if not module_name:
            continue
        if module_name in {"builtins", "__main__"}:
            continue
        return module_name
    return "__main__"

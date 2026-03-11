from __future__ import annotations

from typing import Any, Dict

from aiwf.runtime_catalog import get_runtime_catalog


def collect_capabilities() -> Dict[str, Any]:
    return get_runtime_catalog().capabilities()

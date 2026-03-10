from __future__ import annotations

import os
from typing import Any, Dict, Optional

from aiwf.paths import resolve_job_root, resolve_path_within_root

LEGACY_FLOW_PATH_PARAM_KEYS = frozenset({
    "job_root",
    "stage_dir",
    "artifacts_dir",
    "evidence_dir",
})

RESERVED_ATTACHED_PARAM_KEYS = frozenset({
    "job_context",
    "trace_id",
    "job_root",
    "stage_dir",
    "artifacts_dir",
    "evidence_dir",
})


class LegacyFlowPathParamsError(ValueError):
    pass


def normalize_job_context(
    job_id: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    job_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    params_obj = params if isinstance(params, dict) else {}
    context_obj = job_context if isinstance(job_context, dict) else {}

    legacy_param_keys = [key for key in LEGACY_FLOW_PATH_PARAM_KEYS if params_obj.get(key) is not None]

    if legacy_param_keys:
        raise LegacyFlowPathParamsError(
            "legacy flow path params are no longer supported; provide top-level job_context instead of "
            + ",".join(f"params.{key}" for key in legacy_param_keys)
        )

    job_root_override = context_obj.get("job_root")
    job_root = resolve_job_root(job_id, override=str(job_root_override) if job_root_override else None)

    def _child_dir(key: str, default_leaf: str) -> str:
        raw = context_obj.get(key)
        if raw:
            return resolve_path_within_root(job_root, str(raw))
        return os.path.join(job_root, default_leaf)

    normalized = {
        "job_root": job_root,
        "stage_dir": _child_dir("stage_dir", "stage"),
        "artifacts_dir": _child_dir("artifacts_dir", "artifacts"),
        "evidence_dir": _child_dir("evidence_dir", "evidence"),
    }
    return normalized


def attach_job_context(
    params: Optional[Dict[str, Any]],
    *,
    job_context: Dict[str, str],
    trace_id: Optional[str] = None,
) -> Dict[str, Any]:
    out = {
        key: value
        for key, value in dict(params or {}).items()
        if key not in RESERVED_ATTACHED_PARAM_KEYS
    }
    out["job_context"] = dict(job_context)
    if trace_id:
        out["trace_id"] = trace_id
    return out

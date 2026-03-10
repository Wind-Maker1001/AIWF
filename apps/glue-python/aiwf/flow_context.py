from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from aiwf.paths import resolve_job_root, resolve_path_within_root

log = logging.getLogger("glue.flow_context")

RESERVED_ATTACHED_PARAM_KEYS = frozenset({
    "job_context",
    "trace_id",
    "stage_dir",
    "artifacts_dir",
    "evidence_dir",
})


def normalize_job_context(
    job_id: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    job_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    params_obj = params if isinstance(params, dict) else {}
    context_obj = job_context if isinstance(job_context, dict) else {}

    legacy_sources: list[str] = []

    job_root_override = context_obj.get("job_root") or params_obj.get("job_root")
    if context_obj.get("job_root") is None and params_obj.get("job_root") is not None:
        legacy_sources.append("params.job_root")
    job_root = resolve_job_root(job_id, override=str(job_root_override) if job_root_override else None)

    def _child_dir(key: str, default_leaf: str) -> str:
        raw = context_obj.get(key)
        if raw is None:
            raw = params_obj.get(key)
            if raw is not None:
                legacy_sources.append(f"params.{key}")
        if raw:
            return resolve_path_within_root(job_root, str(raw))
        return os.path.join(job_root, default_leaf)

    normalized = {
        "job_root": job_root,
        "stage_dir": _child_dir("stage_dir", "stage"),
        "artifacts_dir": _child_dir("artifacts_dir", "artifacts"),
        "evidence_dir": _child_dir("evidence_dir", "evidence"),
    }
    if legacy_sources:
        log.warning(
            "legacy flow path fallback used job_id=%s sources=%s; prefer job_context.* over params.*",
            job_id,
            ",".join(legacy_sources),
        )
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
    out["job_root"] = job_context["job_root"]
    if trace_id:
        out["trace_id"] = trace_id
    return out

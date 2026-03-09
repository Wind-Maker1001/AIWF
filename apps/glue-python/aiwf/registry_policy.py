from __future__ import annotations

import os


def normalize_conflict_policy(value: str | None, default: str = "replace") -> str:
    allowed = {"replace", "warn", "keep", "error"}
    raw = str(value or "").strip().lower()
    if not raw:
        raw = default
    if raw not in allowed:
        raise ValueError(f"conflict policy must be one of {sorted(allowed)}")
    return raw


def default_conflict_policy() -> str:
    return normalize_conflict_policy(os.getenv("AIWF_REGISTRY_CONFLICT_POLICY"), default="replace")

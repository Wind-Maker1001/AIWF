from __future__ import annotations

import os


def package_root() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))


def repo_root() -> str:
    return os.path.normpath(os.path.join(package_root(), "..", ".."))


def resolve_aiwf_root() -> str:
    root = str(os.getenv("AIWF_ROOT") or "").strip()
    if root:
        return os.path.normpath(root)
    return repo_root()


def resolve_bus_root() -> str:
    bus_root = str(os.getenv("AIWF_BUS") or "").strip()
    if bus_root:
        return os.path.normpath(bus_root)

    jobs_root = str(os.getenv("AIWF_JOBS_ROOT") or "").strip()
    if jobs_root:
        return os.path.normpath(os.path.dirname(jobs_root))

    return os.path.join(resolve_aiwf_root(), "bus")


def resolve_jobs_root() -> str:
    jobs_root = str(os.getenv("AIWF_JOBS_ROOT") or "").strip()
    if jobs_root:
        return os.path.normpath(jobs_root)
    return os.path.join(resolve_bus_root(), "jobs")


def _normalized_abs(path: str) -> str:
    return os.path.normcase(os.path.normpath(os.path.abspath(path)))


def is_within_root(path: str, root: str) -> bool:
    candidate = _normalized_abs(path)
    base_root = _normalized_abs(root)
    try:
        return os.path.commonpath([candidate, base_root]) == base_root
    except ValueError:
        return False


def _strip_file_uri(path: str) -> str:
    raw = str(path or "").strip()
    if raw.lower().startswith("file://"):
        return raw[7:]
    return raw


def resolve_path_within_root(root: str, path: str | None = None) -> str:
    base_root = os.path.normpath(os.path.abspath(root))
    raw = _strip_file_uri(path or "")
    if not raw:
        return base_root
    if os.path.isabs(raw):
        candidate = os.path.normpath(os.path.abspath(raw))
    else:
        candidate = os.path.normpath(os.path.abspath(os.path.join(base_root, raw)))
    if not is_within_root(candidate, base_root):
        raise ValueError(f"path escapes root {base_root}: {raw}")
    return candidate


def resolve_path(root: str, path: str | None = None, *, allow_absolute: bool = True) -> str:
    raw = _strip_file_uri(path or "")
    if not raw:
        return os.path.normpath(os.path.abspath(root))
    if os.path.isabs(raw):
        if not allow_absolute:
            raise ValueError(f"absolute path is not allowed: {raw}")
        return os.path.normpath(os.path.abspath(raw))
    return resolve_path_within_root(root, raw)


def _allow_external_job_root_override() -> bool:
    value = str(os.getenv("AIWF_ALLOW_EXTERNAL_JOB_ROOT") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def resolve_job_root(job_id: str, override: str | None = None) -> str:
    jobs_root = resolve_jobs_root()
    if override:
        raw = str(override)
        if _allow_external_job_root_override() and os.path.isabs(raw):
            return os.path.normpath(os.path.abspath(raw))
        return resolve_path_within_root(jobs_root, raw)
    return resolve_path_within_root(jobs_root, job_id)

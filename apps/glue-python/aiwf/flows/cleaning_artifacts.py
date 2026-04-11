from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional

from aiwf.flows.artifact_selection import normalize_artifact_selection
from aiwf.registry_domains import normalize_registry_domain, summarize_registry_domains
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.runtime_state import get_runtime_state
from aiwf.registry_utils import infer_caller_module


LocalArtifactWriter = Callable[["CleaningArtifactContext", str], None]
LocalArtifactPathResolver = Callable[["CleaningArtifactContext"], str]


@dataclass(frozen=True)
class CleaningArtifactContext:
    stage_dir: str
    evidence_dir: str
    rows: List[Dict[str, Any]]
    profile: Dict[str, Any]
    quality_summary: Dict[str, Any]
    rejections: List[Dict[str, Any]]
    params_effective: Dict[str, Any]
    write_cleaned_csv: Callable[..., Any]
    write_cleaned_parquet: Callable[..., Any]
    write_profile_json: Callable[..., Any]
    sha256_file: Callable[..., str]


@dataclass(frozen=True)
class CleaningArtifactRegistration:
    name: str
    artifact_id: str
    kind: str
    path_key: str
    sha_key: str
    accel_output_key: Optional[str]
    local_path_resolver: Optional[LocalArtifactPathResolver]
    local_writer: Optional[LocalArtifactWriter]
    required: bool
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


def _normalize_name(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("cleaning artifact name must be non-empty")
    return normalized


def _ensure_builtin_cleaning_artifacts() -> None:
    state = get_runtime_state()
    if state.builtins_cleaning_artifacts_registered or state.cleaning_artifact_bootstrap_in_progress:
        return
    state.cleaning_artifact_bootstrap_in_progress = True
    try:
        from aiwf.flows.domains.cleaning_core_artifacts import register_builtin_cleaning_artifacts

        register_builtin_cleaning_artifacts(_register_cleaning_artifact)
        state.builtins_cleaning_artifacts_registered = True
    finally:
        state.cleaning_artifact_bootstrap_in_progress = False


def _register_cleaning_artifact(
    name: str,
    *,
    artifact_id: str,
    kind: str,
    path_key: str,
    sha_key: str,
    accel_output_key: Optional[str] = None,
    local_path_resolver: Optional[LocalArtifactPathResolver] = None,
    local_writer: Optional[LocalArtifactWriter] = None,
    required: bool = False,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> CleaningArtifactRegistration:
    state = get_runtime_state()
    normalized = _normalize_name(name)
    if local_writer is not None and not callable(local_writer):
        raise TypeError("cleaning artifact local_writer must be callable")
    if local_path_resolver is not None and not callable(local_path_resolver):
        raise TypeError("cleaning artifact local_path_resolver must be callable")
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    existing = state.cleaning_artifacts.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="cleaning_artifact",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"cleaning artifact {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="cleaning_artifact",
                name=normalized,
                action="keep",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="kept existing registration",
            )
            return existing
        action = "replace_with_warning" if policy == "warn" else "replace"
        record_registry_event(
            registry="cleaning_artifact",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = CleaningArtifactRegistration(
        name=normalized,
        artifact_id=str(artifact_id),
        kind=str(kind),
        path_key=str(path_key),
        sha_key=str(sha_key),
        accel_output_key=str(accel_output_key) if accel_output_key else None,
        local_path_resolver=local_path_resolver,
        local_writer=local_writer,
        required=bool(required),
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.cleaning_artifacts[normalized] = registration
    return registration


def register_cleaning_artifact(
    name: str,
    *,
    artifact_id: str,
    kind: str,
    path_key: str,
    sha_key: str,
    accel_output_key: Optional[str] = None,
    local_path_resolver: Optional[LocalArtifactPathResolver] = None,
    local_writer: Optional[LocalArtifactWriter] = None,
    required: bool = False,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> CleaningArtifactRegistration:
    _ensure_builtin_cleaning_artifacts()
    effective_source = source_module or infer_caller_module()
    return _register_cleaning_artifact(
        name,
        artifact_id=artifact_id,
        kind=kind,
        path_key=path_key,
        sha_key=sha_key,
        accel_output_key=accel_output_key,
        local_path_resolver=local_path_resolver,
        local_writer=local_writer,
        required=required,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_cleaning_artifact(name: str) -> Optional[CleaningArtifactRegistration]:
    _ensure_builtin_cleaning_artifacts()
    return get_runtime_state().cleaning_artifacts.pop(_normalize_name(name), None)


def list_cleaning_artifacts() -> List[str]:
    _ensure_builtin_cleaning_artifacts()
    return sorted(get_runtime_state().cleaning_artifacts.keys())


def list_cleaning_artifact_details() -> List[Dict[str, Any]]:
    _ensure_builtin_cleaning_artifacts()
    return [
        {
            "name": registration.name,
            "artifact_id": registration.artifact_id,
            "kind": registration.kind,
            "path_key": registration.path_key,
            "sha_key": registration.sha_key,
            "required": registration.required,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in get_cleaning_artifact_registrations()
    ]


def list_cleaning_artifact_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_cleaning_artifacts()
    return summarize_registry_domains(
        sorted(get_cleaning_artifact_registrations(), key=lambda item: item.name),
        item_name_attr="name",
        list_key="artifacts",
    )


def get_cleaning_artifact(name: str) -> CleaningArtifactRegistration:
    _ensure_builtin_cleaning_artifacts()
    normalized = _normalize_name(name)
    registration = get_runtime_state().cleaning_artifacts.get(normalized)
    if registration is None:
        raise KeyError(f"unknown cleaning artifact: {normalized}")
    return registration


def get_cleaning_artifact_registrations() -> List[CleaningArtifactRegistration]:
    _ensure_builtin_cleaning_artifacts()
    return list(get_runtime_state().cleaning_artifacts.values())


def _artifact_tokens(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _registration_tokens(registration: CleaningArtifactRegistration) -> set[str]:
    tokens = {
        registration.name,
        registration.kind.lower(),
        registration.artifact_id.lower(),
        registration.path_key.lower(),
    }
    if registration.accel_output_key:
        tokens.add(registration.accel_output_key.lower())
    return tokens


def list_cleaning_artifact_tokens() -> List[str]:
    _ensure_builtin_cleaning_artifacts()
    tokens = set()
    for registration in get_cleaning_artifact_registrations():
        tokens.update(_registration_tokens(registration))
    return sorted(tokens)


def select_cleaning_artifact_registrations(params_effective: Dict[str, Any]) -> List[CleaningArtifactRegistration]:
    _ensure_builtin_cleaning_artifacts()
    selection = normalize_artifact_selection(params_effective)["core"]
    core_enabled = bool(selection.get("enabled", True))
    enabled_tokens = _artifact_tokens(selection.get("include"))
    disabled_tokens = _artifact_tokens(selection.get("exclude"))

    selected: List[CleaningArtifactRegistration] = []
    for registration in get_cleaning_artifact_registrations():
        tokens = _registration_tokens(registration)
        if registration.required:
            if not tokens.isdisjoint(disabled_tokens):
                raise RuntimeError(f"required cleaning artifact disabled: {registration.name}")
            selected.append(registration)
            continue
        if not core_enabled and not enabled_tokens:
            continue
        if enabled_tokens and tokens.isdisjoint(enabled_tokens):
            continue
        if not tokens.isdisjoint(disabled_tokens):
            continue
        selected.append(registration)
    return selected


def _csv_local_path(context: CleaningArtifactContext) -> str:
    return os.path.join(context.stage_dir, "cleaned.csv")


def _parquet_local_path(context: CleaningArtifactContext) -> str:
    return os.path.join(context.stage_dir, "cleaned.parquet")


def _profile_local_path(context: CleaningArtifactContext) -> str:
    return os.path.join(context.evidence_dir, "profile.json")


def _quality_summary_local_path(context: CleaningArtifactContext) -> str:
    return os.path.join(context.evidence_dir, "quality_summary.json")


def _rejections_local_path(context: CleaningArtifactContext) -> str:
    return os.path.join(context.evidence_dir, "rejections.jsonl")


def _write_csv_artifact(context: CleaningArtifactContext, output_path: str) -> None:
    context.write_cleaned_csv(output_path, context.rows)


def _write_parquet_artifact(context: CleaningArtifactContext, output_path: str) -> None:
    context.write_cleaned_parquet(output_path, context.rows)


def _write_profile_artifact(context: CleaningArtifactContext, output_path: str) -> None:
    context.write_profile_json(output_path, context.profile, context.params_effective)


def _write_quality_summary_artifact(context: CleaningArtifactContext, output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(context.quality_summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def _write_rejections_artifact(context: CleaningArtifactContext, output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8", newline="\n") as handle:
        for item in context.rejections:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def materialize_local_cleaning_artifacts(context: CleaningArtifactContext) -> Dict[str, Any]:
    out: Dict[str, Any] = {"core_artifacts": []}
    for registration in select_cleaning_artifact_registrations(context.params_effective):
        if registration.local_path_resolver is None or registration.local_writer is None:
            continue
        output_path = registration.local_path_resolver(context)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        registration.local_writer(context, output_path)
        sha = context.sha256_file(output_path)
        out[registration.path_key] = output_path
        out[registration.sha_key] = sha
        out["core_artifacts"].append(
            {
                "artifact_id": registration.artifact_id,
                "kind": registration.kind,
                "path": output_path,
                "sha256": sha,
            }
        )
    return out


def materialize_accel_cleaning_artifacts(
    accel_outputs: Dict[str, Any],
    *,
    params_effective: Dict[str, Any],
    sha256_file: Callable[[str], str],
    local_context: Optional[CleaningArtifactContext] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"core_artifacts": []}
    for registration in select_cleaning_artifact_registrations(params_effective):
        if not registration.accel_output_key:
            if local_context is None or registration.local_path_resolver is None or registration.local_writer is None:
                continue
            output_path = registration.local_path_resolver(local_context)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            registration.local_writer(local_context, output_path)
            sha = local_context.sha256_file(output_path)
            out[registration.path_key] = output_path
            out[registration.sha_key] = sha
            out["core_artifacts"].append(
                {
                    "artifact_id": registration.artifact_id,
                    "kind": registration.kind,
                    "path": output_path,
                    "sha256": sha,
                }
            )
            continue
        obj = accel_outputs.get(registration.accel_output_key) or {}
        path = str(obj.get("path", "")) if isinstance(obj, dict) else ""
        if not path:
            if registration.required:
                raise RuntimeError(
                    f"missing accel artifact output for required artifact: {registration.accel_output_key}"
                )
            continue
        sha = ""
        if isinstance(obj, dict):
            sha = str(obj.get("sha256") or (sha256_file(path) if path else ""))
        out[registration.path_key] = path
        out[registration.sha_key] = sha
        out["core_artifacts"].append(
            {
                "artifact_id": registration.artifact_id,
                "kind": registration.kind,
                "path": path,
                "sha256": sha,
            }
        )
    return out

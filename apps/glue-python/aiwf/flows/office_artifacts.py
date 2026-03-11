from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional

from aiwf.flows.artifact_selection import normalize_artifact_selection
from aiwf.registry_domains import normalize_registry_domain, summarize_registry_domains
from aiwf.registry_events import record_registry_event
from aiwf.registry_policy import default_conflict_policy, normalize_conflict_policy
from aiwf.runtime_state import get_runtime_state
from aiwf.registry_utils import infer_caller_module


OfficeArtifactWriter = Callable[["OfficeArtifactContext", str], None]


@dataclass(frozen=True)
class OfficeArtifactContext:
    job_id: str
    artifacts_dir: str
    params_effective: Dict[str, Any]
    rows: List[Dict[str, Any]]
    quality: Dict[str, Any]
    profile_source: str
    office_rows: List[Dict[str, Any]]
    office_profile: Dict[str, Any]
    illustration_path: str
    write_fin_xlsx: Callable[..., Any]
    write_audit_docx: Callable[..., Any]
    write_deck_pptx: Callable[..., Any]
    sha256_file: Callable[..., str]


@dataclass(frozen=True)
class OfficeArtifactRegistration:
    name: str
    artifact_id: str
    kind: str
    filename: str
    path_key: str
    sha_key: str
    writer: OfficeArtifactWriter
    domain: Optional[str]
    domain_metadata: Mapping[str, Any]
    source_module: str


def _normalize_name(name: str) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("office artifact name must be non-empty")
    return normalized


def _ensure_builtin_office_artifacts() -> None:
    state = get_runtime_state()
    if state.builtins_office_artifacts_registered or state.office_artifact_bootstrap_in_progress:
        return
    state.office_artifact_bootstrap_in_progress = True
    try:
        from aiwf.flows.domains.cleaning_office_artifacts import register_builtin_office_artifacts

        register_builtin_office_artifacts(_register_office_artifact)
        state.builtins_office_artifacts_registered = True
    finally:
        state.office_artifact_bootstrap_in_progress = False


def _register_office_artifact(
    name: str,
    *,
    artifact_id: str,
    kind: str,
    filename: str,
    path_key: str,
    sha_key: str,
    writer: OfficeArtifactWriter,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> OfficeArtifactRegistration:
    state = get_runtime_state()
    normalized = _normalize_name(name)
    if not callable(writer):
        raise TypeError("office artifact writer must be callable")
    normalized_domain, normalized_domain_metadata = normalize_registry_domain(
        domain,
        domain_metadata,
    )
    source = str(source_module or infer_caller_module())
    existing = state.office_artifacts.get(normalized)
    if existing is not None:
        policy = normalize_conflict_policy(on_conflict, default_conflict_policy())
        if policy == "error":
            record_registry_event(
                registry="office_artifact",
                name=normalized,
                action="error",
                policy=policy,
                existing_source_module=existing.source_module,
                new_source_module=source,
                detail="registration already exists",
            )
            raise RuntimeError(
                f"office artifact {normalized} already registered by {existing.source_module}"
            )
        if policy == "keep":
            record_registry_event(
                registry="office_artifact",
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
            registry="office_artifact",
            name=normalized,
            action=action,
            policy=policy,
            existing_source_module=existing.source_module,
            new_source_module=source,
            detail="replaced existing registration",
        )
    registration = OfficeArtifactRegistration(
        name=normalized,
        artifact_id=str(artifact_id),
        kind=str(kind),
        filename=str(filename),
        path_key=str(path_key),
        sha_key=str(sha_key),
        writer=writer,
        domain=normalized_domain,
        domain_metadata=normalized_domain_metadata,
        source_module=source,
    )
    state.office_artifacts[normalized] = registration
    return registration


def register_office_artifact(
    name: str,
    *,
    artifact_id: str,
    kind: str,
    filename: str,
    path_key: str,
    sha_key: str,
    writer: OfficeArtifactWriter,
    domain: Optional[str] = None,
    domain_metadata: Optional[Mapping[str, Any]] = None,
    source_module: Optional[str] = None,
    on_conflict: Optional[str] = None,
) -> OfficeArtifactRegistration:
    _ensure_builtin_office_artifacts()
    effective_source = source_module or infer_caller_module()
    return _register_office_artifact(
        name,
        artifact_id=artifact_id,
        kind=kind,
        filename=filename,
        path_key=path_key,
        sha_key=sha_key,
        writer=writer,
        domain=domain,
        domain_metadata=domain_metadata,
        source_module=effective_source,
        on_conflict=on_conflict,
    )


def unregister_office_artifact(name: str) -> Optional[OfficeArtifactRegistration]:
    _ensure_builtin_office_artifacts()
    return get_runtime_state().office_artifacts.pop(_normalize_name(name), None)


def get_office_artifact(name: str) -> OfficeArtifactRegistration:
    _ensure_builtin_office_artifacts()
    normalized = _normalize_name(name)
    registration = get_runtime_state().office_artifacts.get(normalized)
    if registration is None:
        raise KeyError(f"unknown office artifact: {normalized}")
    return registration


def list_office_artifacts() -> List[str]:
    _ensure_builtin_office_artifacts()
    return sorted(get_runtime_state().office_artifacts.keys())


def list_office_artifact_details() -> List[Dict[str, Any]]:
    _ensure_builtin_office_artifacts()
    return [
        {
            "name": registration.name,
            "artifact_id": registration.artifact_id,
            "kind": registration.kind,
            "filename": registration.filename,
            "path_key": registration.path_key,
            "sha_key": registration.sha_key,
            "domain": registration.domain,
            "domain_metadata": dict(registration.domain_metadata),
            "source_module": registration.source_module,
        }
        for registration in get_office_artifact_registrations()
    ]


def list_office_artifact_domains() -> List[Dict[str, Any]]:
    _ensure_builtin_office_artifacts()
    return summarize_registry_domains(
        sorted(get_office_artifact_registrations(), key=lambda item: item.name),
        item_name_attr="name",
        list_key="artifacts",
    )


def get_office_artifact_registrations() -> List[OfficeArtifactRegistration]:
    _ensure_builtin_office_artifacts()
    return list(get_runtime_state().office_artifacts.values())


def _artifact_tokens(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _registration_tokens(registration: OfficeArtifactRegistration) -> set[str]:
    return {
        registration.name,
        registration.kind.lower(),
        registration.artifact_id.lower(),
        registration.filename.lower(),
        registration.path_key.lower(),
    }


def list_office_artifact_tokens() -> List[str]:
    _ensure_builtin_office_artifacts()
    tokens = set()
    for registration in get_office_artifact_registrations():
        tokens.update(_registration_tokens(registration))
    return sorted(tokens)


def select_office_artifact_registrations(params_effective: Dict[str, Any]) -> List[OfficeArtifactRegistration]:
    _ensure_builtin_office_artifacts()
    selection = normalize_artifact_selection(params_effective)["office"]
    if not bool(selection.get("enabled", True)):
        return []

    enabled_tokens = _artifact_tokens(selection.get("include"))
    disabled_tokens = _artifact_tokens(selection.get("exclude"))

    selected: List[OfficeArtifactRegistration] = []
    for registration in get_office_artifact_registrations():
        tokens = _registration_tokens(registration)
        if enabled_tokens and tokens.isdisjoint(enabled_tokens):
            continue
        if not tokens.isdisjoint(disabled_tokens):
            continue
        selected.append(registration)
    return selected


def _write_xlsx_artifact(context: OfficeArtifactContext, output_path: str) -> None:
    context.write_fin_xlsx(
        output_path,
        context.office_rows,
        context.illustration_path,
        context.params_effective,
    )


def _write_docx_artifact(context: OfficeArtifactContext, output_path: str) -> None:
    context.write_audit_docx(
        output_path,
        context.job_id,
        context.office_profile,
        context.illustration_path,
        context.params_effective,
    )


def _write_pptx_artifact(context: OfficeArtifactContext, output_path: str) -> None:
    context.write_deck_pptx(
        output_path,
        context.job_id,
        context.office_profile,
        context.illustration_path,
        context.params_effective,
    )

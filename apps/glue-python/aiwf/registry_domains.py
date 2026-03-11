from __future__ import annotations

from types import MappingProxyType
from typing import Any, Iterable, Mapping


def normalize_registry_domain(
    domain: str | None,
    domain_metadata: Mapping[str, Any] | None,
) -> tuple[str | None, Mapping[str, Any]]:
    metadata = dict(domain_metadata or {})
    normalized_domain = _normalize_domain_name(domain) if domain is not None else None
    metadata_name = metadata.get("name")
    if metadata_name is not None:
        normalized_metadata_name = _normalize_domain_name(metadata_name)
        if normalized_domain is not None and normalized_metadata_name != normalized_domain:
            raise ValueError("registry domain and domain metadata name must match")
        normalized_domain = normalized_metadata_name
    if normalized_domain is None:
        if metadata:
            raise ValueError("registry domain metadata requires a domain name")
        return None, MappingProxyType({})
    metadata["name"] = normalized_domain
    return normalized_domain, MappingProxyType(metadata)


def summarize_registry_domains(
    registrations: Iterable[Any],
    *,
    item_name_attr: str,
    list_key: str,
) -> list[dict[str, Any]]:
    domains: dict[str, dict[str, Any]] = {}
    for registration in registrations:
        domain_name = getattr(registration, "domain", None)
        if domain_name is None:
            continue
        metadata = dict(getattr(registration, "domain_metadata", {}))
        entry = domains.get(domain_name)
        if entry is None:
            entry = metadata
            entry["name"] = domain_name
            entry[list_key] = []
            domains[domain_name] = entry
        else:
            for key, value in metadata.items():
                entry.setdefault(key, value)
        entry[list_key].append(getattr(registration, item_name_attr))
    return [domains[name] for name in sorted(domains.keys())]


def _normalize_domain_name(name: str | None) -> str:
    normalized = str(name or "").strip().lower()
    if not normalized:
        raise ValueError("registry domain name must be non-empty")
    return normalized

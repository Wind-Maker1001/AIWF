from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

from aiwf.governance_manual_reviews import (
    MANUAL_REVIEW_OWNER,
    MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION,
    MANUAL_REVIEW_SCHEMA_VERSION,
)
from aiwf.governance_quality_rule_sets import (
    QUALITY_RULE_SET_OWNER,
    QUALITY_RULE_SET_SCHEMA_VERSION,
    QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
)
from aiwf.governance_run_baselines import (
    RUN_BASELINE_OWNER,
    RUN_BASELINE_SCHEMA_VERSION,
    RUN_BASELINE_STORE_SCHEMA_VERSION,
)
from aiwf.governance_workflow_apps import (
    WORKFLOW_APP_OWNER,
    WORKFLOW_APP_SCHEMA_VERSION,
    WORKFLOW_APP_STORE_SCHEMA_VERSION,
)
from aiwf.governance_workflow_sandbox_autofix import (
    WORKFLOW_SANDBOX_AUTOFIX_OWNER,
    WORKFLOW_SANDBOX_AUTOFIX_SCHEMA_VERSION,
)
from aiwf.governance_workflow_sandbox_rules import (
    WORKFLOW_SANDBOX_RULE_OWNER,
    WORKFLOW_SANDBOX_RULE_SCHEMA_VERSION,
    WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
)
from aiwf.governance_workflow_versions import (
    WORKFLOW_VERSION_OWNER,
    WORKFLOW_VERSION_SCHEMA_VERSION,
    WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
)


GOVERNANCE_SURFACE_SCHEMA_VERSION = "governance_surface.v1"
GOVERNANCE_SURFACE_META_ROUTE = "/governance/meta/control-plane"
GOVERNANCE_STATE_CONTROL_PLANE_OWNER = "glue-python"
JOB_LIFECYCLE_CONTROL_PLANE_OWNER = "base-java"
OPERATOR_SEMANTICS_AUTHORITY_OWNER = "accel-rust"
WORKFLOW_AUTHORING_SURFACE_OWNER = "dify-desktop"
GOVERNANCE_CONTROL_PLANE_ROLE = "governance_state"
GOVERNANCE_CONTROL_PLANE_STATUS = "effective_second_control_plane"

REQUIRED_GOVERNANCE_SURFACE_FIELDS = (
    "capability",
    "route_prefix",
    "owned_route_prefixes",
    "state_owner",
    "schema_version",
    "source_of_truth",
    "control_plane",
    "control_plane_role",
    "control_plane_status",
    "host_runtime",
    "job_lifecycle_control_plane_owner",
    "operator_semantics_authority_owner",
    "workflow_authoring_surface_owner",
    "lifecycle_mutation_allowed",
)


def _base_governance_surface_entries() -> List[Dict[str, Any]]:
    return [
        {
            "capability": "quality_rule_sets",
            "route_prefix": "/governance/quality-rule-sets",
            "owned_route_prefixes": ["/governance/quality-rule-sets"],
            "state_owner": QUALITY_RULE_SET_OWNER,
            "schema_version": QUALITY_RULE_SET_SCHEMA_VERSION,
            "store_schema_version": QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.quality_rule_sets",
        },
        {
            "capability": "workflow_sandbox_rules",
            "route_prefix": "/governance/workflow-sandbox/rules",
            "owned_route_prefixes": [
                "/governance/workflow-sandbox/rules",
                "/governance/workflow-sandbox/rule-versions",
            ],
            "state_owner": WORKFLOW_SANDBOX_RULE_OWNER,
            "schema_version": WORKFLOW_SANDBOX_RULE_SCHEMA_VERSION,
            "store_schema_version": WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.workflow_sandbox_rules",
        },
        {
            "capability": "workflow_sandbox_autofix",
            "route_prefix": "/governance/workflow-sandbox/autofix-state",
            "owned_route_prefixes": [
                "/governance/workflow-sandbox/autofix-state",
                "/governance/workflow-sandbox/autofix-actions",
            ],
            "state_owner": WORKFLOW_SANDBOX_AUTOFIX_OWNER,
            "schema_version": WORKFLOW_SANDBOX_AUTOFIX_SCHEMA_VERSION,
            "store_schema_version": "",
            "source_of_truth": "glue-python.governance.workflow_sandbox_autofix",
        },
        {
            "capability": "workflow_apps",
            "route_prefix": "/governance/workflow-apps",
            "owned_route_prefixes": ["/governance/workflow-apps"],
            "state_owner": WORKFLOW_APP_OWNER,
            "schema_version": WORKFLOW_APP_SCHEMA_VERSION,
            "store_schema_version": WORKFLOW_APP_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.workflow_apps",
        },
        {
            "capability": "workflow_versions",
            "route_prefix": "/governance/workflow-versions",
            "owned_route_prefixes": ["/governance/workflow-versions"],
            "state_owner": WORKFLOW_VERSION_OWNER,
            "schema_version": WORKFLOW_VERSION_SCHEMA_VERSION,
            "store_schema_version": WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.workflow_versions",
        },
        {
            "capability": "manual_reviews",
            "route_prefix": "/governance/manual-reviews",
            "owned_route_prefixes": ["/governance/manual-reviews"],
            "state_owner": MANUAL_REVIEW_OWNER,
            "schema_version": MANUAL_REVIEW_SCHEMA_VERSION,
            "store_schema_version": MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.manual_reviews",
        },
        {
            "capability": "run_baselines",
            "route_prefix": "/governance/run-baselines",
            "owned_route_prefixes": ["/governance/run-baselines"],
            "state_owner": RUN_BASELINE_OWNER,
            "schema_version": RUN_BASELINE_SCHEMA_VERSION,
            "store_schema_version": RUN_BASELINE_STORE_SCHEMA_VERSION,
            "source_of_truth": "glue-python.governance.run_baselines",
        },
    ]


def _normalize_owned_route_prefixes(value: Any) -> List[str]:
    seen = set()
    normalized: List[str] = []
    for item in value if isinstance(value, list) else []:
        prefix = str(item or "").strip()
        if not prefix or prefix in seen:
            continue
        normalized.append(prefix)
        seen.add(prefix)
    return normalized


def _enrich_governance_surface_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    route_prefix = str(entry.get("route_prefix") or "").strip()
    owned_route_prefixes = _normalize_owned_route_prefixes(entry.get("owned_route_prefixes"))
    if route_prefix and route_prefix not in owned_route_prefixes:
        owned_route_prefixes = [route_prefix, *owned_route_prefixes]
    return {
        "schema_version": GOVERNANCE_SURFACE_SCHEMA_VERSION,
        "control_plane": GOVERNANCE_CONTROL_PLANE_ROLE,
        "control_plane_role": GOVERNANCE_CONTROL_PLANE_ROLE,
        "control_plane_status": GOVERNANCE_CONTROL_PLANE_STATUS,
        "host_runtime": GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
        "job_lifecycle_control_plane_owner": JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
        "operator_semantics_authority_owner": OPERATOR_SEMANTICS_AUTHORITY_OWNER,
        "workflow_authoring_surface_owner": WORKFLOW_AUTHORING_SURFACE_OWNER,
        "lifecycle_mutation_allowed": False,
        **entry,
        "route_prefix": route_prefix,
        "owned_route_prefixes": owned_route_prefixes,
    }


def list_governance_surface_entries() -> List[Dict[str, Any]]:
    return [_enrich_governance_surface_entry(entry) for entry in _base_governance_surface_entries()]


def build_governance_capability_map() -> Dict[str, Any]:
    capability_map: Dict[str, Any] = {
        "surface_schema_version": GOVERNANCE_SURFACE_SCHEMA_VERSION,
        "control_plane_status": GOVERNANCE_CONTROL_PLANE_STATUS,
        "control_plane_role": GOVERNANCE_CONTROL_PLANE_ROLE,
        "governance_state_control_plane_owner": GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
        "job_lifecycle_control_plane_owner": JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
        "operator_semantics_authority_owner": OPERATOR_SEMANTICS_AUTHORITY_OWNER,
        "workflow_authoring_surface_owner": WORKFLOW_AUTHORING_SURFACE_OWNER,
    }
    for entry in list_governance_surface_entries():
        capability = str(entry.get("capability") or "").strip()
        if not capability:
            continue
        capability_map[capability] = {
            "owner": str(entry.get("state_owner") or "").strip(),
            "schema_version": str(entry.get("schema_version") or "").strip(),
            "store_schema_version": str(entry.get("store_schema_version") or "").strip(),
            "service": GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
            "route_prefix": str(entry.get("route_prefix") or "").strip(),
            "owned_route_prefixes": deepcopy(entry.get("owned_route_prefixes") or []),
            "source_of_truth": str(entry.get("source_of_truth") or "").strip(),
            "control_plane_role": str(entry.get("control_plane_role") or "").strip(),
            "lifecycle_mutation_allowed": bool(entry.get("lifecycle_mutation_allowed")),
        }
    return capability_map


def list_governance_owned_route_prefixes(entries: Optional[Iterable[Dict[str, Any]]] = None) -> List[str]:
    source = list(entries) if entries is not None else list_governance_surface_entries()
    seen = set()
    prefixes: List[str] = []
    for entry in source:
        for prefix in _normalize_owned_route_prefixes(entry.get("owned_route_prefixes")):
            if prefix in seen:
                continue
            seen.add(prefix)
            prefixes.append(prefix)
    return prefixes


def validate_governance_surface_entries(entries: Optional[Iterable[Dict[str, Any]]] = None) -> List[str]:
    source = list(entries) if entries is not None else list_governance_surface_entries()
    issues: List[str] = []
    owned_route_claims: Dict[str, List[str]] = {}

    for entry in source:
        capability = str(entry.get("capability") or "").strip() or "<missing-capability>"
        for field in REQUIRED_GOVERNANCE_SURFACE_FIELDS:
            value = entry.get(field)
            if field == "owned_route_prefixes":
                if not isinstance(value, list) or len(_normalize_owned_route_prefixes(value)) == 0:
                    issues.append(f"{capability} missing non-empty {field}")
                continue
            if isinstance(value, str):
                if not value.strip():
                    issues.append(f"{capability} missing {field}")
            elif value is None:
                issues.append(f"{capability} missing {field}")

        route_prefix = str(entry.get("route_prefix") or "").strip()
        owned_route_prefixes = _normalize_owned_route_prefixes(entry.get("owned_route_prefixes"))
        if route_prefix and route_prefix not in owned_route_prefixes:
            issues.append(f"{capability} route_prefix missing from owned_route_prefixes")
        for prefix in owned_route_prefixes:
            if not prefix.startswith("/governance/"):
                issues.append(f"{capability} owned route prefix is not governance-scoped: {prefix}")
                continue
            owned_route_claims.setdefault(prefix, []).append(capability)

    duplicate_prefixes = sorted(
        prefix
        for prefix, capabilities in owned_route_claims.items()
        if len(set(capabilities)) > 1
    )
    for prefix in duplicate_prefixes:
        issues.append(f"owned route prefix claimed by multiple capabilities: {prefix}")

    return sorted(set(issues))


def build_governance_control_plane_boundary() -> Dict[str, Any]:
    return {
        "schema_version": GOVERNANCE_SURFACE_SCHEMA_VERSION,
        "status": GOVERNANCE_CONTROL_PLANE_STATUS,
        "control_plane_role": GOVERNANCE_CONTROL_PLANE_ROLE,
        "meta_route": GOVERNANCE_SURFACE_META_ROUTE,
        "governance_state_control_plane_owner": GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
        "job_lifecycle_control_plane_owner": JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
        "operator_semantics_authority_owner": OPERATOR_SEMANTICS_AUTHORITY_OWNER,
        "workflow_authoring_surface_owner": WORKFLOW_AUTHORING_SURFACE_OWNER,
        "notes": [
            "base-java remains the formal owner of job lifecycle and job_context transport",
            "glue-python is the current owner of governance state and governance API surfaces under /governance/*",
            "this makes glue-python the effective second control plane for governance state, not the owner of job lifecycle",
            "all governance surfaces must declare explicit route ownership and must keep lifecycle_mutation_allowed=false",
            "accel-rust remains the authority for operator semantics and operator metadata",
        ],
        "governance_surfaces": deepcopy(list_governance_surface_entries()),
    }


__all__ = [
    "GOVERNANCE_CONTROL_PLANE_ROLE",
    "GOVERNANCE_CONTROL_PLANE_STATUS",
    "GOVERNANCE_STATE_CONTROL_PLANE_OWNER",
    "GOVERNANCE_SURFACE_META_ROUTE",
    "GOVERNANCE_SURFACE_SCHEMA_VERSION",
    "JOB_LIFECYCLE_CONTROL_PLANE_OWNER",
    "OPERATOR_SEMANTICS_AUTHORITY_OWNER",
    "REQUIRED_GOVERNANCE_SURFACE_FIELDS",
    "WORKFLOW_AUTHORING_SURFACE_OWNER",
    "build_governance_capability_map",
    "build_governance_control_plane_boundary",
    "list_governance_owned_route_prefixes",
    "list_governance_surface_entries",
    "validate_governance_surface_entries",
]

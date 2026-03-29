from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from aiwf.accel_transport import DEFAULT_ACCEL_BASE_URL, operator_url


WORKFLOW_GRAPH_ERROR_CODE = "workflow_graph_invalid"
WORKFLOW_VALIDATION_UNAVAILABLE_CODE = "workflow_validation_unavailable"
WORKFLOW_GRAPH_CONTRACT_AUTHORITY = "contracts/workflow/workflow.schema.json"
NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY = "contracts/desktop/node_config_validation_errors.v1.json"


@dataclass
class WorkflowValidationFailure(ValueError):
    message: str
    error_items: List[Dict[str, Any]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    normalized_workflow_definition: Dict[str, Any] = field(default_factory=dict)
    graph_contract: str = WORKFLOW_GRAPH_CONTRACT_AUTHORITY
    error_item_contract: str = NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY

    def __str__(self) -> str:
        return self.message


@dataclass
class WorkflowValidationUnavailable(RuntimeError):
    message: str

    def __str__(self) -> str:
        return self.message


def _validation_error_message(payload: Dict[str, Any]) -> str:
    items = payload.get("error_items") if isinstance(payload.get("error_items"), list) else []
    for item in items:
        if isinstance(item, dict):
            text = str(item.get("message") or "").strip()
            if text:
                return text
    return str(payload.get("error") or "workflow contract invalid").strip() or "workflow contract invalid"


def validate_workflow_definition_authoritatively(
    workflow_definition: Dict[str, Any],
    *,
    accel_url: str = "",
    timeout: float = 10.0,
    allow_version_migration: bool = False,
    require_non_empty_nodes: bool = False,
    validation_scope: str = "governance_write",
) -> Dict[str, Any]:
    import requests

    payload = workflow_definition if isinstance(workflow_definition, dict) else {}
    base_url = str(accel_url or DEFAULT_ACCEL_BASE_URL).rstrip("/")
    url = operator_url(base_url, "/operators/workflow_contract_v1/validate")

    try:
        response = requests.post(
            url,
            json={
                "workflow_definition": payload,
                "allow_version_migration": bool(allow_version_migration),
                "require_non_empty_nodes": bool(require_non_empty_nodes),
                "validation_scope": str(validation_scope or "governance_write"),
            },
            timeout=timeout,
        )
    except Exception as exc:
        raise WorkflowValidationUnavailable(
            f"workflow validation unavailable: {str(exc)}"
        ) from exc

    try:
        body = response.json()
    except Exception as exc:
        raise WorkflowValidationUnavailable(
            f"workflow validation unavailable: invalid JSON response from {url}"
        ) from exc

    if response.status_code >= 400 or body.get("ok") is False:
        raise WorkflowValidationUnavailable(
            str(body.get("error") or f"workflow validation unavailable: http {response.status_code}")
        )

    if body.get("valid") is False or str(body.get("status") or "").strip().lower() == "invalid":
        raise WorkflowValidationFailure(
            _validation_error_message(body),
            error_items=body.get("error_items") if isinstance(body.get("error_items"), list) else [],
            notes=body.get("notes") if isinstance(body.get("notes"), list) else [],
            normalized_workflow_definition=body.get("normalized_workflow_definition")
            if isinstance(body.get("normalized_workflow_definition"), dict)
            else dict(payload),
            graph_contract=str(body.get("graph_contract") or WORKFLOW_GRAPH_CONTRACT_AUTHORITY),
            error_item_contract=str(body.get("error_item_contract") or NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY),
        )

    return body if isinstance(body, dict) else {}

import os
import json
import re
import time
import traceback
import logging
import uuid
import inspect
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from aiwf.runtime_catalog import get_runtime_catalog
from aiwf.flow_context import LegacyFlowPathParamsError, attach_job_context, normalize_job_context
from aiwf.paths import resolve_jobs_root
from aiwf.governance_quality_rule_sets import (
    QUALITY_RULE_SET_OWNER,
    QUALITY_RULE_SET_SCHEMA_VERSION,
    QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
    get_quality_rule_set,
    list_quality_rule_sets,
    remove_quality_rule_set,
    save_quality_rule_set,
)
from aiwf.governance_workflow_sandbox_rules import (
    WORKFLOW_SANDBOX_RULE_OWNER,
    WORKFLOW_SANDBOX_RULE_SCHEMA_VERSION,
    WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
    get_workflow_sandbox_rules,
    list_workflow_sandbox_rule_versions,
    rollback_workflow_sandbox_rule_version,
    set_workflow_sandbox_rules,
)
from aiwf.governance_workflow_sandbox_autofix import (
    WORKFLOW_SANDBOX_AUTOFIX_OWNER,
    WORKFLOW_SANDBOX_AUTOFIX_SCHEMA_VERSION,
    get_workflow_sandbox_autofix_state,
    list_workflow_sandbox_autofix_actions,
    save_workflow_sandbox_autofix_state,
)
from aiwf.governance_workflow_apps import (
    WORKFLOW_APP_OWNER,
    WORKFLOW_APP_SCHEMA_VERSION,
    WORKFLOW_APP_STORE_SCHEMA_VERSION,
    get_workflow_app,
    list_workflow_apps,
    save_workflow_app,
)
from aiwf.governance_workflow_versions import (
    WORKFLOW_VERSION_OWNER,
    WORKFLOW_VERSION_SCHEMA_VERSION,
    WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
    compare_workflow_versions,
    get_workflow_version,
    list_workflow_versions,
    save_workflow_version,
)
from aiwf.governance_manual_reviews import (
    MANUAL_REVIEW_OWNER,
    MANUAL_REVIEW_QUEUE_STORE_SCHEMA_VERSION,
    MANUAL_REVIEW_SCHEMA_VERSION,
    enqueue_manual_reviews,
    filter_manual_review_history,
    list_manual_review_history,
    list_manual_reviews,
    submit_manual_review,
)
from aiwf.governance_run_baselines import (
    RUN_BASELINE_OWNER,
    RUN_BASELINE_SCHEMA_VERSION,
    RUN_BASELINE_STORE_SCHEMA_VERSION,
    get_run_baseline,
    list_run_baselines,
    save_run_baseline,
)
from aiwf.governance_surface import (
    GOVERNANCE_CONTROL_PLANE_ROLE,
    GOVERNANCE_CONTROL_PLANE_STATUS,
    GOVERNANCE_STATE_CONTROL_PLANE_OWNER,
    GOVERNANCE_SURFACE_META_ROUTE,
    GOVERNANCE_SURFACE_SCHEMA_VERSION,
    JOB_LIFECYCLE_CONTROL_PLANE_OWNER,
    OPERATOR_SEMANTICS_AUTHORITY_OWNER,
    WORKFLOW_AUTHORING_SURFACE_OWNER,
    build_governance_capability_map,
    build_governance_control_plane_boundary,
    list_governance_surface_entries,
)
from aiwf.node_config_contract_runtime import (
    NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("glue")


class Settings(BaseModel):
    base_url: str = Field(default_factory=lambda: os.getenv("AIWF_BASE_URL", "http://127.0.0.1:18080"))
    jobs_root: str = Field(default_factory=resolve_jobs_root)
    api_key: Optional[str] = Field(default_factory=lambda: os.getenv("AIWF_API_KEY"))
    timeout_seconds: float = Field(default_factory=lambda: float(os.getenv("AIWF_HTTP_TIMEOUT", "30")))


settings = Settings()
runtime_catalog = get_runtime_catalog()

WORKFLOW_GRAPH_CONTRACT_AUTHORITY = "contracts/workflow/workflow.schema.json"
WORKFLOW_GRAPH_ERROR_CODE = "workflow_graph_invalid"
GOVERNANCE_VALIDATION_ERROR_CODE = "governance_validation_invalid"


def _normalize_workflow_graph_error_messages(message: str) -> list[str]:
    text = str(message or "").strip()
    if not text:
        return []

    node_config_prefixes = (
        "workflow app graph node config invalid: ",
        "workflow version graph node config invalid: ",
    )
    for prefix in node_config_prefixes:
        if text.startswith(prefix):
            tail = text[len(prefix):].strip()
            return [item.strip() for item in tail.split(";") if item.strip()]

    replacements = [
        (re.compile(r"^workflow (?:app|version) graph must be an object$"), "workflow must be an object"),
        (re.compile(r"^workflow (?:app|version) graph requires workflow_id$"), "workflow.workflow_id is required"),
        (re.compile(r"^workflow (?:app|version) graph requires version$"), "workflow.version is required"),
        (re.compile(r"^workflow (?:app|version) graph requires nodes array$"), "workflow.nodes must be an array"),
        (re.compile(r"^workflow (?:app|version) graph requires edges array$"), "workflow.edges must be an array"),
        (re.compile(r"^workflow (?:app|version) graph contains unregistered node types: (.+)$"), lambda m: f"workflow contains unregistered node types: {m.group(1)}"),
        (re.compile(r"^workflow (?:app|version) graph nodes\[(\d+)\] must be an object$"), lambda m: f"workflow.nodes[{m.group(1)}] must be an object"),
        (re.compile(r"^workflow (?:app|version) graph nodes\[(\d+)\] requires id$"), lambda m: f"workflow.nodes[{m.group(1)}].id is required"),
        (re.compile(r"^workflow (?:app|version) graph nodes\[(\d+)\] requires type$"), lambda m: f"workflow.nodes[{m.group(1)}].type is required"),
        (re.compile(r"^workflow (?:app|version) graph edges\[(\d+)\] must be an object$"), lambda m: f"workflow.edges[{m.group(1)}] must be an object"),
        (re.compile(r"^workflow (?:app|version) graph edges\[(\d+)\] requires from$"), lambda m: f"workflow.edges[{m.group(1)}].from is required"),
        (re.compile(r"^workflow (?:app|version) graph edges\[(\d+)\] requires to$"), lambda m: f"workflow.edges[{m.group(1)}].to is required"),
    ]
    for pattern, replacement in replacements:
        match = pattern.match(text)
        if not match:
            continue
        if callable(replacement):
            return [str(replacement(match))]
        return [str(replacement)]

    return [text]


def _normalize_validation_error_item(message: str) -> dict[str, str]:
    text = str(message or "").strip()
    if not text:
        return {"path": "", "code": "validation_error", "message": ""}
    if re.match(r"^workflow contains unregistered node types:", text):
        return {"path": "workflow.nodes", "code": "unknown_node_type", "message": text}
    path = text
    code = "validation_error"
    for pattern in [
        r"^(.*?) keys must not be empty$",
        r"^(.*?) must match .*$",
        r"^(.*?) must be included in .* when both are provided$",
        r"^(.*?) is required when .*$",
        r"^(.*?) requires one of .*$",
        r"^(.*?) must contain at least one node$",
        r"^(.*?) must not be empty$",
        r"^(.*?) must be .*$",
        r"^(.*?) is required$",
    ]:
        match = re.match(pattern, text)
        if match:
            path = str(match.group(1) or "").strip() or text
            break
    if text.endswith(" must be a boolean"): code = "type_boolean"
    elif text.endswith(" must be a string"): code = "type_string"
    elif text.endswith(" must not be empty"): code = "string_empty"
    elif " must be one of: " in text: code = "enum_not_allowed"
    elif text.endswith(" must be an array"): code = "type_array"
    elif text.endswith(" must contain at least one node"): code = "array_min_items"
    elif text.endswith(" must be an object"): code = "type_object"
    elif text.endswith(" keys must not be empty"): code = "empty_key"
    elif text.endswith(" must be JSON-compatible"): code = "json_not_compatible"
    elif text.endswith(" must be an integer"): code = "type_integer"
    elif text.endswith(" must be a number"): code = "type_number"
    elif " must be >= " in text: code = "min_value"
    elif " requires one of " in text: code = "missing_one_of"
    elif " is required when " in text and text.endswith(" is provided"): code = "paired_required"
    elif " is required when " in text: code = "conditional_required"
    elif " must be included in " in text and text.endswith(" when both are provided"): code = "membership_required"
    elif " validator kind unsupported: " in text: code = "unsupported_validator_kind"
    elif text.endswith(" must not be undefined"): code = "undefined_not_allowed"
    elif text.endswith(" is required"): code = "required"
    return {"path": path, "code": code, "message": text}


def _build_validation_error_items(errors: list[str]) -> list[dict[str, str]]:
    return [_normalize_validation_error_item(msg) for msg in errors if str(msg or "").strip()]


def _workflow_graph_error_response(provider: str, scope: str, exc: ValueError) -> JSONResponse:
    normalized_errors = _normalize_workflow_graph_error_messages(str(exc))
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "provider": provider,
            "error": str(exc),
            "error_code": WORKFLOW_GRAPH_ERROR_CODE,
            "error_scope": scope,
            "graph_contract": WORKFLOW_GRAPH_CONTRACT_AUTHORITY,
            "error_item_contract": NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
            "error_items": _build_validation_error_items(normalized_errors),
        },
    )


def _governance_validation_error_response(
    provider: str,
    scope: str,
    exc: ValueError,
    *,
    normalized_errors: Optional[list[str]] = None,
) -> JSONResponse:
    items = normalized_errors if normalized_errors is not None else [str(exc)]
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "provider": provider,
            "error": str(exc),
            "error_code": GOVERNANCE_VALIDATION_ERROR_CODE,
            "error_scope": scope,
            "error_item_contract": NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
            "error_items": _build_validation_error_items(items),
        },
    )


def _debug_errors_enabled() -> bool:
    env_mode = str(os.getenv("AIWF_ENV") or "").strip().lower()
    if env_mode in {"prod", "production"} or str(os.getenv("AIWF_RELEASE") or "").strip() == "1":
        return False
    v = str(os.getenv("AIWF_DEBUG_ERRORS") or os.getenv("AIWF_DEBUG") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


class RunReq(BaseModel):
    actor: str = "glue"
    ruleset_version: str = "v1"
    trace_id: Optional[str] = None
    job_context: Optional[Dict[str, str]] = None
    params: Dict[str, Any] = Field(default_factory=dict)


class QualityRuleSetReq(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    version: str = "v1"
    scope: str = "workflow"
    rules: Dict[str, Any] = Field(default_factory=dict)


class QualityRuleSetUpsertReq(BaseModel):
    set: QualityRuleSetReq


class WorkflowSandboxRuleUpdateReq(BaseModel):
    rules: Dict[str, Any] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)


class WorkflowSandboxAutoFixStateReq(BaseModel):
    violation_events: list[Dict[str, Any]] = Field(default_factory=list)
    forced_isolation_mode: str = ""
    forced_until: str = ""
    last_actions: list[Dict[str, Any]] = Field(default_factory=list)
    green_streak: int = 0


class WorkflowAppReq(BaseModel):
    app_id: Optional[str] = None
    name: Optional[str] = None
    workflow_id: Optional[str] = None
    published_version_id: Optional[str] = None
    graph: Optional[Dict[str, Any]] = None
    params_schema: Dict[str, Any] = Field(default_factory=dict)
    template_policy: Dict[str, Any] = Field(default_factory=dict)


class WorkflowAppUpsertReq(BaseModel):
    app: WorkflowAppReq


class WorkflowVersionReq(BaseModel):
    version_id: Optional[str] = None
    ts: Optional[str] = None
    workflow_id: Optional[str] = None
    workflow_name: Optional[str] = None
    path: Optional[str] = None
    graph: Dict[str, Any] = Field(default_factory=dict)


class WorkflowVersionUpsertReq(BaseModel):
    version: WorkflowVersionReq


class ManualReviewItemReq(BaseModel):
    run_id: str
    review_key: Optional[str] = None
    workflow_id: Optional[str] = None
    node_id: Optional[str] = None
    reviewer: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[str] = None
    status: str = "pending"
    approved: Optional[bool] = None


class ManualReviewEnqueueReq(BaseModel):
    items: list[ManualReviewItemReq] = Field(default_factory=list)


class ManualReviewSubmitReq(BaseModel):
    run_id: str
    review_key: str
    approved: bool
    reviewer: str = "reviewer"
    comment: str = ""


class RunBaselineReq(BaseModel):
    baseline_id: Optional[str] = None
    name: Optional[str] = None
    run_id: str
    workflow_id: Optional[str] = None
    created_at: Optional[str] = None
    notes: str = ""


class RunBaselineEnvelope(BaseModel):
    baseline: RunBaselineReq


def _call_compatible(callable_obj, candidates):
    try:
        callable_signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        callable_signature = None

    if callable_signature is not None:
        for args, kwargs in candidates:
            try:
                callable_signature.bind(*args, **kwargs)
            except TypeError:
                continue
            return callable_obj(*args, **kwargs)

    args, kwargs = candidates[-1]
    return callable_obj(*args, **kwargs)


def make_base_client():
    """
    Build BaseClient with best-effort compatibility for different constructor signatures.
    """
    try:
        from aiwf.base_client import BaseClient  # type: ignore
    except Exception as e:
        log.warning("make_base_client: cannot import/use aiwf.base_client.BaseClient: %s", e)
        return None

    return _call_compatible(
        BaseClient,
        [
            (
                (settings.base_url,),
                {"api_key": settings.api_key, "timeout": settings.timeout_seconds},
            ),
            (
                (settings.base_url, settings.api_key),
                {},
            ),
            (
                (settings.base_url,),
                {},
            ),
        ],
    )


def _run_flow_with_runner(job_id: str, req: RunReq, runner):
    """Compatibility wrapper for flow runners with mixed signatures."""
    base = make_base_client()

    try:
        normalized_context = normalize_job_context(
            job_id,
            params=req.params,
            job_context=req.job_context,
        )
    except ValueError as exc:
        raise LegacyFlowPathParamsError(str(exc)) from exc
    params_obj = attach_job_context(
        req.params,
        job_context=normalized_context,
        trace_id=req.trace_id,
    )
    params_json = json.dumps(params_obj, ensure_ascii=False)
    return _call_compatible(
        runner,
        [
            (
                (),
                {
                    "job_id": job_id,
                    "actor": req.actor,
                    "ruleset_version": req.ruleset_version,
                    "s": settings,
                    "base": base,
                    "params": params_obj,
                },
            ),
            (
                (),
                {
                    "job_id": job_id,
                    "actor": req.actor,
                    "ruleset_version": req.ruleset_version,
                    "s": settings,
                    "base": base,
                    "params_json": params_json,
                },
            ),
            (
                (),
                {
                    "job_id": job_id,
                    "actor": req.actor,
                    "ruleset_version": req.ruleset_version,
                    "s": settings,
                    "base": base,
                },
            ),
            (
                (),
                {
                    "job_id": job_id,
                    "actor": req.actor,
                    "ruleset_version": req.ruleset_version,
                },
            ),
        ],
    )


def run_registered_flow(job_id: str, flow: str, req: RunReq):
    runner = runtime_catalog.get_flow_runner(flow)
    return _run_flow_with_runner(job_id, req, runner)


def run_cleaning_flow(job_id: str, req: RunReq):
    return run_registered_flow(job_id, "cleaning", req)


app = FastAPI(title="AIWF glue-python", version="0.1.0")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/capabilities")
def capabilities():
    caps = runtime_catalog.capabilities()
    caps["governance"] = build_governance_capability_map()
    caps["governance_surface"] = {
        "schema_version": GOVERNANCE_SURFACE_SCHEMA_VERSION,
        "status": GOVERNANCE_CONTROL_PLANE_STATUS,
        "control_plane_role": GOVERNANCE_CONTROL_PLANE_ROLE,
        "items": list_governance_surface_entries(),
    }
    caps["control_plane_boundary"] = build_governance_control_plane_boundary()
    return {"ok": True, "capabilities": caps}


@app.get(GOVERNANCE_SURFACE_META_ROUTE)
def governance_control_plane_meta():
    return {
        "ok": True,
        "boundary": build_governance_control_plane_boundary(),
    }


@app.get("/governance/quality-rule-sets")
def list_governance_quality_rule_sets(limit: int = 500):
    return {
        "ok": True,
        "provider": QUALITY_RULE_SET_OWNER,
        "schema_version": QUALITY_RULE_SET_STORE_SCHEMA_VERSION,
        "sets": list_quality_rule_sets(limit),
    }


@app.get("/governance/quality-rule-sets/{set_id}")
def get_governance_quality_rule_set(set_id: str):
    try:
        item = get_quality_rule_set(set_id)
    except ValueError as exc:
        return _governance_validation_error_response(
            QUALITY_RULE_SET_OWNER,
            "quality_rule_set",
            exc,
            normalized_errors=[f"set.id {str(exc).replace('quality rule set id ', '')}"],
        )
    if item is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"quality rule set not found: {set_id}"},
        )
    return {"ok": True, "provider": QUALITY_RULE_SET_OWNER, "set": item}


@app.put("/governance/quality-rule-sets/{set_id}")
def put_governance_quality_rule_set(set_id: str, req: QualityRuleSetUpsertReq):
    payload = req.set.model_dump()
    payload["id"] = str(set_id or payload.get("id") or "")
    try:
        item = save_quality_rule_set(payload)
    except ValueError as exc:
        return _governance_validation_error_response(
            QUALITY_RULE_SET_OWNER,
            "quality_rule_set",
            exc,
            normalized_errors=[f"set.id {str(exc).replace('quality rule set id ', '')}"],
        )
    return {"ok": True, "provider": QUALITY_RULE_SET_OWNER, "set": item}


@app.delete("/governance/quality-rule-sets/{set_id}")
def delete_governance_quality_rule_set(set_id: str):
    try:
        removed = remove_quality_rule_set(set_id)
    except ValueError as exc:
        return _governance_validation_error_response(
            QUALITY_RULE_SET_OWNER,
            "quality_rule_set",
            exc,
            normalized_errors=[f"set.id {str(exc).replace('quality rule set id ', '')}"],
        )
    if not removed:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"quality rule set not found: {set_id}"},
        )
    return {"ok": True, "provider": QUALITY_RULE_SET_OWNER, "id": str(set_id)}


@app.get("/governance/workflow-sandbox/rules")
def get_governance_workflow_sandbox_rules():
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_RULE_OWNER,
        "schema_version": WORKFLOW_SANDBOX_RULE_STORE_SCHEMA_VERSION,
        "rules": get_workflow_sandbox_rules(),
    }


@app.put("/governance/workflow-sandbox/rules")
def put_governance_workflow_sandbox_rules(req: WorkflowSandboxRuleUpdateReq):
    result = set_workflow_sandbox_rules(req.rules, req.meta)
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_RULE_OWNER,
        "rules": result["rules"],
        "version_id": str(result["version"].get("version_id") or ""),
    }


@app.get("/governance/workflow-sandbox/rule-versions")
def list_governance_workflow_sandbox_rule_versions(limit: int = 200):
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_RULE_OWNER,
        "items": list_workflow_sandbox_rule_versions(limit),
    }


@app.post("/governance/workflow-sandbox/rule-versions/{version_id}/rollback")
def rollback_governance_workflow_sandbox_rule_version(version_id: str):
    result = rollback_workflow_sandbox_rule_version(version_id)
    if result is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"workflow sandbox rule version not found: {version_id}"},
        )
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_RULE_OWNER,
        "rules": result["rules"],
        "version_id": str(result["version"].get("version_id") or ""),
    }


@app.get("/governance/workflow-sandbox/autofix-state")
def get_governance_workflow_sandbox_autofix_state():
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_AUTOFIX_OWNER,
        "state": get_workflow_sandbox_autofix_state(),
    }


@app.put("/governance/workflow-sandbox/autofix-state")
def put_governance_workflow_sandbox_autofix_state(req: WorkflowSandboxAutoFixStateReq):
    state = save_workflow_sandbox_autofix_state(req.model_dump())
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_AUTOFIX_OWNER,
        "state": state,
    }


@app.get("/governance/workflow-sandbox/autofix-actions")
def list_governance_workflow_sandbox_autofix_actions(limit: int = 120):
    state = get_workflow_sandbox_autofix_state()
    return {
        "ok": True,
        "provider": WORKFLOW_SANDBOX_AUTOFIX_OWNER,
        "forced_isolation_mode": str(state.get("forced_isolation_mode") or ""),
        "forced_until": str(state.get("forced_until") or ""),
        "items": list_workflow_sandbox_autofix_actions(limit),
    }


@app.get("/governance/workflow-apps")
def list_governance_workflow_apps(limit: int = 200):
    return {
        "ok": True,
        "provider": WORKFLOW_APP_OWNER,
        "schema_version": WORKFLOW_APP_STORE_SCHEMA_VERSION,
        "items": list_workflow_apps(limit),
    }


@app.get("/governance/workflow-apps/{app_id}")
def get_governance_workflow_app(app_id: str):
    try:
        item = get_workflow_app(app_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    if item is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"workflow app not found: {app_id}"},
        )
    return {"ok": True, "provider": WORKFLOW_APP_OWNER, "item": item}


@app.put("/governance/workflow-apps/{app_id}")
def put_governance_workflow_app(app_id: str, req: WorkflowAppUpsertReq):
    payload = req.app.model_dump()
    payload["app_id"] = str(app_id or payload.get("app_id") or "")
    try:
        item = save_workflow_app(payload)
    except ValueError as exc:
        return _governance_validation_error_response(WORKFLOW_APP_OWNER, "workflow_app", exc)
    return {"ok": True, "provider": WORKFLOW_APP_OWNER, "item": item}


@app.get("/governance/workflow-versions")
def list_governance_workflow_versions(limit: int = 200, workflow_name: str = ""):
    return {
        "ok": True,
        "provider": WORKFLOW_VERSION_OWNER,
        "schema_version": WORKFLOW_VERSION_STORE_SCHEMA_VERSION,
        "items": list_workflow_versions(limit, workflow_name),
    }


@app.get("/governance/workflow-versions/{version_id}")
def get_governance_workflow_version(version_id: str):
    try:
        item = get_workflow_version(version_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    if item is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"version not found: {version_id}"},
        )
    return {"ok": True, "provider": WORKFLOW_VERSION_OWNER, "item": item}


@app.put("/governance/workflow-versions/{version_id}")
def put_governance_workflow_version(version_id: str, req: WorkflowVersionUpsertReq):
    payload = req.version.model_dump()
    payload["version_id"] = str(version_id or payload.get("version_id") or "")
    try:
        item = save_workflow_version(payload)
    except ValueError as exc:
        return _workflow_graph_error_response(WORKFLOW_VERSION_OWNER, "workflow_version", exc)
    return {"ok": True, "provider": WORKFLOW_VERSION_OWNER, "item": item}


@app.post("/governance/workflow-versions/compare")
def post_governance_workflow_version_compare(req: Dict[str, Any]):
    version_a = str((req or {}).get("version_a") or "").strip()
    version_b = str((req or {}).get("version_b") or "").strip()
    try:
        result = compare_workflow_versions(version_a, version_b)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"provider": WORKFLOW_VERSION_OWNER, **result}


@app.get("/governance/manual-reviews")
def list_governance_manual_reviews(limit: int = 200):
    return {
        "ok": True,
        "provider": MANUAL_REVIEW_OWNER,
        "items": list_manual_reviews(limit),
    }


@app.post("/governance/manual-reviews/enqueue")
def post_governance_manual_reviews_enqueue(req: ManualReviewEnqueueReq):
    items = enqueue_manual_reviews([item.model_dump() for item in req.items])
    return {
        "ok": True,
        "provider": MANUAL_REVIEW_OWNER,
        "items": items,
    }


@app.get("/governance/manual-reviews/history")
def list_governance_manual_review_history(
    limit: int = 200,
    run_id: str = "",
    reviewer: str = "",
    status: str = "",
    date_from: str = "",
    date_to: str = "",
):
    items = list_manual_review_history(limit)
    filtered = filter_manual_review_history(items, {
        "run_id": run_id,
        "reviewer": reviewer,
        "status": status,
        "date_from": date_from,
        "date_to": date_to,
    })
    return {
        "ok": True,
        "provider": MANUAL_REVIEW_OWNER,
        "items": filtered,
    }


@app.post("/governance/manual-reviews/submit")
def post_governance_manual_review_submit(req: ManualReviewSubmitReq):
    try:
        result = submit_manual_review(
            req.run_id,
            req.review_key,
            approved=req.approved,
            reviewer=req.reviewer,
            comment=req.comment,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {
        "ok": True,
        "provider": MANUAL_REVIEW_OWNER,
        "item": result["item"],
        "remaining": result["remaining"],
    }


@app.get("/governance/run-baselines")
def list_governance_run_baselines(limit: int = 200):
    return {
        "ok": True,
        "provider": RUN_BASELINE_OWNER,
        "items": list_run_baselines(limit),
    }


@app.get("/governance/run-baselines/{baseline_id}")
def get_governance_run_baseline(baseline_id: str):
    try:
        item = get_run_baseline(baseline_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    if item is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"baseline not found: {baseline_id}"},
        )
    return {"ok": True, "provider": RUN_BASELINE_OWNER, "item": item}


@app.put("/governance/run-baselines/{baseline_id}")
def put_governance_run_baseline(baseline_id: str, req: RunBaselineEnvelope):
    payload = req.baseline.model_dump()
    payload["baseline_id"] = str(baseline_id or payload.get("baseline_id") or "")
    try:
        item = save_run_baseline(payload)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "provider": RUN_BASELINE_OWNER, "item": item}


@app.exception_handler(Exception)
async def all_exception_handler(request, exc: Exception):
    error_id = uuid.uuid4().hex[:12]
    debug = _debug_errors_enabled()
    tb = traceback.format_exc() if debug else None
    if debug:
        log.error("Unhandled exception id=%s: %s\n%s", error_id, exc, tb)
    else:
        log.error("Unhandled exception id=%s: %s", error_id, exc)

    content: Dict[str, Any] = {
        "ok": False,
        "error": "internal server error",
        "error_id": error_id,
    }
    if debug:
        content["exception"] = str(exc)
        content["traceback"] = tb
    return JSONResponse(status_code=500, content=content)


@app.post("/jobs/{job_id}/run/{flow}")
def run_flow(job_id: str, flow: str, req: RunReq):
    t0 = time.time()
    flow = (flow or "").strip().lower()

    try:
        runner = runtime_catalog.get_flow_runner(flow)
    except KeyError:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"unknown flow: {flow}", "available_flows": runtime_catalog.list_flows()},
        )
    try:
        result = _run_flow_with_runner(job_id, req, runner)
    except LegacyFlowPathParamsError as exc:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": str(exc), "job_id": job_id, "flow": flow},
        )

    if isinstance(result, BaseModel):
        out = result.model_dump()
    elif isinstance(result, dict):
        out = result
    else:
        out = {"result": result}

    out.setdefault("ok", True)
    out.setdefault("job_id", job_id)
    out.setdefault("flow", flow)
    out.setdefault("seconds", round(time.time() - t0, 3))
    return out

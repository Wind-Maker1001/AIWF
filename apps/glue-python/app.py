import os
import json
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
from aiwf.governance_workflow_run_audit import (
    WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION,
    WORKFLOW_RUN_AUDIT_OWNER,
    WORKFLOW_RUN_AUDIT_SCHEMA_VERSION,
    failure_summary as workflow_failure_summary,
    get_workflow_run,
    list_workflow_audit_events,
    list_workflow_runs,
    record_workflow_audit_event,
    record_workflow_run,
    run_timeline as workflow_run_timeline,
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
    graph: Dict[str, Any] = Field(default_factory=dict)
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


class WorkflowRunRecordReq(BaseModel):
    ts: Optional[str] = None
    workflow_id: Optional[str] = None
    status: Optional[str] = None
    ok: Optional[bool] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    config: Dict[str, Any] = Field(default_factory=dict)
    result: Dict[str, Any] = Field(default_factory=dict)


class WorkflowRunRecordEnvelope(BaseModel):
    run: WorkflowRunRecordReq


class WorkflowAuditEventReq(BaseModel):
    ts: Optional[str] = None
    action: str
    detail: Dict[str, Any] = Field(default_factory=dict)


class WorkflowAuditEventEnvelope(BaseModel):
    event: WorkflowAuditEventReq


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
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
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
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "provider": QUALITY_RULE_SET_OWNER, "set": item}


@app.delete("/governance/quality-rule-sets/{set_id}")
def delete_governance_quality_rule_set(set_id: str):
    try:
        removed = remove_quality_rule_set(set_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
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
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
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
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
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


@app.get("/governance/workflow-runs")
def list_governance_workflow_runs(limit: int = 200):
    return {
        "ok": True,
        "provider": WORKFLOW_RUN_AUDIT_OWNER,
        "items": list_workflow_runs(limit),
    }


@app.get("/governance/workflow-runs/failure-summary")
def get_governance_workflow_failure_summary(limit: int = 400):
    return {
        "provider": WORKFLOW_RUN_AUDIT_OWNER,
        **workflow_failure_summary(limit),
    }


@app.get("/governance/workflow-runs/{run_id}")
def get_governance_workflow_run(run_id: str):
    try:
        item = get_workflow_run(run_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    if item is None:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"run not found: {run_id}"},
        )
    return {"ok": True, "provider": WORKFLOW_RUN_AUDIT_OWNER, "item": item}


@app.put("/governance/workflow-runs/{run_id}")
def put_governance_workflow_run(run_id: str, req: WorkflowRunRecordEnvelope):
    payload = req.run.model_dump()
    payload["run_id"] = str(run_id or "")
    try:
        item = record_workflow_run(payload)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "provider": WORKFLOW_RUN_AUDIT_OWNER, "item": item}


@app.get("/governance/workflow-runs/{run_id}/timeline")
def get_governance_workflow_run_timeline(run_id: str):
    try:
        result = workflow_run_timeline(run_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {
        "provider": WORKFLOW_RUN_AUDIT_OWNER,
        **result,
    }


@app.post("/governance/workflow-audit-events")
def post_governance_workflow_audit_event(req: WorkflowAuditEventEnvelope):
    try:
        item = record_workflow_audit_event(req.event.model_dump())
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "provider": WORKFLOW_RUN_AUDIT_OWNER, "item": item}


@app.get("/governance/workflow-audit-events")
def list_governance_workflow_audit_events(limit: int = 200, action: str = ""):
    return {
        "ok": True,
        "provider": WORKFLOW_RUN_AUDIT_OWNER,
        "items": list_workflow_audit_events(limit, action),
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

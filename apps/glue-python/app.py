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
from pydantic import BaseModel, ConfigDict, Field

from aiwf import ingest
from aiwf.cleaning_spec_v2 import (
    CLEANING_SPEC_V2_CONTRACT,
    build_header_mapping,
    build_quality_decisions,
    candidate_profiles_from_headers,
    get_canonical_profile_registry,
    reason_codes_from_quality_errors,
)
from aiwf.runtime_catalog import get_runtime_catalog
from aiwf.dependency_status import dependency_status
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
from aiwf.workflow_validation_client import (
    WORKFLOW_VALIDATION_UNAVAILABLE_CODE,
    WorkflowValidationFailure,
    WorkflowValidationUnavailable,
    validate_workflow_definition_authoritatively,
)
from aiwf.rust_client import workflow_reference_run_v1
from aiwf.flows.cleaning_flow_materialization import materialize_accel_outputs
from aiwf.flows.cleaning_orchestrator_support import collect_materialized_artifacts, build_success_result
from aiwf.flows.cleaning_runtime_support import sha256_file
from aiwf.flows.cleaning_transport import (
    base_artifact_upsert_impl,
    base_step_done_impl,
    base_step_fail_impl,
    base_step_start_impl,
    headers_from_params_impl,
    post_json_impl,
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
INGEST_EXTRACT_CONTRACT_AUTHORITY = "contracts/glue/ingest_extract.schema.json"
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


def _workflow_graph_validation_failure_response(
    provider: str,
    scope: str,
    exc: WorkflowValidationFailure,
) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "provider": provider,
            "error": str(exc),
            "error_code": WORKFLOW_GRAPH_ERROR_CODE,
            "error_scope": scope,
            "graph_contract": str(exc.graph_contract or WORKFLOW_GRAPH_CONTRACT_AUTHORITY),
            "error_item_contract": str(exc.error_item_contract or NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY),
            "error_items": list(exc.error_items or []),
            "notes": list(exc.notes or []),
        },
    )


def _workflow_validation_unavailable_response(
    provider: str,
    scope: str,
    exc: WorkflowValidationUnavailable,
) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "ok": False,
            "provider": provider,
            "error": str(exc),
            "error_code": WORKFLOW_VALIDATION_UNAVAILABLE_CODE,
            "error_scope": scope,
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


class RunReferenceReq(BaseModel):
    model_config = ConfigDict(extra="allow")
    version_id: str
    published_version_id: Optional[str] = None
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
    workflow_definition: Optional[Dict[str, Any]] = None
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


class IngestExtractReq(BaseModel):
    input_files: list[str] = Field(default_factory=list)
    input_path: str = ""
    text_split_by_line: bool = False
    ocr_enabled: bool = True
    ocr_lang: Optional[str] = None
    ocr_config: Optional[str] = None
    ocr_preprocess: Optional[str] = None
    xlsx_all_sheets: bool = True
    include_hidden_sheets: bool = False
    sheet_allowlist: list[str] = Field(default_factory=list)
    quality_rules: Dict[str, Any] = Field(default_factory=dict)
    image_rules: Dict[str, Any] = Field(default_factory=dict)
    xlsx_rules: Dict[str, Any] = Field(default_factory=dict)
    sheet_profiles: Dict[str, Any] = Field(default_factory=dict)
    canonical_profile: str = ""
    on_file_error: str = "raise"


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


def _resolve_reference_version_id(req: RunReferenceReq) -> str:
    forbidden = []
    extras = req.model_extra if isinstance(getattr(req, "model_extra", None), dict) else {}
    for key in ("flow", "graph", "workflow_definition"):
        if key in extras:
            forbidden.append(key)
    if forbidden:
        raise ValueError("run-reference must not include " + ", ".join(forbidden))
    version_id = str(req.version_id or "").strip().lower()
    if not version_id:
        raise ValueError("version_id is required")
    published_version_id = str(req.published_version_id or "").strip().lower()
    if published_version_id and published_version_id != version_id:
        raise ValueError("published_version_id must match version_id in the current compatibility stage")
    return version_id


def _resolve_reference_version_item(req: RunReferenceReq) -> tuple[str, Dict[str, Any]]:
    version_id = _resolve_reference_version_id(req)
    item = get_workflow_version(version_id)
    if item is None:
        raise ValueError(f"unknown workflow version reference: {version_id}")
    workflow_definition = item.get("workflow_definition")
    if not isinstance(workflow_definition, dict):
        raise ValueError(f"workflow version workflow_definition missing: {version_id}")
    validated = validate_workflow_definition_authoritatively(
        workflow_definition,
        accel_url=str(os.getenv("AIWF_ACCEL_URL") or "").strip(),
        allow_version_migration=False,
        require_non_empty_nodes=False,
        validation_scope="run",
    )
    normalized_workflow_definition = validated.get("normalized_workflow_definition")
    if not isinstance(normalized_workflow_definition, dict):
        raise ValueError(f"workflow version workflow_definition invalid: {version_id}")
    normalized_item = dict(item)
    normalized_item["workflow_definition"] = normalized_workflow_definition
    if validated.get("notes"):
        normalized_item["workflow_definition_notes"] = list(validated.get("notes") or [])
    return version_id, normalized_item


def _run_workflow_definition_reference(job_id: str, req: RunReferenceReq, version_item: Dict[str, Any]):
    workflow_definition = version_item.get("workflow_definition") if isinstance(version_item.get("workflow_definition"), dict) else {}
    workflow_id = str(workflow_definition.get("workflow_id") or "").strip().lower()
    if not workflow_id:
        raise ValueError(f"workflow version workflow_definition invalid: {str(version_item.get('version_id') or '')}")
    version_id = str(version_item.get("version_id") or "").strip()
    published_version_id = str(version_item.get("version_id") or "").strip()
    normalized_context = normalize_job_context(
        job_id,
        params=req.params,
        job_context=req.job_context,
    )
    params_obj = attach_job_context(
        req.params,
        job_context=normalized_context,
        trace_id=req.trace_id,
    )
    params_obj["workflow_reference"] = {
        "version_id": version_id,
        "published_version_id": published_version_id,
        "workflow_definition_source": "version_reference",
    }
    headers = headers_from_params_impl(params_obj, env_api_key=settings.api_key)
    input_uri = params_obj.get("input_uri") or os.path.join(normalized_context["job_root"], "")
    output_uri = params_obj.get("output_uri") or os.path.join(normalized_context["job_root"], "")
    started_at = time.time()
    base_step_start_impl(
        base_url=settings.base_url,
        job_id=job_id,
        step_id=workflow_id,
        actor=req.actor,
        ruleset_version=req.ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        params=params_obj,
        headers=headers,
        post_json=post_json_impl,
    )
    rust_out = workflow_reference_run_v1(
        workflow_definition=workflow_definition,
        version_id=version_id,
        published_version_id=published_version_id,
        job_id=job_id,
        actor=req.actor,
        ruleset_version=req.ruleset_version,
        trace_id=str(req.trace_id or ""),
        run_id=job_id,
        job_context=normalized_context,
        params=params_obj,
        base_url=str(os.getenv("AIWF_ACCEL_URL") or "").strip() or "http://127.0.0.1:18082",
    )
    if not rust_out.get("ok"):
        base_step_fail_impl(
            base_url=settings.base_url,
            job_id=job_id,
            step_id=workflow_id,
            actor=req.actor,
            error=str(rust_out.get("error") or "workflow reference execution failed"),
            headers=headers,
            post_json=post_json_impl,
        )
        return rust_out

    execution = rust_out.get("execution") if isinstance(rust_out.get("execution"), dict) else {}
    final_output = rust_out.get("final_output") if isinstance(rust_out.get("final_output"), dict) else {}
    effective_output = final_output if final_output else execution
    accel_outputs = effective_output.get("outputs") if isinstance(effective_output.get("outputs"), dict) else {}
    accel_profile = effective_output.get("profile") if isinstance(effective_output.get("profile"), dict) else {}
    materialized = materialize_accel_outputs(
        params_effective=params_obj,
        accel_outputs=accel_outputs,
        accel_profile=accel_profile,
        sha256_file=sha256_file,
    )
    artifacts = collect_materialized_artifacts(materialized)
    for artifact in artifacts:
        base_artifact_upsert_impl(
            base_url=settings.base_url,
            job_id=job_id,
            actor=req.actor,
            artifact_id=artifact["artifact_id"],
            kind=artifact["kind"],
            path=artifact["path"],
            sha256=artifact["sha256"],
            extra_json=None,
            headers=headers,
        )
    base_step_done_impl(
        base_url=settings.base_url,
        job_id=job_id,
        step_id=workflow_id,
        actor=req.actor,
        output_hash=str(materialized.get("sha_parquet") or ""),
        headers=headers,
        post_json=post_json_impl,
    )
    result = build_success_result(
        job_id=job_id,
        materialized=materialized,
        artifacts=artifacts,
        accel_result={
            "accel": {"attempted": True, "ok": True},
            "accel_validation_error": None,
            "use_accel_outputs": True,
            "accel_resp": effective_output,
        },
        started_at=started_at,
    )
    result["version_id"] = version_id
    result["published_version_id"] = published_version_id
    result["workflow_definition_source"] = "version_reference"
    result["workflow_id"] = workflow_id
    result["execution"] = execution
    result["final_output"] = effective_output
    result["operator"] = str(rust_out.get("operator") or "workflow_reference_run_v1")
    return result


def _run_workflow_reference(job_id: str, req: RunReferenceReq):
    _version_id, version_item = _resolve_reference_version_item(req)
    return _run_workflow_definition_reference(job_id, req, version_item)


app = FastAPI(title="AIWF glue-python", version="0.1.0")


@app.get("/health")
def health():
    return {
        "ok": True,
        "dependencies": dependency_status(),
        "ingest_sidecar": {
            "extract_route": "/ingest/extract",
            "contract": INGEST_EXTRACT_CONTRACT_AUTHORITY,
            "supported_modalities": ["txt", "docx", "pdf", "image", "xlsx"],
        },
    }


@app.get("/capabilities")
def capabilities():
    caps = runtime_catalog.capabilities()
    caps["ingest_sidecar"] = {
        "extract_route": "/ingest/extract",
        "contract": INGEST_EXTRACT_CONTRACT_AUTHORITY,
        "supported_modalities": ["txt", "docx", "pdf", "image", "xlsx"],
    }
    caps["cleaning_spec_v2"] = {
        "contract": CLEANING_SPEC_V2_CONTRACT,
        "profiles": sorted(get_canonical_profile_registry().keys()),
    }
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


def _ingest_extract_metadata(rows: list[dict[str, Any]], meta: Dict[str, Any], req: IngestExtractReq) -> Dict[str, Any]:
    sheet_frames = meta.get("sheet_frames") if isinstance(meta.get("sheet_frames"), list) else []
    header_labels: list[str] = []
    for frame in sheet_frames:
        if not isinstance(frame, dict):
            continue
        labels = frame.get("header_labels")
        if isinstance(labels, list):
            header_labels.extend([str(item) for item in labels if str(item).strip()])
    if not header_labels and rows:
        sample = rows[0] if isinstance(rows[0], dict) else {}
        header_labels.extend([str(item) for item in sample.keys() if str(item).strip()])
    canonical_profile = str(req.canonical_profile or "").strip().lower()
    header_mapping = build_header_mapping(
        header_labels,
        canonical_profile=canonical_profile,
        sheet_profiles=req.sheet_profiles,
    )
    candidate_profiles = candidate_profiles_from_headers(
        header_labels,
        sheet_profiles=req.sheet_profiles,
    )
    quality_report = meta.get("quality_report") if isinstance(meta.get("quality_report"), dict) else {}
    quality_blocked = bool(meta.get("quality_blocked"))
    quality_decisions = build_quality_decisions(
        quality_report=quality_report,
        quality_blocked=quality_blocked,
    )
    blocked_reason_codes = reason_codes_from_quality_errors(quality_report.get("errors") or [])
    return {
        "header_mapping": header_mapping,
        "candidate_profiles": candidate_profiles,
        "quality_decisions": quality_decisions,
        "blocked_reason_codes": blocked_reason_codes,
        "sample_rows": rows[: min(5, len(rows))],
    }


@app.post("/ingest/extract")
def ingest_extract(req: IngestExtractReq):
    raw_paths = []
    if str(req.input_path or "").strip():
        raw_paths.append(str(req.input_path).strip())
    raw_paths.extend([str(item).strip() for item in req.input_files if str(item).strip()])
    paths = list(dict.fromkeys(raw_paths))
    if not paths:
        return JSONResponse(status_code=400, content={"ok": False, "error": "input_path or input_files is required"})

    options = req.model_dump()
    file_results = []
    all_rows: list[dict[str, Any]] = []
    blocked_inputs: list[dict[str, Any]] = []
    all_image_blocks: list[dict[str, Any]] = []
    all_table_cells: list[dict[str, Any]] = []
    all_sheet_frames: list[dict[str, Any]] = []
    engine_trace: list[dict[str, Any]] = []
    for path in paths:
        try:
            rows, meta = ingest.load_rows_from_file(
                path,
                text_by_line=req.text_split_by_line,
                ocr_enabled=req.ocr_enabled,
                ocr_lang=req.ocr_lang,
                ocr_config=req.ocr_config,
                ocr_preprocess=req.ocr_preprocess,
                xlsx_all_sheets=req.xlsx_all_sheets,
                extra_options=options,
            )
        except Exception as exc:
            if str(req.on_file_error or "raise").strip().lower() == "raise":
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": str(exc), "path": path},
                )
            file_results.append(
                {
                    "path": path,
                    "ok": False,
                    "error": str(exc),
                    "input_format": "",
                    "rows": [],
                    "row_count": 0,
                    "quality_blocked": False,
                    "quality_report": None,
                    "quality_metrics": None,
                    "image_blocks": [],
                    "table_cells": [],
                    "sheet_frames": [],
                    "engine_trace": [],
                    "header_mapping": [],
                    "candidate_profiles": [],
                    "quality_decisions": [],
                    "blocked_reason_codes": [],
                    "sample_rows": [],
                }
            )
            continue
        file_results.append(
            {
                "path": path,
                "ok": True,
                "rows": rows,
                "row_count": len(rows),
                "input_format": meta.get("input_format"),
                "quality_blocked": bool(meta.get("quality_blocked")),
                "quality_report": meta.get("quality_report"),
                "quality_metrics": meta.get("quality_metrics"),
                "image_blocks": meta.get("image_blocks") if isinstance(meta.get("image_blocks"), list) else [],
                "table_cells": meta.get("table_cells") if isinstance(meta.get("table_cells"), list) else [],
                "sheet_frames": meta.get("sheet_frames") if isinstance(meta.get("sheet_frames"), list) else [],
                "engine_trace": meta.get("engine_trace") if isinstance(meta.get("engine_trace"), list) else [],
                **_ingest_extract_metadata(rows, meta, req),
            }
        )
        all_rows.extend(rows)
        all_image_blocks.extend(meta.get("image_blocks") if isinstance(meta.get("image_blocks"), list) else [])
        all_table_cells.extend(meta.get("table_cells") if isinstance(meta.get("table_cells"), list) else [])
        all_sheet_frames.extend(meta.get("sheet_frames") if isinstance(meta.get("sheet_frames"), list) else [])
        engine_trace.extend(meta.get("engine_trace") if isinstance(meta.get("engine_trace"), list) else [])
        if bool(meta.get("quality_blocked")):
            blocked_inputs.append(
                {
                    "path": path,
                    "error": str(meta.get("quality_error") or "quality blocked"),
                    "quality_report": meta.get("quality_report"),
                }
            )

    return {
        "ok": True,
        "rows": all_rows,
        "file_results": file_results,
        "image_blocks": all_image_blocks,
        "table_cells": all_table_cells,
        "sheet_frames": all_sheet_frames,
        "quality_metrics": [item.get("quality_metrics") for item in file_results if item.get("quality_metrics")],
        "engine_trace": engine_trace,
        "quality_blocked": len(blocked_inputs) > 0,
        "blocked_inputs": blocked_inputs,
        "header_mapping": file_results[0].get("header_mapping") if file_results else [],
        "candidate_profiles": file_results[0].get("candidate_profiles") if file_results else [],
        "quality_decisions": [
            decision
            for item in file_results
            if isinstance(item, dict)
            for decision in (item.get("quality_decisions") if isinstance(item.get("quality_decisions"), list) else [])
        ],
        "blocked_reason_codes": sorted(
            {
                str(code)
                for item in file_results
                if isinstance(item, dict)
                for code in (item.get("blocked_reason_codes") if isinstance(item.get("blocked_reason_codes"), list) else [])
                if str(code).strip()
            }
        ),
        "sample_rows": all_rows[: min(5, len(all_rows))],
        "contract": INGEST_EXTRACT_CONTRACT_AUTHORITY,
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
        workflow_definition = (
            payload.get("workflow_definition")
            if isinstance(payload.get("workflow_definition"), dict)
            else (payload.get("graph") if isinstance(payload.get("graph"), dict) else {})
        )
        validated = validate_workflow_definition_authoritatively(
            workflow_definition,
            accel_url=str(os.getenv("AIWF_ACCEL_URL") or "").strip(),
            allow_version_migration=False,
            require_non_empty_nodes=False,
            validation_scope="governance_write",
        )
        normalized_workflow_definition = validated.get("normalized_workflow_definition")
        if isinstance(normalized_workflow_definition, dict):
            payload["workflow_definition"] = normalized_workflow_definition
        payload.pop("graph", None)
        item = save_workflow_version(payload)
    except WorkflowValidationFailure as exc:
        return _workflow_graph_validation_failure_response(WORKFLOW_VERSION_OWNER, "workflow_version", exc)
    except WorkflowValidationUnavailable as exc:
        return _workflow_validation_unavailable_response(WORKFLOW_VERSION_OWNER, "workflow_version", exc)
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

    if flow == "workflow_reference":
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "workflow_reference bridge has been retired; use /jobs/{job_id}/run-reference",
                "job_id": job_id,
                "flow": flow,
            },
        )

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


@app.post("/jobs/{job_id}/run-reference")
def run_reference(job_id: str, req: RunReferenceReq):
    t0 = time.time()
    try:
        version_id, _version_item = _resolve_reference_version_item(req)
        result = _run_workflow_definition_reference(job_id, req, _version_item)
    except WorkflowValidationFailure as exc:
        return _workflow_graph_validation_failure_response("glue-python", "workflow_reference_run", exc)
    except WorkflowValidationUnavailable as exc:
        return _workflow_validation_unavailable_response("glue-python", "workflow_reference_run", exc)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": str(exc), "job_id": job_id, "version_id": str(req.version_id or "")},
        )
    except LegacyFlowPathParamsError as exc:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": str(exc), "job_id": job_id, "version_id": version_id},
        )

    if isinstance(result, BaseModel):
        out = result.model_dump()
    elif isinstance(result, dict):
        out = result
    else:
        out = {"result": result}

    if out.get("ok") is False:
        status_code = 503 if str(out.get("error_code") or "") == WORKFLOW_VALIDATION_UNAVAILABLE_CODE else 400
        return JSONResponse(status_code=status_code, content=out)

    out.setdefault("ok", True)
    out.setdefault("job_id", job_id)
    out.setdefault("version_id", version_id)
    out.setdefault("published_version_id", version_id)
    out.setdefault("seconds", round(time.time() - t0, 3))
    return out

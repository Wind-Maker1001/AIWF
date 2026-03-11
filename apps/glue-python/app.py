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
    return {"ok": True, "capabilities": runtime_catalog.capabilities()}


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

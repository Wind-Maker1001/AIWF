import os
import json
import time
import traceback
import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("glue")


def _default_jobs_root() -> str:
    root = os.getenv("AIWF_ROOT")
    if root:
        return os.path.join(root, "bus", "jobs")
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "bus", "jobs"))


class Settings(BaseModel):
    base_url: str = Field(default_factory=lambda: os.getenv("AIWF_BASE_URL", "http://127.0.0.1:18080"))
    jobs_root: str = Field(default_factory=lambda: os.getenv("AIWF_JOBS_ROOT", _default_jobs_root()))
    api_key: Optional[str] = Field(default_factory=lambda: os.getenv("AIWF_API_KEY"))
    timeout_seconds: float = Field(default_factory=lambda: float(os.getenv("AIWF_HTTP_TIMEOUT", "30")))


settings = Settings()


def _debug_errors_enabled() -> bool:
    env_mode = str(os.getenv("AIWF_ENV") or "").strip().lower()
    if env_mode in {"prod", "production"} or str(os.getenv("AIWF_RELEASE") or "").strip() == "1":
        return False
    v = str(os.getenv("AIWF_DEBUG_ERRORS") or os.getenv("AIWF_DEBUG") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


class RunReq(BaseModel):
    actor: str = "glue"
    ruleset_version: str = "v1"
    params: Dict[str, Any] = Field(default_factory=dict)


def make_base_client():
    """
    Build BaseClient with best-effort compatibility for different constructor signatures.
    """
    try:
        from aiwf.base_client import BaseClient  # type: ignore

        try:
            return BaseClient(settings.base_url, api_key=settings.api_key, timeout=settings.timeout_seconds)
        except TypeError:
            try:
                return BaseClient(settings.base_url, settings.api_key)
            except TypeError:
                return BaseClient(settings.base_url)
    except Exception as e:
        log.warning("make_base_client: cannot import/use aiwf.base_client.BaseClient: %s", e)
        return None


def run_cleaning_flow(job_id: str, req: RunReq):
    """
    Compatibility wrapper for multiple run_cleaning signatures.
    """
    base = make_base_client()

    try:
        from aiwf.flows.cleaning import run_cleaning  # type: ignore
    except Exception as e:
        raise RuntimeError(f"cannot import aiwf.flows.cleaning.run_cleaning: {e}")

    params_obj = req.params or {}
    params_json = json.dumps(params_obj, ensure_ascii=False)

    try:
        return run_cleaning(
            job_id=job_id,
            actor=req.actor,
            ruleset_version=req.ruleset_version,
            s=settings,
            base=base,
            params=params_obj,
        )
    except TypeError:
        pass

    try:
        return run_cleaning(
            job_id=job_id,
            actor=req.actor,
            ruleset_version=req.ruleset_version,
            s=settings,
            base=base,
            params_json=params_json,
        )
    except TypeError:
        pass

    try:
        return run_cleaning(
            job_id=job_id,
            actor=req.actor,
            ruleset_version=req.ruleset_version,
            s=settings,
            base=base,
        )
    except TypeError:
        pass

    return run_cleaning(job_id=job_id, actor=req.actor, ruleset_version=req.ruleset_version)


app = FastAPI(title="AIWF glue-python", version="0.1.0")


@app.get("/health")
def health():
    return {"ok": True}


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

    if flow != "cleaning":
        return JSONResponse(status_code=404, content={"ok": False, "error": f"unknown flow: {flow}"})

    result = run_cleaning_flow(job_id, req)

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

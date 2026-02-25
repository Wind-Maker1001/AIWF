function normalizeDifyRequest(payload) {
  const p = payload && typeof payload === "object" ? payload : {};
  return {
    owner: String(p.owner || "dify"),
    actor: String(p.actor || "dify"),
    ruleset_version: String(p.ruleset_version || "v1"),
    params: p.params && typeof p.params === "object" ? p.params : {},
  };
}

function normalizeDifyResponse(resp) {
  const r = resp && typeof resp === "object" ? resp : {};
  return {
    ok: !!r.ok,
    job_id: String(r.job_id || ""),
    run_id: String(r.run_id || ""),
    status: String(r.status || (r.ok ? "done" : "failed")),
    artifacts: Array.isArray(r.artifacts) ? r.artifacts : [],
    quality: r.quality && typeof r.quality === "object" ? r.quality : {},
    raw: r,
  };
}

function normalizeDifyError(err, httpStatus) {
  const msg = String((err && err.error) || err || "unknown error");
  const status = Number(httpStatus || 0);
  const code =
    status === 401 || status === 403
      ? "AUTH_FAILED"
      : status === 408 || status === 504
        ? "TIMEOUT"
        : status >= 500
          ? "UPSTREAM_5XX"
          : "UPSTREAM_4XX";
  return { ok: false, code, status, error: msg };
}

function normalizeDifyAiConfig(raw) {
  const x = raw && typeof raw === "object" ? raw : {};
  const router = x.router && typeof x.router === "object" ? x.router : {};
  return {
    provider: String(x.provider || "openai"),
    model: String(x.model || ""),
    endpoint: String(x.endpoint || ""),
    api_key: String(x.api_key || ""),
    timeout_ms: Math.max(3000, Number(x.timeout_ms || 120000)),
    max_retries: Math.max(0, Number(x.max_retries || 2)),
    prompt_template_id: String(x.prompt_template_id || ""),
    router: {
      strategy: String(router.strategy || "primary_first"),
      endpoints: Array.isArray(router.endpoints)
        ? router.endpoints
          .filter((e) => e && typeof e === "object")
          .map((e) => ({
            name: String(e.name || ""),
            endpoint: String(e.endpoint || ""),
            weight: Math.max(0, Number(e.weight || 1)),
            local_only: e.local_only === true,
          }))
        : [],
    },
  };
}

function isLocalEndpoint(endpoint) {
  const s = String(endpoint || "").trim();
  if (!s) return false;
  try {
    const u = new URL(s);
    const host = String(u.hostname || "").toLowerCase();
    return host === "127.0.0.1" || host === "localhost" || host === "::1";
  } catch {
    return false;
  }
}

function pickDifyEndpoint(aiConfig) {
  const cfg = normalizeDifyAiConfig(aiConfig);
  const eps = cfg.router.endpoints.filter((e) => String(e.endpoint || "").trim());
  if (!eps.length) return cfg.endpoint;
  const strategy = String(cfg.router.strategy || "primary_first").toLowerCase();
  if (strategy === "prefer_local") {
    const local = eps.find((e) => isLocalEndpoint(e.endpoint));
    return local ? local.endpoint : eps[0].endpoint;
  }
  if (strategy === "weighted") {
    const total = eps.reduce((s, e) => s + Math.max(0, Number(e.weight || 0)), 0);
    if (total > 0) {
      let x = Math.random() * total;
      for (const e of eps) {
        x -= Math.max(0, Number(e.weight || 0));
        if (x <= 0) return e.endpoint;
      }
    }
  }
  return eps[0].endpoint;
}

function buildDifyInvokePayload(baseReq, aiConfig, cookedPayload) {
  const req = normalizeDifyRequest(baseReq);
  const ai = normalizeDifyAiConfig(aiConfig);
  return {
    ...req,
    params: {
      ...req.params,
      ai: {
        provider: ai.provider,
        model: ai.model,
        endpoint: pickDifyEndpoint(ai),
        api_key: ai.api_key,
        timeout_ms: ai.timeout_ms,
        max_retries: ai.max_retries,
        prompt_template_id: ai.prompt_template_id,
      },
      cooked_payload: cookedPayload && typeof cookedPayload === "object" ? cookedPayload : {},
    },
  };
}

module.exports = {
  normalizeDifyRequest,
  normalizeDifyResponse,
  normalizeDifyError,
  normalizeDifyAiConfig,
  pickDifyEndpoint,
  buildDifyInvokePayload,
};

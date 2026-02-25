const {
  computeViaRust,
  callExternalAi,
} = require("../workflow_services");
const { summarizeCorpus } = require("../workflow_utils");

function isLocalEndpoint(raw) {
  const s = String(raw || "").trim();
  if (!s) return true;
  try {
    const u = new URL(s);
    const host = String(u.hostname || "").toLowerCase();
    return host === "127.0.0.1" || host === "localhost" || host === "::1";
  } catch {
    return false;
  }
}

function enforceSandboxNetwork(task, payload) {
  const level = String(payload?.isolation_level || "").trim().toLowerCase();
  if (level !== "sandbox") return;
  if (task === "compute_rust") {
    const endpoint = String(payload?.options?.rust_endpoint || "").trim();
    if (!isLocalEndpoint(endpoint)) throw new Error(`sandbox_egress_blocked:${task}`);
    return;
  }
  if (task === "rust_operator_http") {
    const base = String(payload?.base || "").trim();
    if (!isLocalEndpoint(base)) throw new Error(`sandbox_egress_blocked:${task}`);
    return;
  }
  if (task === "ai_call" || task === "ai_refine") {
    const endpoint = String(payload?.workflowPayload?.ai?.endpoint || "").trim();
    if (endpoint && !isLocalEndpoint(endpoint)) throw new Error(`sandbox_egress_blocked:${task}`);
  }
}

function normalizeSandboxLimits(payload) {
  const src = payload?.sandbox_limits && typeof payload.sandbox_limits === "object" ? payload.sandbox_limits : {};
  function pick(name, def) {
    const n = Number(src[name]);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
  }
  return {
    max_duration_ms: pick("max_duration_ms", Number(process.env.AIWF_SANDBOX_MAX_DURATION_MS || 180000)),
    max_cpu_ms: pick("max_cpu_ms", Number(process.env.AIWF_SANDBOX_MAX_CPU_MS || 120000)),
    max_rss_mb: pick("max_rss_mb", Number(process.env.AIWF_SANDBOX_MAX_RSS_MB || 512)),
    max_output_bytes: pick("max_output_bytes", Number(process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES || 2000000)),
  };
}

async function withSandboxResourceLimits(payload, fn) {
  const level = String(payload?.isolation_level || "").trim().toLowerCase();
  if (level !== "sandbox") return await fn();
  const limits = normalizeSandboxLimits(payload);
  const startedAt = Date.now();
  const cpuStart = process.cpuUsage();
  let finished = false;
  let timer = null;
  const guard = new Promise((_, reject) => {
    timer = setInterval(() => {
      if (finished) return;
      const elapsed = Date.now() - startedAt;
      if (elapsed > limits.max_duration_ms) {
        reject(new Error("sandbox_limit_exceeded:duration"));
        return;
      }
      const cpu = process.cpuUsage(cpuStart);
      const cpuMs = (Number(cpu.user || 0) + Number(cpu.system || 0)) / 1000;
      if (cpuMs > limits.max_cpu_ms) {
        reject(new Error("sandbox_limit_exceeded:cpu"));
        return;
      }
      const rssMb = process.memoryUsage().rss / (1024 * 1024);
      if (rssMb > limits.max_rss_mb) {
        reject(new Error("sandbox_limit_exceeded:rss"));
      }
    }, 100);
  });
  try {
    const out = await Promise.race([fn(), guard]);
    const bytes = Buffer.byteLength(JSON.stringify(out || {}), "utf8");
    if (bytes > limits.max_output_bytes) throw new Error("sandbox_limit_exceeded:output");
    return out;
  } finally {
    finished = true;
    if (timer) clearInterval(timer);
  }
}

process.on("message", async (msg) => {
  try {
    const reqType = String(msg?.type || "");
    const requestId = String(msg?.id || "");
    const task = String(msg?.task || "");
    const payload = msg?.payload || {};
    if (reqType && reqType !== "run") return;
    if (!task) throw new Error("missing task");
    enforceSandboxNetwork(task, payload);

    if (task === "compute_rust") {
      const out = await withSandboxResourceLimits(payload, async () => await computeViaRust(String(payload.corpusText || ""), payload.options || {}));
      process.send?.({ type: "ok", id: requestId, data: out });
      return;
    }

    if (task === "rust_operator_http") {
      const base = String(payload.base || "").replace(/\/$/, "");
      const operatorPath = String(payload.operator_path || "");
      const body = payload.body && typeof payload.body === "object" ? payload.body : {};
      const required = payload.required !== false;
      const out = await withSandboxResourceLimits(payload, async () => {
        const resp = await fetch(`${base}${operatorPath}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const txt = await resp.text();
        if (!resp.ok) {
          if (!required) return { ok: false, status: "fallback", detail: `rust_http_${resp.status}:${txt.slice(0, 240)}` };
          throw new Error(`rust_http_${resp.status}:${txt.slice(0, 240)}`);
        }
        return txt ? JSON.parse(txt) : {};
      });
      process.send?.({
        type: "ok",
        id: requestId,
        data: {
          ok: !!out.ok,
          operator: out.operator || payload.operator_name || "rust_operator_http",
          status: out.status || "done",
          detail: out,
        },
      });
      return;
    }

    if (task === "ai_refine") {
      const corpusText = String(payload.corpusText || "");
      const metrics = payload.metrics && typeof payload.metrics === "object"
        ? payload.metrics
        : summarizeCorpus(corpusText);
      const out = await withSandboxResourceLimits(payload, async () => await callExternalAi(payload.workflowPayload || {}, corpusText, metrics));
      process.send?.({ type: "ok", id: requestId, data: out });
      return;
    }

    if (task === "ai_call") {
      const corpusText = String(payload.corpusText || "");
      const metrics = payload.metrics && typeof payload.metrics === "object"
        ? payload.metrics
        : summarizeCorpus(corpusText);
      const out = await withSandboxResourceLimits(payload, async () => await callExternalAi(payload.workflowPayload || {}, corpusText, metrics));
      process.send?.({ type: "ok", id: requestId, data: out });
      return;
    }

    throw new Error(`unsupported isolated task: ${task}`);
  } catch (e) {
    process.send?.({
      type: "error",
      id: String(msg?.id || ""),
      error: String(e && e.stack ? e.stack : e),
    });
  }
});

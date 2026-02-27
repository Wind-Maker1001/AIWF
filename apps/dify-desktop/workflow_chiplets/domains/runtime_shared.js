function createRuntimeSharedHelpers({ runIsolatedTask }) {
  function rustBase(ctx) {
    return String(ctx?.payload?.rust?.endpoint || "http://127.0.0.1:18082").replace(/\/$/, "");
  }

  function rustRequired(ctx) {
    return ctx?.payload?.rust?.required !== false;
  }

  function parseIsolationTypeList(ctx) {
    const fromPayload = Array.isArray(ctx?.payload?.chiplet_isolated_types)
      ? ctx.payload.chiplet_isolated_types
      : [];
    const fromCfg = Array.isArray(ctx?.config?.chiplet_isolated_types)
      ? ctx.config.chiplet_isolated_types
      : [];
    const fromEnv = String(process.env.AIWF_CHIPLET_ISOLATED_TYPES || "")
      .split(/[;,]/)
      .map((x) => String(x || "").trim())
      .filter(Boolean);
    return Array.from(new Set([...fromPayload, ...fromCfg, ...fromEnv].map((x) => String(x || "").trim()).filter(Boolean)));
  }

  function resolveSandboxLimits(ctx, node = null) {
    const env = {
      max_duration_ms: Number(process.env.AIWF_SANDBOX_MAX_DURATION_MS || 180000),
      max_cpu_ms: Number(process.env.AIWF_SANDBOX_MAX_CPU_MS || 120000),
      max_rss_mb: Number(process.env.AIWF_SANDBOX_MAX_RSS_MB || 512),
      max_output_bytes: Number(process.env.AIWF_SANDBOX_MAX_OUTPUT_BYTES || 2000000),
    };
    const cfg = ctx?.config?.sandbox_limits && typeof ctx.config.sandbox_limits === "object" ? ctx.config.sandbox_limits : {};
    const payload = ctx?.payload?.sandbox_limits && typeof ctx.payload.sandbox_limits === "object" ? ctx.payload.sandbox_limits : {};
    const nodeCfg = node?.config?.sandbox_limits && typeof node.config.sandbox_limits === "object" ? node.config.sandbox_limits : {};
    function pick(name, def) {
      const n = Number(nodeCfg[name] ?? payload[name] ?? cfg[name] ?? def);
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
    }
    return {
      max_duration_ms: pick("max_duration_ms", env.max_duration_ms),
      max_cpu_ms: pick("max_cpu_ms", env.max_cpu_ms),
      max_rss_mb: pick("max_rss_mb", env.max_rss_mb),
      max_output_bytes: pick("max_output_bytes", env.max_output_bytes),
    };
  }

  function resolveIsolationLevel(ctx, nodeType, defaultOn = false, node = null) {
    if (typeof runIsolatedTask !== "function") return "none";
    const nodeCfg = node?.config && typeof node.config === "object" ? node.config : {};
    const nodeLevel = String(nodeCfg.isolation_level || "").trim().toLowerCase();
    if (nodeLevel === "none" || nodeLevel === "off" || nodeLevel === "disabled") return "none";
    if (nodeLevel === "process" || nodeLevel === "sandbox") return nodeLevel;
    if (ctx?.payload?.chiplet_isolation_enabled === false) return "none";
    if (ctx?.config?.chiplet_isolation_enabled === false) return "none";
    const mode = String(
      ctx?.payload?.chiplet_isolation_mode
      || ctx?.config?.chiplet_isolation_mode
      || process.env.AIWF_CHIPLET_ISOLATION_MODE
      || "high_risk"
    ).trim().toLowerCase();
    if (mode === "off" || mode === "none" || mode === "disabled") return "none";
    if (mode === "all") return "process";
    const t = String(nodeType || "").trim();
    const list = parseIsolationTypeList(ctx);
    if (list.includes("*") || list.includes(t)) return "process";
    return defaultOn ? "process" : "none";
  }

  return {
    rustBase,
    rustRequired,
    resolveIsolationLevel,
    resolveSandboxLimits,
  };
}

module.exports = {
  createRuntimeSharedHelpers,
};

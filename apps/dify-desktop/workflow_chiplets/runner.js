const {
  validateEnvelope,
  validateChipletOutput,
} = require("./contract");

const CIRCUIT = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, ms, label) {
  const timeout = Math.max(1000, Number(ms) || 180000);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`chiplet timeout: ${label}`)), timeout);
    Promise.resolve(promise)
      .then((v) => resolve(v))
      .catch((e) => reject(e))
      .finally(() => clearTimeout(timer));
  });
}

function circuitKey(nodeType, chipletId) {
  return `${String(nodeType || "")}::${String(chipletId || "")}`;
}

function circuitState(nodeType, chipletId) {
  const key = circuitKey(nodeType, chipletId);
  if (!CIRCUIT.has(key)) CIRCUIT.set(key, { failures: 0, opened_until: 0 });
  return CIRCUIT.get(key);
}

function checkCircuit(nodeType, chiplet) {
  if (!chiplet?.circuit?.enabled) return;
  const st = circuitState(nodeType, chiplet.id);
  if (Date.now() < st.opened_until) {
    throw new Error(`chiplet circuit open: ${nodeType}`);
  }
}

function markCircuitSuccess(nodeType, chiplet) {
  if (!chiplet?.circuit?.enabled) return;
  const st = circuitState(nodeType, chiplet.id);
  st.failures = 0;
  st.opened_until = 0;
}

function markCircuitFailure(nodeType, chiplet) {
  if (!chiplet?.circuit?.enabled) return;
  const st = circuitState(nodeType, chiplet.id);
  st.failures += 1;
  const threshold = Number(chiplet.circuit.failure_threshold || 3);
  if (st.failures >= threshold) {
    st.opened_until = Date.now() + Number(chiplet.circuit.cooldown_ms || 30000);
  }
}

function runnerPolicy(chiplet) {
  return {
    timeout_ms: Number(chiplet?.timeout_ms || process.env.AIWF_CHIPLET_NODE_TIMEOUT_MS || 180000),
    retries: Number(chiplet?.retries || process.env.AIWF_CHIPLET_RETRIES || 0),
  };
}

async function runWithRetry({ chiplet, nodeType, ctx, node, env }) {
  const policy = runnerPolicy(chiplet);
  const retries = Number.isFinite(policy.retries) && policy.retries >= 0 ? Math.floor(policy.retries) : 0;
  let lastErr = null;
  const started = Date.now();
  for (let i = 0; i <= retries; i += 1) {
    try {
      const out = await withTimeout(chiplet.run(ctx, node, env), policy.timeout_ms, nodeType);
      markCircuitSuccess(nodeType, chiplet);
      const durationMs = Date.now() - started;
      return {
        ...out,
        __runner: {
          attempts: i + 1,
          retries,
          timeout_ms: policy.timeout_ms,
          duration_ms: durationMs,
          circuit_enabled: !!chiplet?.circuit?.enabled,
        },
      };
    } catch (e) {
      lastErr = e;
      markCircuitFailure(nodeType, chiplet);
      if (i < retries) {
        // exponential backoff keeps retries bounded and deterministic.
        // eslint-disable-next-line no-await-in-loop
        await sleep(Math.min(1200, 100 * (2 ** i)));
      }
    }
  }
  throw lastErr || new Error(`chiplet failed: ${nodeType}`);
}

async function runChipletNode({ registry, node, ctx, envelope }) {
  const t = String(node?.type || "").trim();
  const chiplet = registry.resolve(t);
  if (!chiplet) throw new Error(`unsupported node type: ${t}`);
  checkCircuit(t, chiplet);
  const env = envelope || {};
  validateEnvelope(env, node);
  const out = await runWithRetry({ chiplet, nodeType: t, ctx, node, env });
  validateChipletOutput(out, t);
  return out;
}

function __resetRunnerCircuitForTests() {
  CIRCUIT.clear();
}

module.exports = {
  runChipletNode,
  __resetRunnerCircuitForTests,
};

const CHIPLET_SCHEMA_VERSION = "aiwf.chiplet.v1";
const CHIPLET_ERROR_SCHEMA_VERSION = "aiwf.chiplet.error.v1";

const CHIPLET_ERROR_CATALOG = {
  unsupported_node_type: { kind: "routing", retryable: false },
  envelope_missing: { kind: "contract", retryable: false },
  envelope_invalid_version: { kind: "contract", retryable: false },
  envelope_missing_field: { kind: "contract", retryable: false },
  envelope_node_mismatch: { kind: "contract", retryable: false },
  output_invalid: { kind: "contract", retryable: false },
  runner_timeout: { kind: "timeout", retryable: true },
  circuit_open: { kind: "circuit", retryable: true },
  node_execution_failed: { kind: "runtime", retryable: true },
  workflow_incomplete: { kind: "runtime", retryable: false },
};

function buildEnvelope(base = {}) {
  return {
    schema_version: CHIPLET_SCHEMA_VERSION,
    run_id: String(base.run_id || ""),
    workflow_id: String(base.workflow_id || ""),
    node_id: String(base.node_id || ""),
    node_type: String(base.node_type || ""),
    trace_id: String(base.trace_id || ""),
    parent_span_id: String(base.parent_span_id || ""),
    span_id: String(base.span_id || ""),
    ts: new Date().toISOString(),
  };
}

function validateEnvelope(envelope, node) {
  if (!envelope || typeof envelope !== "object") {
    throw createChipletError("envelope_missing", "chiplet envelope missing");
  }
  if (String(envelope.schema_version || "") !== CHIPLET_SCHEMA_VERSION) {
    throw createChipletError(
      "envelope_invalid_version",
      `unsupported chiplet schema_version: ${String(envelope.schema_version || "")}`
    );
  }
  const required = ["run_id", "workflow_id", "node_id", "node_type"];
  for (const k of required) {
    if (!String(envelope[k] || "").trim()) {
      throw createChipletError("envelope_missing_field", `chiplet envelope missing ${k}`, { field: k });
    }
  }
  if (node && String(node.id || "") !== String(envelope.node_id || "")) {
    throw createChipErrorNodeMismatch("node_id");
  }
  if (node && String(node.type || "") !== String(envelope.node_type || "")) {
    throw createChipErrorNodeMismatch("node_type");
  }
}

function createChipErrorNodeMismatch(field) {
  return createChipletError("envelope_node_mismatch", `chiplet envelope ${field} mismatch`, { field });
}

function assertString(value, label) {
  if (!String(value || "").trim()) throw new Error(`chiplet output missing ${label}`);
}

function assertNumber(value, label) {
  if (!Number.isFinite(Number(value))) throw new Error(`chiplet output invalid ${label}`);
}
function assertInteger(value, label) {
  if (!Number.isInteger(Number(value))) throw new Error(`chiplet output invalid ${label}`);
}

function assertBoolean(value, label) {
  if (typeof value !== "boolean") throw new Error(`chiplet output invalid ${label}`);
}

function assertArray(value, label) {
  if (!Array.isArray(value)) throw new Error(`chiplet output invalid ${label}`);
}

function assertObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`chiplet output invalid ${label}`);
  }
}

function validateByNodeType(out, nodeType) {
  const t = String(nodeType || "");
  if (!t) return;
  if (t === "ingest_files") {
    assertArray(out.input_files, "input_files");
    assertInteger(out.count, "count");
    if (out.count !== out.input_files.length) throw new Error("chiplet output count mismatch input_files length");
    return;
  }
  if (t === "clean_md") {
    assertString(out.job_id, "job_id");
    assertString(out.ai_corpus_path, "ai_corpus_path");
    assertArray(out.warnings, "warnings");
    return;
  }
  if (t === "compute_rust" || t === "compute_rust_placeholder") {
    assertString(out.engine, "engine");
    assertObject(out.metrics, "metrics");
    assertBoolean(out.rust_started, "rust_started");
    assertInteger(out.metrics.sections, "metrics.sections");
    assertInteger(out.metrics.bullets, "metrics.bullets");
    assertInteger(out.metrics.chars, "metrics.chars");
    assertInteger(out.metrics.cjk, "metrics.cjk");
    assertInteger(out.metrics.latin, "metrics.latin");
    assertString(out.metrics.sha256, "metrics.sha256");
    return;
  }
  if (t === "ai_refine") {
    assertString(out.ai_mode, "ai_mode");
    assertInteger(out.ai_text_chars, "ai_text_chars");
    return;
  }
  if (t === "ai_audit") {
    assertBoolean(out.passed, "passed");
    assertArray(out.reasons, "reasons");
    assertString(out.metrics_hash, "metrics_hash");
    assertString(out.ai_hash, "ai_hash");
    return;
  }
  if (t === "md_output") {
    assertString(out.artifact_id, "artifact_id");
    assertString(out.kind, "kind");
    if (String(out.kind) !== "md") throw new Error("chiplet output invalid kind");
    assertString(out.path, "path");
    if (!String(out.path).toLowerCase().endsWith(".md")) throw new Error("chiplet output path must end with .md");
    assertString(out.sha256, "sha256");
    return;
  }
  if (t === "manual_review") {
    assertBoolean(out.approved, "approved");
    assertString(out.status, "status");
    assertString(out.review_key, "review_key");
    return;
  }
}

function validateChipletOutput(out, nodeType) {
  if (!out || typeof out !== "object" || Array.isArray(out)) {
    throw createChipletError("output_invalid", "chiplet output must be an object");
  }
  try {
    JSON.stringify(out);
  } catch {
    throw createChipletError("output_invalid", "chiplet output must be JSON-serializable");
  }
  try {
    validateByNodeType(out, nodeType);
  } catch (e) {
    throw createChipletError("output_invalid", String(e && e.message ? e.message : e));
  }
}

function createChipletError(code, message, details = {}) {
  const c = String(code || "node_execution_failed");
  const meta = CHIPLET_ERROR_CATALOG[c] || CHIPLET_ERROR_CATALOG.node_execution_failed;
  const err = new Error(String(message || c));
  err.name = "ChipletContractError";
  err.schema_version = CHIPLET_ERROR_SCHEMA_VERSION;
  err.code = c;
  err.kind = String(meta.kind || "runtime");
  err.retryable = meta.retryable !== false;
  err.details = details && typeof details === "object" ? { ...details } : {};
  return err;
}

function normalizeChipletError(error, fallbackCode = "node_execution_failed") {
  if (error && typeof error === "object" && error.code && error.kind && error.message) {
    return {
      schema_version: String(error.schema_version || CHIPLET_ERROR_SCHEMA_VERSION),
      code: String(error.code),
      kind: String(error.kind),
      retryable: error.retryable !== false,
      message: String(error.message),
      details: error.details && typeof error.details === "object" ? { ...error.details } : {},
    };
  }
  const raw = String(error && error.message ? error.message : error || "");
  const lower = raw.toLowerCase();
  let code = String(fallbackCode || "node_execution_failed");
  if (lower.includes("timeout")) code = "runner_timeout";
  else if (lower.includes("circuit open")) code = "circuit_open";
  else if (lower.includes("unsupported node type")) code = "unsupported_node_type";
  else if (lower.includes("envelope")) code = "envelope_missing";
  else if (lower.includes("output")) code = "output_invalid";
  const meta = CHIPLET_ERROR_CATALOG[code] || CHIPLET_ERROR_CATALOG.node_execution_failed;
  return {
    schema_version: CHIPLET_ERROR_SCHEMA_VERSION,
    code,
    kind: String(meta.kind || "runtime"),
    retryable: meta.retryable !== false,
    message: raw || code,
    details: {},
  };
}

function toError(normalized) {
  const n = normalized && typeof normalized === "object" ? normalized : {};
  const err = new Error(String(n.message || "chiplet error"));
  err.name = "ChipletRuntimeError";
  err.schema_version = String(n.schema_version || CHIPLET_ERROR_SCHEMA_VERSION);
  err.code = String(n.code || "node_execution_failed");
  err.kind = String(n.kind || "runtime");
  err.retryable = n.retryable !== false;
  err.details = n.details && typeof n.details === "object" ? { ...n.details } : {};
  return err;
}

module.exports = {
  CHIPLET_SCHEMA_VERSION,
  CHIPLET_ERROR_SCHEMA_VERSION,
  buildEnvelope,
  validateEnvelope,
  validateChipletOutput,
  createChipletError,
  normalizeChipletError,
  toError,
};

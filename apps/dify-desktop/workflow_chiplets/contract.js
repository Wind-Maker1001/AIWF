const CHIPLET_SCHEMA_VERSION = "aiwf.chiplet.v1";

function buildEnvelope(base = {}) {
  return {
    schema_version: CHIPLET_SCHEMA_VERSION,
    run_id: String(base.run_id || ""),
    workflow_id: String(base.workflow_id || ""),
    node_id: String(base.node_id || ""),
    node_type: String(base.node_type || ""),
    ts: new Date().toISOString(),
  };
}

function validateEnvelope(envelope, node) {
  if (!envelope || typeof envelope !== "object") throw new Error("chiplet envelope missing");
  if (String(envelope.schema_version || "") !== CHIPLET_SCHEMA_VERSION) {
    throw new Error(`unsupported chiplet schema_version: ${String(envelope.schema_version || "")}`);
  }
  const required = ["run_id", "workflow_id", "node_id", "node_type"];
  for (const k of required) {
    if (!String(envelope[k] || "").trim()) throw new Error(`chiplet envelope missing ${k}`);
  }
  if (node && String(node.id || "") !== String(envelope.node_id || "")) {
    throw new Error("chiplet envelope node_id mismatch");
  }
  if (node && String(node.type || "") !== String(envelope.node_type || "")) {
    throw new Error("chiplet envelope node_type mismatch");
  }
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
  }
}

function validateChipletOutput(out, nodeType) {
  if (!out || typeof out !== "object" || Array.isArray(out)) {
    throw new Error("chiplet output must be an object");
  }
  try {
    JSON.stringify(out);
  } catch {
    throw new Error("chiplet output must be JSON-serializable");
  }
  validateByNodeType(out, nodeType);
}

module.exports = {
  CHIPLET_SCHEMA_VERSION,
  buildEnvelope,
  validateEnvelope,
  validateChipletOutput,
};

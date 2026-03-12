const IO_CONTRACT_COMPATIBLE_OPERATORS = new Set([
  "transform_rows_v2",
  "transform_rows_v3",
  "load_rows_v3",
  "finance_ratio_v1",
  "anomaly_explain_v1",
  "stream_window_v2",
  "plugin_operator_v1",
]);

function buildIoContractInput(operator, nodeConfig, firstInputFile = "") {
  const op = String(operator || "").trim();
  const cfg = nodeConfig && typeof nodeConfig === "object" ? nodeConfig : {};
  const fallbackInput = String(firstInputFile || "").trim();
  if (op === "transform_rows_v2" || op === "transform_rows_v3" || op === "load_rows_v3") {
    const input = {};
    if (Array.isArray(cfg.rows) && cfg.rows.length) input.rows = cfg.rows;
    else if (cfg.input_uri) input.input_uri = cfg.input_uri;
    else if (fallbackInput) input.input_uri = fallbackInput;
    else input.rows = [];
    return input;
  }
  if (op === "finance_ratio_v1") return { rows: Array.isArray(cfg.rows) ? cfg.rows : [] };
  if (op === "anomaly_explain_v1") {
    return {
      rows: Array.isArray(cfg.rows) ? cfg.rows : [],
      score_field: String(cfg.score_field || "").trim(),
    };
  }
  if (op === "stream_window_v2") {
    return {
      stream_key: String(cfg.stream_key || "").trim(),
      event_time_field: String(cfg.event_time_field || "").trim(),
    };
  }
  if (op === "plugin_operator_v1") return { plugin: String(cfg.plugin || "").trim() };
  return cfg;
}

async function postRustOperator(endpoint, operatorPath, payload, fetchImpl = fetch) {
  const resp = await fetchImpl(`${endpoint}${operatorPath}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  if (!resp.ok) {
    return { ok: false, status: resp.status, error: `HTTP ${resp.status}` };
  }
  const body = await resp.json();
  return { ok: true, body };
}

export {
  IO_CONTRACT_COMPATIBLE_OPERATORS,
  buildIoContractInput,
  postRustOperator,
};

function createAiGuardrailsHelpers({ collectFiles }) {
  function extractNumericTokens(text) {
    const src = String(text || "");
    const out = [];
    const rx = /[-+]?\d+(?:[.,]\d+)?%?/g;
    let m = null;
    while ((m = rx.exec(src))) {
      const raw = String(m[0] || "");
      const norm = raw.replace(/,/g, "");
      if (!norm) continue;
      out.push(norm);
    }
    return out;
  }

  function hasCitationMarkers(text) {
    const s = String(text || "");
    if (!s.trim()) return false;
    if (/\[[0-9]{1,3}\]/.test(s)) return true;
    if (/(\u6765\u6e90|\u51fa\u5904|source|reference)\s*[:\uFF1A]/i.test(s)) return true;
    if (/https?:\/\/\S+/i.test(s)) return true;
    if (/[\(\uFF08][^()\uFF08\uFF09]{0,28}(\u6765\u6e90|\u51fa\u5904|source|reference)[^()\uFF08\uFF09]{0,28}[\)\uFF09]/i.test(s)) return true;
    return false;
  }

  function compareMetricCore(base = {}, now = {}) {
    const keys = ["sections", "bullets", "chars", "cjk", "latin"];
    const diffs = [];
    for (const k of keys) {
      const a = Number(base?.[k] || 0);
      const b = Number(now?.[k] || 0);
      const d = Math.abs(a - b);
      if (d > 0) diffs.push({ key: k, base: a, now: b, delta: d });
    }
    return diffs;
  }

  function looksLikeDataFile(p) {
    const s = String(p || "").trim().toLowerCase();
    if (!s) return false;
    return /\.(csv|tsv|xlsx|xls|json|jsonl|parquet|feather|orc|db|sqlite|sql)$/i.test(s);
  }

  function hasRowsLikeOutput(nodeOutputs) {
    if (!nodeOutputs || typeof nodeOutputs !== "object") return false;
    const values = Array.isArray(nodeOutputs) ? nodeOutputs : Object.values(nodeOutputs);
    for (const v of values) {
      if (!v || typeof v !== "object") continue;
      if (Array.isArray(v.rows) && v.rows.length > 0) return true;
      if (v.detail && typeof v.detail === "object" && Array.isArray(v.detail.rows) && v.detail.rows.length > 0) return true;
      if (Array.isArray(v.left_rows) && v.left_rows.length > 0) return true;
      if (Array.isArray(v.right_rows) && v.right_rows.length > 0) return true;
    }
    return false;
  }

  function shouldBlockAiOnData(ctx, node) {
    const payloadAi = ctx?.payload?.ai && typeof ctx.payload.ai === "object" ? ctx.payload.ai : {};
    const cfg = node?.config && typeof node.config === "object" ? node.config : {};
    const allowOnData = cfg.allow_ai_on_data === true || payloadAi.allow_on_data === true;
    if (allowOnData) return { block: false, reason: "" };
    const strict = payloadAi.no_hallucination_data !== false;
    if (!strict) return { block: false, reason: "" };
    const filesFromCtx = Array.isArray(ctx?.files) ? ctx.files : [];
    const filesFromPayload = collectFiles(ctx?.payload || {});
    const fileList = Array.from(new Set([...filesFromCtx, ...filesFromPayload]));
    const dataFileHit = fileList.find((f) => looksLikeDataFile(f));
    const dataRowsHit = hasRowsLikeOutput(ctx?.nodeOutputs);
    if (dataFileHit || dataRowsHit) {
      return {
        block: true,
        reason: dataFileHit ? `data_file_detected:${String(dataFileHit)}` : "rows_detected_in_upstream_outputs",
      };
    }
    return { block: false, reason: "" };
  }

  function estimateTextTokens(s) {
    const chars = String(s || "").length;
    return Math.max(1, Math.ceil(chars / 4));
  }

  function enforceAiBudgetBeforeCall(ctx, corpusText) {
    const st = ctx?.aiBudget && typeof ctx.aiBudget === "object" ? ctx.aiBudget : null;
    if (!st || st.enabled === false) return;
    if (Number(st.calls || 0) >= Number(st.max_calls_per_run || 2)) {
      throw new Error(`ai_budget_exceeded:calls>${st.max_calls_per_run}`);
    }
    const projected = Number(st.estimated_tokens || 0) + estimateTextTokens(corpusText);
    if (projected > Number(st.max_estimated_tokens_per_run || 120000)) {
      throw new Error(`ai_budget_exceeded:tokens>${st.max_estimated_tokens_per_run}`);
    }
  }

  function recordAiBudgetAfterCall(ctx, corpusText, aiText) {
    const st = ctx?.aiBudget && typeof ctx.aiBudget === "object" ? ctx.aiBudget : null;
    if (!st || st.enabled === false) return;
    st.calls = Number(st.calls || 0) + 1;
    const inTokens = estimateTextTokens(corpusText);
    const outTokens = estimateTextTokens(aiText);
    st.estimated_tokens = Number(st.estimated_tokens || 0) + inTokens + outTokens;
    const unit = Number(st.token_price_usd_per_1k || 0.002);
    st.estimated_cost_usd = Number((((Number(st.estimated_tokens || 0) / 1000) * unit).toFixed(6)));
    if (Number(st.estimated_cost_usd || 0) > Number(st.max_estimated_cost_usd_per_run || 0.8)) {
      throw new Error(`ai_budget_exceeded:cost>${st.max_estimated_cost_usd_per_run}`);
    }
  }

  return {
    shouldBlockAiOnData,
    enforceAiBudgetBeforeCall,
    recordAiBudgetAfterCall,
    extractNumericTokens,
    hasCitationMarkers,
    compareMetricCore,
  };
}

module.exports = {
  createAiGuardrailsHelpers,
};



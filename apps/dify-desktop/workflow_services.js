const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { nowIso, sha256Text, summarizeCorpus } = require("./workflow_utils");

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function isTruthy(v) {
  const s = String(v || "").trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}

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

function canUseNetworkEgress() {
  return isTruthy(process.env.AIWF_ALLOW_EGRESS) || isTruthy(process.env.AIWF_ALLOW_CLOUD_LLM);
}

async function checkRustHealth(baseUrl) {
  const url = `${String(baseUrl || "").replace(/\/$/, "")}/health`;
  const resp = await fetch(url, { method: "GET" });
  if (!resp.ok) return false;
  const j = await resp.json().catch(() => ({}));
  return !!j.ok;
}

function rustExeCandidates() {
  const local = path.resolve(__dirname, "..", "accel-rust", "target");
  return [
    path.join(local, "debug", "accel-rust.exe"),
    path.join(local, "release", "accel-rust.exe"),
  ];
}

function tryStartRustService() {
  for (const exe of rustExeCandidates()) {
    if (!fs.existsSync(exe)) continue;
    try {
      const child = spawn(exe, [], { windowsHide: true, detached: true, stdio: "ignore" });
      child.unref();
      return { ok: true, path: exe };
    } catch {}
  }
  return { ok: false, path: "" };
}

async function computeTextQualityViaRust(corpusText, options = {}) {
  const base = String(options.rust_endpoint || "http://127.0.0.1:18082").replace(/\/$/, "");
  if (!isLocalEndpoint(base) && !canUseNetworkEgress()) {
    return { score: 0, mode: "egress_blocked", fallback: true };
  }
  try {
    const resp = await fetch(`${base}/operators/text_quality_v1`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text: String(corpusText || "") }),
    });
    if (!resp.ok) throw new Error(`rust_http_${resp.status}`);
    const j = await resp.json();
    const out = j?.result || j?.quality || j;
    if (!out || typeof out !== "object") throw new Error("invalid_text_quality");
    const score = Number(out.score || out.quality_score || 0);
    return {
      ...out,
      score: Number.isFinite(score) ? score : 0,
      mode: "rust_http",
      fallback: false,
    };
  } catch {
    const text = String(corpusText || "");
    const cjk = (text.match(/[\u4e00-\u9fff]/g) || []).length;
    const latin = (text.match(/[A-Za-z]/g) || []).length;
    const repl = (text.match(/\uFFFD|�/g) || []).length;
    const readable = cjk + latin;
    const total = Math.max(1, text.length);
    let score = 100;
    score -= Math.min(50, (repl / total) * 400);
    score -= readable < 80 ? 20 : 0;
    return {
      score: Math.max(0, Number(score.toFixed(2))),
      mode: "js_fallback",
      fallback: true,
      readable_chars: readable,
      replacement_chars: repl,
    };
  }
}

async function computeViaRust(corpusText, options = {}) {
  const base = String(options.rust_endpoint || "http://127.0.0.1:18082").replace(/\/$/, "");
  const strict = options.rust_required !== false;
  if (!isLocalEndpoint(base) && !canUseNetworkEgress()) {
    return { metrics: summarizeCorpus(corpusText), mode: "js_fallback_egress_blocked", started: false };
  }
  const tryCall = async () => {
    const resp = await fetch(`${base}/operators/compute_metrics`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_id: options.run_id || "", text: String(corpusText || "") }),
    });
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(`rust_http_${resp.status}:${t.slice(0, 240)}`);
    }
    const j = await resp.json();
    if (!j?.ok || !j?.metrics) throw new Error("rust_invalid_response");
    const textQuality = await computeTextQualityViaRust(corpusText, { rust_endpoint: base, rust_required: false });
    return { ...j.metrics, text_quality: textQuality };
  };

  try {
    return { metrics: await tryCall(), mode: "rust_http", started: false };
  } catch (e1) {
    const started = tryStartRustService();
    if (started.ok) {
      for (let i = 0; i < 12; i += 1) {
        try {
          const healthy = await checkRustHealth(base);
          if (healthy) break;
        } catch {}
        await sleep(500);
      }
      try {
        return { metrics: await tryCall(), mode: "rust_http_started", started: true, rust_path: started.path };
      } catch (e2) {
        if (strict) throw e2;
      }
    } else if (strict) {
      throw e1;
    }
  }

  return {
    metrics: {
      ...summarizeCorpus(corpusText),
      text_quality: await computeTextQualityViaRust(corpusText, { rust_endpoint: base, rust_required: false }),
    },
    mode: "js_fallback",
    started: false,
  };
}

function buildFallbackRefine(corpusText, metrics) {
  const lines = String(corpusText || "").split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const bullets = lines.filter((x) => x.startsWith("- ")).slice(0, 12).map((x) => x.replace(/^- /, ""));
  const out = [];
  out.push("以下为基于算法清洗结果生成的提炼摘要（未调用外部 AI）：");
  out.push(`- 语料分段数: ${metrics.sections}`);
  out.push(`- 要点条目数: ${metrics.bullets}`);
  out.push(`- 字符数: ${metrics.chars}`);
  if (metrics?.text_quality && typeof metrics.text_quality === "object") {
    out.push(`- 文本质量分: ${Number(metrics.text_quality.score || 0).toFixed(1)} (${String(metrics.text_quality.mode || "unknown")})`);
  }
  out.push("- 核心要点:");
  bullets.forEach((b, i) => out.push(`${i + 1}. ${b}`));
  return out.join("\n");
}

async function callExternalAi(payload, corpusText, metrics) {
  const ai = payload.ai || {};
  const endpoint = String(ai.endpoint || "").trim();
  const apiKey = String(ai.api_key || "").trim();
  const model = String(ai.model || "").trim();
  if (endpoint && !isLocalEndpoint(endpoint) && !canUseNetworkEgress()) {
    return { ok: false, reason: "ai_egress_blocked", text: buildFallbackRefine(corpusText, metrics) };
  }
  if (!endpoint || !apiKey || !model) {
    return { ok: false, reason: "ai_not_configured", text: buildFallbackRefine(corpusText, metrics) };
  }
  const prompt = [
    "你是严谨的信息提炼助手。",
    "只允许基于输入语料提炼，不得编造数据，不得新增数字结论。",
    `语料哈希: ${metrics.sha256}`,
    "",
    String(corpusText || "").slice(0, 120000),
  ].join("\n");
  try {
    const resp = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({
        model,
        temperature: Number.isFinite(Number(ai.temperature)) ? Number(ai.temperature) : 0.2,
        messages: [
          { role: "system", content: "你必须忠实于输入语料，不得捏造数据。" },
          { role: "user", content: prompt },
        ],
      }),
    });
    const bodyText = await resp.text();
    if (!resp.ok) return { ok: false, reason: `ai_http_${resp.status}`, text: buildFallbackRefine(corpusText, metrics), detail: bodyText.slice(0, 500) };
    const obj = JSON.parse(bodyText || "{}");
    const content = obj?.choices?.[0]?.message?.content;
    if (!content || !String(content).trim()) return { ok: false, reason: "ai_empty", text: buildFallbackRefine(corpusText, metrics) };
    return { ok: true, reason: "ai_ok", text: String(content) };
  } catch (e) {
    return { ok: false, reason: "ai_exception", text: buildFallbackRefine(corpusText, metrics), detail: String(e) };
  }
}

function auditAiText(aiText, metrics) {
  const t = String(aiText || "").trim();
  const reasons = [];
  if (!t) reasons.push("AI 输出为空");
  if (t.length < 80) reasons.push("AI 输出过短，信息不足");
  if (/捏造|编造|虚构|猜测/i.test(t)) reasons.push("AI 输出包含不合规表述");
  if (!t.includes("证据") && !t.includes("要点")) reasons.push("AI 输出缺少“证据/要点”结构");
  const tq = metrics?.text_quality;
  if (tq && Number.isFinite(Number(tq.score)) && Number(tq.score) < 50) {
    reasons.push(`输入语料质量偏低(${Number(tq.score).toFixed(1)})，建议先提升清洗质量`);
  }
  return {
    passed: reasons.length === 0,
    reasons,
    metrics_hash: metrics.sha256,
    ai_hash: sha256Text(t),
  };
}

function writeWorkflowSummary(summaryPath, context) {
  const lines = [];
  lines.push(`# Workflow 结果 - ${context.run_id}`);
  lines.push("");
  lines.push(`- 时间: ${nowIso()}`);
  lines.push(`- workflow_id: ${context.workflow_id}`);
  lines.push(`- clean_job_id: ${context.clean_job_id || ""}`);
  lines.push(`- 审核结果: ${context.audit?.passed ? "通过" : "未通过"}`);
  lines.push(`- 语料哈希: ${context.metrics?.sha256 || ""}`);
  lines.push("");
  lines.push("## 算法指标");
  lines.push(`- sections: ${context.metrics?.sections || 0}`);
  lines.push(`- bullets: ${context.metrics?.bullets || 0}`);
  lines.push(`- chars: ${context.metrics?.chars || 0}`);
  lines.push(`- cjk: ${context.metrics?.cjk || 0}`);
  lines.push(`- latin: ${context.metrics?.latin || 0}`);
  if (context.metrics?.text_quality) {
    lines.push(`- text_quality_score: ${Number(context.metrics.text_quality.score || 0).toFixed(1)}`);
    lines.push(`- text_quality_mode: ${String(context.metrics.text_quality.mode || "unknown")}`);
  }
  lines.push("");
  lines.push("## AI 审核");
  if (!context.audit?.reasons?.length) lines.push("- 无阻断问题。");
  else context.audit.reasons.forEach((r) => lines.push(`- ${r}`));
  lines.push("");
  lines.push("## AI 提炼文本");
  lines.push(context.ai_text || "");
  lines.push("");
  fs.writeFileSync(summaryPath, `\uFEFF${lines.join("\n")}\n`, "utf8");
}

module.exports = {
  computeViaRust,
  computeTextQualityViaRust,
  callExternalAi,
  auditAiText,
  writeWorkflowSummary,
};

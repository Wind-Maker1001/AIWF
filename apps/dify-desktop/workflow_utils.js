const crypto = require("crypto");

function nowIso() {
  return new Date().toISOString();
}

function sha256Text(s) {
  return crypto.createHash("sha256").update(String(s || ""), "utf8").digest("hex");
}

function collectFiles(payload = {}) {
  const p = payload.params || {};
  const out = [];
  const add = (v) => {
    if (Array.isArray(v)) {
      v.forEach(add);
      return;
    }
    if (v && typeof v === "object") {
      add(v.path || v.file || v.filePath || v.value || "");
      return;
    }
    const s = String(v || "").trim();
    if (!s) return;
    out.push(s);
  };
  add(p.input_path);
  if (typeof p.input_files === "string") {
    const raw = String(p.input_files || "").trim();
    if (raw.startsWith("[") && raw.endsWith("]")) {
      try {
        add(JSON.parse(raw));
      } catch {
        raw.split(/\r?\n/).forEach(add);
      }
    } else {
      raw.split(/\r?\n/).forEach(add);
    }
  } else {
    add(p.input_files);
  }
  return Array.from(new Set(out));
}

function readArtifactById(artifacts, id) {
  const it = (artifacts || []).find((a) => a.artifact_id === id);
  return it ? it.path : "";
}

function summarizeCorpus(corpusText) {
  const text = String(corpusText || "");
  const lines = text.split(/\r?\n/);
  return {
    sections: lines.filter((x) => x.startsWith("## ")).length,
    bullets: lines.filter((x) => x.trim().startsWith("- ")).length,
    chars: text.length,
    cjk: (text.match(/[\u4e00-\u9fff]/g) || []).length,
    latin: (text.match(/[A-Za-z]/g) || []).length,
    sha256: sha256Text(text),
  };
}

function makeNodeRun(node) {
  return {
    id: node.id,
    type: node.type,
    status: "pending",
    started_at: null,
    ended_at: null,
    seconds: 0,
    output: null,
    error: null,
  };
}

function finalizeNode(node, ok, output, error) {
  node.ended_at = nowIso();
  const start = Date.parse(node.started_at || nowIso());
  const end = Date.parse(node.ended_at || nowIso());
  node.seconds = Math.max(0, Math.round(((end - start) / 1000) * 1000) / 1000);
  node.status = ok ? "done" : "failed";
  if (output && typeof output === "object" && output.__runner && !Array.isArray(output)) {
    node.telemetry = output.__runner;
    const cloned = { ...output };
    delete cloned.__runner;
    node.output = cloned;
  } else {
    node.output = output || null;
    node.telemetry = null;
  }
  node.error = error ? String(error) : null;
}

function nodeOutputByType(ctx, type) {
  for (let i = ctx.orderedNodeRuns.length - 1; i >= 0; i -= 1) {
    const n = ctx.orderedNodeRuns[i];
    if (n.type === type && n.output) return n.output;
  }
  return null;
}

module.exports = {
  nowIso,
  sha256Text,
  collectFiles,
  readArtifactById,
  summarizeCorpus,
  makeNodeRun,
  finalizeNode,
  nodeOutputByType,
};

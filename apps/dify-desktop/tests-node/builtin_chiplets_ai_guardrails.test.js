const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerBuiltinWorkflowChiplets } = require("../workflow_chiplets/builtin_chiplets");
const { runChipletNode } = require("../workflow_chiplets/runner");
const { buildEnvelope } = require("../workflow_chiplets/contract");
const { collectFiles: realCollectFiles } = require("../workflow_utils");

function makeDeps(overrides = {}) {
  return {
    fs,
    path,
    runOfflineCleaning: async () => ({ job_id: "j1", artifacts: [], warnings: [] }),
    collectFiles: realCollectFiles,
    readArtifactById: () => "",
    summarizeCorpus: (text) => ({
      sections: 1,
      bullets: 1,
      chars: String(text || "").length,
      cjk: 0,
      latin: 0,
      sha256: "metrics_sha",
    }),
    computeViaRust: async () => ({
      mode: "rust_http",
      started: false,
      metrics: { sections: 1, bullets: 1, chars: 10, cjk: 0, latin: 10, sha256: "metrics_sha" },
    }),
    callExternalAi: async () => ({ reason: "ai_ok", text: "证据 [1] 数值 10" }),
    auditAiText: () => ({ passed: true, reasons: [], metrics_hash: "metrics_sha", ai_hash: "ai_sha" }),
    writeWorkflowSummary: () => {},
    sha256Text: () => "sha",
    nodeOutputByType: () => null,
    runIsolatedTask: undefined,
    ...overrides,
  };
}

function makeCtx(payload = {}) {
  return {
    runId: "r1",
    workflowId: "w1",
    payload,
    corpusText: "原文包含数值 10。",
    metrics: { sections: 1, bullets: 1, chars: 10, cjk: 0, latin: 10, sha256: "metrics_sha" },
    aiText: "",
  };
}

async function runNode(registry, ctx, node) {
  return runChipletNode({
    registry,
    node,
    ctx,
    envelope: buildEnvelope({
      run_id: ctx.runId,
      workflow_id: ctx.workflowId,
      node_id: node.id,
      node_type: node.type,
    }),
  });
}

test("ai_refine merges node-level DS config into AI payload", async () => {
  let seenPayload = null;
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(
    registry,
    makeDeps({
      callExternalAi: async (payload) => {
        seenPayload = payload;
        return { reason: "ai_ok", text: "证据 [1] 数值 10" };
      },
    }),
  );
  const ctx = makeCtx({
    ai: {
      endpoint: "https://old-endpoint",
      api_key: "old-key",
      model: "old-model",
      name: "old-provider",
    },
  });
  const out = await runNode(registry, ctx, {
    id: "n1",
    type: "ai_refine",
    config: {
      reuse_existing: false,
      provider_name: "DeepSeek",
      ai_endpoint: "https://api.deepseek.com/v1/chat/completions",
      ai_api_key: "ds-key",
      ai_model: "deepseek-chat",
    },
  });
  assert.equal(out.ai_mode, "ai_ok");
  assert.ok(seenPayload && seenPayload.ai);
  assert.equal(seenPayload.ai.name, "DeepSeek");
  assert.equal(seenPayload.ai.endpoint, "https://api.deepseek.com/v1/chat/completions");
  assert.equal(seenPayload.ai.api_key, "ds-key");
  assert.equal(seenPayload.ai.model, "deepseek-chat");
});

test("ai_refine blocks AI when data raw files are detected by default", async () => {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(registry, makeDeps());
  const ctx = makeCtx({ params: { input_files: ["D:/data/finance.xlsx"] } });
  await assert.rejects(
    runNode(registry, ctx, { id: "n1b", type: "ai_refine", config: { reuse_existing: false } }),
    /ai_for_data_blocked/,
  );
});

test("ai_refine allows explicit override for data workflows", async () => {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(registry, makeDeps());
  const ctx = makeCtx({
    params: { input_files: ["D:/data/finance.xlsx"] },
    ai: { allow_on_data: true },
  });
  const out = await runNode(registry, ctx, {
    id: "n1c",
    type: "ai_refine",
    config: { reuse_existing: false, allow_ai_on_data: true },
  });
  assert.equal(out.ai_mode, "ai_ok");
});

test("ai_audit blocks output when numeric lock detects new numbers", async () => {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(registry, makeDeps());
  const ctx = makeCtx();
  ctx.aiText = "结论里出现新数字 999 [1]";
  await assert.rejects(
    runNode(registry, ctx, { id: "n2", type: "ai_audit", config: { numeric_lock: true, max_new_numbers: 0 } }),
    /numeric_lock_failed/,
  );
});

test("ai_audit blocks output when citations are required but absent", async () => {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(registry, makeDeps());
  const ctx = makeCtx();
  ctx.aiText = "这是纯文本，没有任何引用标记";
  await assert.rejects(
    runNode(registry, ctx, { id: "n3", type: "ai_audit", config: { citation_required: true } }),
    /citation_required_failed/,
  );
});

test("ai_audit blocks output when recalc verification mismatches metrics", async () => {
  const registry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowChiplets(
    registry,
    makeDeps({
      computeViaRust: async () => ({
        mode: "rust_http",
        started: false,
        metrics: { sections: 9, bullets: 9, chars: 999, cjk: 0, latin: 999, sha256: "other_sha" },
      }),
    }),
  );
  const ctx = makeCtx();
  ctx.aiText = "证据 [1] 数值 10";
  await assert.rejects(
    runNode(registry, ctx, { id: "n4", type: "ai_audit", config: { recalc_verify: true, max_metric_delta: 0 } }),
    /recalc_verify_failed/,
  );
});

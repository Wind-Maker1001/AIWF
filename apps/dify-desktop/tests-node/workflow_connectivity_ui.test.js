const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadConnectivityUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/connectivity-ui.js")).href;
  return import(file);
}

test("workflow connectivity ui shows local-only boundary hint", async () => {
  const { createWorkflowConnectivityUi } = await loadConnectivityUiModule();
  const els = {
    offlineBoundaryHint: { textContent: "" },
    aiEndpoint: { value: "" },
  };
  const ui = createWorkflowConnectivityUi(els, {
    exportGraph: () => ({
      nodes: [
        { id: "n1", type: "clean_md" },
        { id: "n2", type: "quality_check_v3" },
      ],
    }),
  });

  ui.refreshOfflineBoundaryHint();

  assert.equal(els.offlineBoundaryHint.textContent, "离线能力边界：当前流程全部为本地可执行节点（离线可跑）。");
});

test("workflow connectivity ui reports online and unknown boundary nodes", async () => {
  const { createWorkflowConnectivityUi } = await loadConnectivityUiModule();
  const els = {
    offlineBoundaryHint: { textContent: "" },
    aiEndpoint: { value: "" },
  };
  const ui = createWorkflowConnectivityUi(els, {
    exportGraph: () => ({
      nodes: [
        { id: "n1", type: "ai_refine" },
        { id: "n2", type: "custom_external" },
      ],
    }),
  });

  ui.refreshOfflineBoundaryHint();

  assert.equal(
    els.offlineBoundaryHint.textContent,
    "离线能力边界：检测到在线节点: ai_refine。 未配置外部 AI Endpoint，这些节点离线不可执行。 检测到未知边界节点: custom_external（请确认是否需要外部服务）。"
  );
});

test("workflow connectivity ui applies deepseek defaults and refreshes hint", async () => {
  const { createWorkflowConnectivityUi } = await loadConnectivityUiModule();
  const statuses = [];
  const els = {
    offlineBoundaryHint: { textContent: "" },
    aiEndpoint: { value: "" },
    aiModel: { value: "" },
  };
  const ui = createWorkflowConnectivityUi(els, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    exportGraph: () => ({
      nodes: [{ id: "n1", type: "ai_refine" }],
    }),
  });

  ui.applyDeepSeekDefaults();

  assert.equal(els.aiEndpoint.value, "https://api.deepseek.com/v1/chat/completions");
  assert.equal(els.aiModel.value, "deepseek-chat");
  assert.equal(
    els.offlineBoundaryHint.textContent,
    "离线能力边界：检测到在线节点: ai_refine。 已配置外部 AI Endpoint，可在线执行。"
  );
  assert.deepEqual(statuses, [{ text: "已填充 DeepSeek 接口参数（请确认 API Key）", ok: true }]);
});

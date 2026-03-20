import {
  buildOfflineBoundaryHint,
  deepSeekDefaults,
} from "./connectivity-support.js";

function createWorkflowConnectivityUi(els, deps = {}) {
  const {
    setStatus = () => {},
    exportGraph = () => ({}),
    deepSeekEndpoint = "https://api.deepseek.com/v1/chat/completions",
    deepSeekModel = "deepseek-chat",
  } = deps;

  function refreshOfflineBoundaryHint() {
    if (!els.offlineBoundaryHint) return;
    els.offlineBoundaryHint.textContent = buildOfflineBoundaryHint(
      exportGraph(),
      String(els.aiEndpoint?.value || "").trim()
    );
  }

  function applyDeepSeekDefaults() {
    const defaults = deepSeekDefaults(deepSeekEndpoint, deepSeekModel);
    if (els.aiEndpoint) els.aiEndpoint.value = defaults.endpoint;
    if (els.aiModel) els.aiModel.value = defaults.model;
    refreshOfflineBoundaryHint();
    setStatus("已填充 DeepSeek 接口参数（请确认 API Key）", true);
  }

  return {
    refreshOfflineBoundaryHint,
    applyDeepSeekDefaults,
  };
}

export { createWorkflowConnectivityUi };

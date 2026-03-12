function createWorkflowConnectivityUi(els, deps = {}) {
  const {
    setStatus = () => {},
    exportGraph = () => ({}),
    deepSeekEndpoint = "https://api.deepseek.com/v1/chat/completions",
    deepSeekModel = "deepseek-chat",
  } = deps;

  const offlineLocalNodeTypes = new Set([
    "ingest_files",
    "clean_md",
    "compute_rust",
    "transform_rows_v3",
    "join_rows_v2",
    "join_rows_v3",
    "join_rows_v4",
    "aggregate_rows_v2",
    "aggregate_rows_v3",
    "aggregate_rows_v4",
    "quality_check_v2",
    "quality_check_v3",
    "quality_check_v4",
    "window_rows_v1",
    "optimizer_v1",
    "load_rows_v2",
    "load_rows_v3",
    "schema_registry_v1_infer",
    "schema_registry_v1_get",
    "schema_registry_v1_register",
    "schema_registry_v2_check_compat",
    "schema_registry_v2_suggest_migration",
    "query_lang_v1",
    "columnar_eval_v1",
    "stream_window_v1",
    "stream_window_v2",
    "sketch_v1",
    "runtime_stats_v1",
    "capabilities_v1",
    "io_contract_v1",
    "failure_policy_v1",
    "incremental_plan_v1",
    "tenant_isolation_v1",
    "operator_policy_v1",
    "optimizer_adaptive_v2",
    "vector_index_v2_build",
    "vector_index_v2_search",
    "vector_index_v2_eval",
    "stream_reliability_v1",
    "lineage_provenance_v1",
    "contract_regression_v1",
    "perf_baseline_v1",
    "md_output",
    "manual_review",
    "sql_chart_v1",
    "office_slot_fill_v1",
  ]);

  const onlineRequiredNodeTypes = new Set([
    "ai_refine",
    "ai_audit",
    "ai_strategy_v1",
    "ai_call",
  ]);

  function refreshOfflineBoundaryHint() {
    if (!els.offlineBoundaryHint) return;
    const graph = exportGraph();
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
    const onlineNodes = nodes.filter((n) => onlineRequiredNodeTypes.has(String(n?.type || "")));
    const unknownNodes = nodes.filter((n) => {
      const t = String(n?.type || "");
      return !offlineLocalNodeTypes.has(t) && !onlineRequiredNodeTypes.has(t);
    });
    if (onlineNodes.length === 0 && unknownNodes.length === 0) {
      els.offlineBoundaryHint.textContent = "离线能力边界：当前流程全部为本地可执行节点（离线可跑）。";
      return;
    }
    const aiEndpoint = String(els.aiEndpoint?.value || "").trim();
    const lines = [];
    if (onlineNodes.length > 0) {
      const types = Array.from(new Set(onlineNodes.map((n) => String(n.type || "")))).join(", ");
      lines.push(`检测到在线节点: ${types}。`);
      lines.push(aiEndpoint ? "已配置外部 AI Endpoint，可在线执行。" : "未配置外部 AI Endpoint，这些节点离线不可执行。");
    }
    if (unknownNodes.length > 0) {
      const types = Array.from(new Set(unknownNodes.map((n) => String(n.type || "")))).join(", ");
      lines.push(`检测到未知边界节点: ${types}（请确认是否需要外部服务）。`);
    }
    els.offlineBoundaryHint.textContent = `离线能力边界：${lines.join(" ")}`;
  }

  function applyDeepSeekDefaults() {
    if (els.aiEndpoint) els.aiEndpoint.value = deepSeekEndpoint;
    if (els.aiModel) els.aiModel.value = deepSeekModel;
    refreshOfflineBoundaryHint();
    setStatus("已填充 DeepSeek 接口参数（请确认 API Key）", true);
  }

  return {
    refreshOfflineBoundaryHint,
    applyDeepSeekDefaults,
  };
}

export { createWorkflowConnectivityUi };

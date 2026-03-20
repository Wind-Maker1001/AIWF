import { NODE_CATALOG } from "./defaults-catalog.js";
import { NODE_CONFIG_TEMPLATES } from "./defaults-templates.js";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function defaultNodeConfig(type) {
  const normalizedType = String(type || "");
  const template = NODE_CONFIG_TEMPLATES[normalizedType];
  return template && typeof template === "object" ? clone(template) : {};
}

function defaultWorkflowGraph() {
  return {
    workflow_id: "standard_pipeline_v1",
    name: "标准数据流水线 v1",
    nodes: [
      { id: "n1", type: "ingest_files", x: 40, y: 80, config: defaultNodeConfig("ingest_files") },
      { id: "n2", type: "clean_md", x: 300, y: 80, config: defaultNodeConfig("clean_md") },
      { id: "n3", type: "load_rows_v3", x: 560, y: 80, config: defaultNodeConfig("load_rows_v3") },
      { id: "n4", type: "columnar_eval_v1", x: 820, y: 80, config: defaultNodeConfig("columnar_eval_v1") },
      { id: "n5", type: "stream_window_v1", x: 1080, y: 80, config: defaultNodeConfig("stream_window_v1") },
      { id: "n6", type: "sketch_v1", x: 1340, y: 80, config: defaultNodeConfig("sketch_v1") },
      { id: "n7", type: "explain_plan_v1", x: 1600, y: 80, config: defaultNodeConfig("explain_plan_v1") },
      { id: "n8", type: "runtime_stats_v1", x: 1860, y: 80, config: defaultNodeConfig("runtime_stats_v1") },
      { id: "n9", type: "md_output", x: 2120, y: 80, config: defaultNodeConfig("md_output") },
    ],
    edges: [
      { from: "n1", to: "n2" },
      { from: "n2", to: "n3" },
      { from: "n3", to: "n4" },
      { from: "n4", to: "n5" },
      { from: "n5", to: "n6" },
      { from: "n6", to: "n7" },
      { from: "n7", to: "n8" },
      { from: "n8", to: "n9" },
    ],
  };
}

export {
  NODE_CATALOG,
  NODE_CONFIG_TEMPLATES,
  defaultNodeConfig,
  defaultWorkflowGraph,
};

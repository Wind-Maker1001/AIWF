const { DESKTOP_RUST_OPERATOR_TYPES } = require("./workflow_chiplets/domains/rust_operator_manifest.generated");

const LOCAL_WORKFLOW_NODE_TYPES = Object.freeze([
  "ingest_files",
  "clean_md",
  "compute_rust",
  "manual_review",
  "sql_chart_v1",
  "office_slot_fill_v1",
  "ai_strategy_v1",
  "ds_refine",
  "ai_refine",
  "ai_audit",
  "md_output",
]);

const REGISTERED_WORKFLOW_NODE_TYPES = Object.freeze(
  Array.from(new Set([
    ...LOCAL_WORKFLOW_NODE_TYPES,
    ...DESKTOP_RUST_OPERATOR_TYPES,
  ])).sort(),
);

function findUnknownWorkflowNodeTypes(graph = {}) {
  const known = new Set(REGISTERED_WORKFLOW_NODE_TYPES);
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  return Array.from(new Set(
    nodes
      .map((node) => String(node?.type || "").trim())
      .filter((type) => type && !known.has(type)),
  )).sort();
}

module.exports = {
  LOCAL_WORKFLOW_NODE_TYPES,
  REGISTERED_WORKFLOW_NODE_TYPES,
  findUnknownWorkflowNodeTypes,
};

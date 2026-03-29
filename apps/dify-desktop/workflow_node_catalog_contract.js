const { DESKTOP_RUST_OPERATOR_TYPES } = require("./workflow_chiplets/domains/rust_operator_manifest.generated");
const { LOCAL_NODE_TYPES } = require("./renderer/workflow/local-node-palette-policy.js");

const LOCAL_WORKFLOW_NODE_TYPES = Object.freeze(
  Array.from(new Set(
    (Array.isArray(LOCAL_NODE_TYPES) ? LOCAL_NODE_TYPES : [])
      .map((type) => String(type || "").trim())
      .filter(Boolean),
  )).sort(),
);

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

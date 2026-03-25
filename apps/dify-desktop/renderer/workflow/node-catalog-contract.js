import { NODE_CATALOG } from "./defaults-catalog.js";

const REGISTERED_WORKFLOW_NODE_TYPES = Object.freeze(
  Array.from(
    new Set(
      (Array.isArray(NODE_CATALOG) ? NODE_CATALOG : [])
        .map((item) => String(item?.type || "").trim())
        .filter(Boolean),
    ),
  ).sort(),
);

function findUnknownWorkflowNodeTypes(graph) {
  const known = new Set(REGISTERED_WORKFLOW_NODE_TYPES);
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  return Array.from(
    new Set(
      nodes
        .map((node) => String(node?.type || "").trim())
        .filter((type) => type && !known.has(type)),
    ),
  ).sort();
}

function createUnknownWorkflowNodeTypeError(types, stage = "workflow") {
  const unknown = Array.isArray(types) ? types.map((type) => String(type || "").trim()).filter(Boolean) : [];
  const error = new Error(`workflow contains unregistered node types in ${stage}: ${unknown.join(", ")}`);
  error.name = "WorkflowNodeCatalogError";
  error.code = "workflow_node_type_unregistered";
  error.details = {
    stage,
    unknown_node_types: unknown,
  };
  return error;
}

function isRegisteredWorkflowNodeType(type) {
  const normalized = String(type || "").trim();
  return REGISTERED_WORKFLOW_NODE_TYPES.includes(normalized);
}

function assertRegisteredWorkflowNodeType(type, options = {}) {
  const normalized = String(type || "").trim();
  if (isRegisteredWorkflowNodeType(normalized)) {
    return normalized;
  }
  throw createUnknownWorkflowNodeTypeError([normalized], String(options?.stage || "workflow").trim() || "workflow");
}

function assertRegisteredWorkflowNodeTypes(graph, options = {}) {
  const stage = String(options?.stage || "workflow").trim() || "workflow";
  const unknown = findUnknownWorkflowNodeTypes(graph);
  if (unknown.length > 0) {
    throw createUnknownWorkflowNodeTypeError(unknown, stage);
  }
  return true;
}

export {
  REGISTERED_WORKFLOW_NODE_TYPES,
  findUnknownWorkflowNodeTypes,
  createUnknownWorkflowNodeTypeError,
  isRegisteredWorkflowNodeType,
  assertRegisteredWorkflowNodeType,
  assertRegisteredWorkflowNodeTypes,
};

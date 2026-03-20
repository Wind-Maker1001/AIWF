function stringifyWorkflowGraph(graph) {
  return JSON.stringify(graph || {}, null, 2);
}

function saveWorkflowName(value) {
  return String(value || "").trim() || "workflow";
}

function loadWorkflowStatusMessage(path, migrated) {
  if (migrated?.migrated) {
    return `流程已加载并迁移: ${path} (${(migrated.notes || []).join(", ")})`;
  }
  return `流程已加载: ${path}`;
}

export {
  loadWorkflowStatusMessage,
  saveWorkflowName,
  stringifyWorkflowGraph,
};

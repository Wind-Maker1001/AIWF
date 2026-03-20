function workflowStatusClassName(ok = true) {
  return `status ${ok ? "ok" : "bad"}`;
}

function applyWorkflowStatus(els, text, ok = true) {
  if (!els?.status) return;
  els.status.className = workflowStatusClassName(ok);
  els.status.textContent = text;
}

export {
  applyWorkflowStatus,
  workflowStatusClassName,
};

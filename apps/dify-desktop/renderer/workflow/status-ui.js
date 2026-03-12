function createWorkflowStatusUi(els) {
  function setStatus(text, ok = true) {
    if (!els.status) return;
    els.status.className = `status ${ok ? "ok" : "bad"}`;
    els.status.textContent = text;
  }

  return { setStatus };
}

export { createWorkflowStatusUi };

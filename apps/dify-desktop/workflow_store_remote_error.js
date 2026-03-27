function createWorkflowStoreRemoteError(payload, fallbackMessage = "workflow store remote request failed") {
  const safePayload = payload && typeof payload === "object" ? { ...payload } : {};
  const error = new Error(String(safePayload.error || fallbackMessage));
  error.name = "WorkflowStoreRemoteError";
  error.remote_payload = safePayload;
  return error;
}

function workflowStoreRemoteErrorResult(error) {
  const payload = error && typeof error === "object" && error.remote_payload && typeof error.remote_payload === "object"
    ? error.remote_payload
    : (
      error && typeof error === "object" && (
        error.ok === false
        || typeof error.error_code === "string"
        || Array.isArray(error.error_items)
      )
        ? error
        : null
    );
  if (payload) {
    return {
      ok: false,
      ...payload,
      error: String(payload.error || error?.message || error || "workflow store remote request failed"),
    };
  }
  return { ok: false, error: String(error) };
}

module.exports = {
  createWorkflowStoreRemoteError,
  workflowStoreRemoteErrorResult,
};

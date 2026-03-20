function runHistoryRequestPayload(limit = 80) {
  return { limit };
}

function queueRequestPayload(limit = 120) {
  return { limit };
}

function queueControlStatusText(paused, error) {
  if (paused === true) return "队列已暂停";
  if (paused === false) return error ? `恢复失败: ${error}` : "队列已恢复";
  return "";
}

function normalizeQueueItems(out) {
  return Array.isArray(out?.items) ? out.items : [];
}

function normalizeQueueControl(out) {
  return out?.control && typeof out.control === "object" ? out.control : {};
}

export {
  normalizeQueueControl,
  normalizeQueueItems,
  queueControlStatusText,
  queueRequestPayload,
  runHistoryRequestPayload,
};

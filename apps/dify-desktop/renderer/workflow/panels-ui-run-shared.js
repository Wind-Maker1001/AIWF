function statusColor(status) {
  const s = String(status || "").trim().toLowerCase();
  if (!s) return "";
  if (s === "done" || s === "passed" || s === "approved") return "#087443";
  if (s === "failed" || s === "forbidden_graph" || s === "invalid_graph" || s === "rejected") return "#b42318";
  if (s === "quality_blocked" || s === "pending_review" || s === "blocked") return "#b54708";
  if (s === "canceled" || s === "cancelled" || s === "queued") return "#5c6b7a";
  if (s === "running") return "#1d4ed8";
  return "";
}

export { statusColor };

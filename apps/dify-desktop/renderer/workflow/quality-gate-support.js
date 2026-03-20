function normalizeQualityGateExportFormat(value) {
  return String(value || "md").trim().toLowerCase() === "json" ? "json" : "md";
}

function normalizeQualityGateStatus(value) {
  const status = String(value || "all").trim().toLowerCase();
  return status === "blocked" || status === "pass" ? status : "all";
}

function parseQualityGatePrefs(raw) {
  if (!raw) return null;
  const obj = JSON.parse(raw);
  const filter = obj?.filter && typeof obj.filter === "object" ? obj.filter : {};
  return {
    filter: {
      run_id: String(filter.run_id || ""),
      status: normalizeQualityGateStatus(filter.status || "all"),
    },
    format: normalizeQualityGateExportFormat(obj?.format || "md"),
  };
}

export {
  normalizeQualityGateExportFormat,
  normalizeQualityGateStatus,
  parseQualityGatePrefs,
};

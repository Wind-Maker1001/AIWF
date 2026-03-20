function exportFormatFromValue(value) {
  return String(value || "md").trim().toLowerCase() === "json" ? "json" : "md";
}

function findTemplateById(templates = [], templateId = "") {
  const id = String(templateId || "").trim();
  return Array.isArray(templates)
    ? templates.find((item) => String(item?.id || "") === id) || null
    : null;
}

function buildTemplateAcceptanceReport(ctx = {}) {
  const {
    templateId = "",
    templateName = "",
    accepted = false,
    governance = {},
    before = null,
    autoFix = null,
    after = null,
    now = () => new Date().toISOString(),
  } = ctx;

  return {
    ts: now(),
    template_id: String(templateId || ""),
    template_name: String(templateName || ""),
    accepted: !!accepted,
    governance,
    before,
    auto_fix: autoFix,
    after,
  };
}

export {
  buildTemplateAcceptanceReport,
  exportFormatFromValue,
  findTemplateById,
};

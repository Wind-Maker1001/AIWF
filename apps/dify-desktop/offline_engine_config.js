const fs = require("fs");
const path = require("path");
const {
  normalizeCleaningTemplateRegistry,
  normalizeOfficeLayoutCatalog,
  normalizeOfficeThemeCatalog,
} = require("./offline_template_catalog_contract");
const {
  applyCleaningSpecToParams,
  normalizeCleaningTemplatePayload,
} = require("./offline_cleaning_spec");

function loadDesktopThemes() {
  const defaults = {
    fluent_ms: { title: "Fluent 宣传风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    fluent_ms_light: { title: "Fluent Light", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    fluent_ms_strong: { title: "Fluent Strong", primary: "005FB8", secondary: "083B7A", bg: "EEF5FD" },
    fluent_ms_vibrant: { title: "Fluent Vibrant", primary: "0A66C2", secondary: "0078D4", bg: "EDF5FF" },
    professional: { title: "专业风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    academic: { title: "学术风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    debate: { title: "辩论风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    assignment: { title: "作业风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    debate_plus: { title: "辩论增强", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    business: { title: "商务风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
  };
  const filePath = process.env.AIWF_OFFICE_THEME_FILE_DESKTOP
    || path.join(__dirname, "..", "..", "rules", "templates", "office_themes_desktop.json");
  try {
    if (!fs.existsSync(filePath)) return defaults;
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const normalized = normalizeOfficeThemeCatalog(parsed, { allowLegacyMap: true });
    if (normalized && normalized.themes && typeof normalized.themes === "object") {
      return { ...defaults, ...normalized.themes };
    }
  } catch {}
  return defaults;
}

function loadDesktopLayouts() {
  const defaults = {
    default: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 20,
      pptx_sample_rows: 8,
      pptx_max_findings: 6,
      pptx_max_evidence: 6,
    },
    fluent_ms: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 24,
      pptx_sample_rows: 10,
      pptx_max_findings: 8,
      pptx_max_evidence: 8,
    },
    fluent_ms_light: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 24,
      pptx_sample_rows: 10,
      pptx_max_findings: 8,
      pptx_max_evidence: 8,
    },
    fluent_ms_strong: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 22,
      pptx_sample_rows: 8,
      pptx_max_findings: 7,
      pptx_max_evidence: 7,
    },
    fluent_ms_vibrant: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 26,
      pptx_sample_rows: 10,
      pptx_max_findings: 9,
      pptx_max_evidence: 10,
    },
  };
  const filePath = process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP
    || path.join(__dirname, "..", "..", "rules", "templates", "office_layouts_desktop.json");
  try {
    if (!fs.existsSync(filePath)) return defaults;
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const normalized = normalizeOfficeLayoutCatalog(parsed, { allowLegacyMap: true });
    if (normalized && normalized.layouts && typeof normalized.layouts === "object") {
      return { ...defaults, ...normalized.layouts };
    }
  } catch {}
  return defaults;
}

function toTemplateLabel(id) {
  if (!id) return "通用模板";
  if (id === "finance_report_v1") return "财报模板 v1（资产/利润/现金流）";
  const normalized = String(id).replace(/[_-]+/g, " ").trim();
  if (!normalized) return "通用模板";
  return normalized.replace(/\b\w/g, (match) => match.toUpperCase());
}

function normalizeTemplateEntry(entry, templatesDir) {
  if (!entry || typeof entry !== "object") return null;
  const id = String(entry.id || "").trim().toLowerCase();
  if (!id || id === "default") return null;
  const file = String(entry.file || "").trim();
  if (!file) return null;
  const filePath = path.join(templatesDir, file);
  if (!fs.existsSync(filePath)) return null;

  let normalizedPayload = null;
  try {
    const raw = String(fs.readFileSync(filePath, "utf8") || "").replace(/^\uFEFF/, "");
    const parsed = JSON.parse(raw);
    normalizedPayload = normalizeCleaningTemplatePayload(parsed);
    if (!normalizedPayload) return null;
  } catch {
    return null;
  }

  return {
    id,
    file,
    label: String(entry.label || "").trim() || toTemplateLabel(id),
    description: String(entry.description || "").trim(),
    template_expected_profile: String(entry.template_expected_profile || "").trim().toLowerCase(),
    blank_output_expected: entry.blank_output_expected,
    rules: normalizedPayload.rules || null,
    cleaning_spec_v2: normalizedPayload.cleaning_spec_v2 || null,
    params_schema: normalizedPayload.params_schema || {},
    template_format: normalizedPayload.template_format || "",
  };
}

function loadCleaningTemplates() {
  const templatesDir = process.env.AIWF_CLEANING_TEMPLATE_DIR
    || path.join(__dirname, "..", "..", "rules", "templates");
  const list = [];
  const byId = new Map();
  const add = (entry) => {
    if (!entry || !entry.id || byId.has(entry.id)) return;
    byId.set(entry.id, entry);
    list.push(entry);
  };

  add({
    id: "default",
    file: "",
    label: "通用模板",
    description: "不追加模板规则，沿用通用清洗链路。",
    rules: null,
    cleaning_spec_v2: null,
    params_schema: {},
    template_format: "builtin_default",
  });

  const registryPath = path.join(templatesDir, "cleaning_templates_desktop.json");
  try {
    if (fs.existsSync(registryPath)) {
      const raw = String(fs.readFileSync(registryPath, "utf8") || "").replace(/^\uFEFF/, "");
      const registry = JSON.parse(raw);
      const normalizedRegistry = normalizeCleaningTemplateRegistry(registry, { allowLegacyRegistry: true });
      const items = Array.isArray(normalizedRegistry?.templates) ? normalizedRegistry.templates : [];
      items.forEach((item) => add(normalizeTemplateEntry(item, templatesDir)));
    }
  } catch {}

  try {
    if (fs.existsSync(templatesDir)) {
      const files = fs.readdirSync(templatesDir).filter((name) => /^generic_.*\.json$/i.test(name));
      files.forEach((file) => {
        const filePath = path.join(templatesDir, file);
        let parsed = null;
        try {
          const raw = String(fs.readFileSync(filePath, "utf8") || "").replace(/^\uFEFF/, "");
          parsed = JSON.parse(raw);
        } catch {
          return;
        }
        const normalizedPayload = normalizeCleaningTemplatePayload(parsed);
        if (!normalizedPayload) return;
        const id = String(parsed?.meta?.template_id || path.basename(file, ".json")).trim().toLowerCase();
        if (!id || byId.has(id)) return;
        add({
          id,
          file,
          label: String(parsed?.meta?.template_label || "").trim() || toTemplateLabel(id),
          description: String(parsed?.meta?.template_description || "").trim(),
          rules: normalizedPayload.rules || null,
          cleaning_spec_v2: normalizedPayload.cleaning_spec_v2 || null,
          params_schema: normalizedPayload.params_schema || {},
          template_format: normalizedPayload.template_format || "",
        });
      });
    }
  } catch {}

  return {
    list: list.map((entry) => ({
      id: entry.id,
      label: entry.label,
      description: entry.description,
      template_expected_profile: entry.template_expected_profile || "",
      blank_output_expected: entry.blank_output_expected,
      file: entry.file,
      rules: entry.rules || null,
      cleaning_spec_v2: entry.cleaning_spec_v2 || null,
      params_schema: entry.params_schema || {},
      template_format: entry.template_format || "",
    })),
    byId,
  };
}

function normalizeReportTitle(rawTitle, fallback = "辩论资料库") {
  const title = String(rawTitle || "").trim();
  if (!title) return fallback;
  const badCount = (title.match(/[?\uFFFD]/g) || []).length;
  const cjkCount = (title.match(/[\u4E00-\u9FFF]/g) || []).length;
  const ratio = badCount / Math.max(1, title.length);
  if (badCount >= 2 && (ratio >= 0.2 || cjkCount === 0)) return fallback;
  return title;
}

function createOfflineEngineConfig() {
  const officeThemes = loadDesktopThemes();
  const officeLayouts = loadDesktopLayouts();
  const cleaningTemplates = loadCleaningTemplates();

  function resolveOfficeTheme(name) {
    const forceFluent = String(process.env.AIWF_FORCE_FLUENT_STYLE || "1").trim() !== "0";
    if (forceFluent) {
      const normalized = String(name || "").trim().toLowerCase();
      if (normalized && officeThemes[normalized] && normalized.startsWith("fluent_ms")) return officeThemes[normalized];
      return officeThemes.fluent_ms_light || officeThemes.fluent_ms || officeThemes.assignment;
    }
    const normalized = String(name || "assignment").trim().toLowerCase();
    return officeThemes[normalized] || officeThemes.assignment;
  }

  function resolveOfficeLayout(name) {
    const forceFluent = String(process.env.AIWF_FORCE_FLUENT_STYLE || "1").trim() !== "0";
    const normalized = String(name || "assignment").trim().toLowerCase();
    const key = forceFluent
      ? ((normalized && normalized.startsWith("fluent_ms")) ? normalized : "fluent_ms_light")
      : normalized;
    const base = officeLayouts.default || {};
    const extension = officeLayouts[key] || {};
    return { ...base, ...extension };
  }

  function resolveCleaningTemplateParams(params = {}) {
    const nextParams = { ...(params || {}) };
    if (nextParams.cleaning_spec_v2 && typeof nextParams.cleaning_spec_v2 === "object") {
      Object.assign(nextParams, applyCleaningSpecToParams(nextParams, nextParams.cleaning_spec_v2));
    }
    const templateId = String(nextParams.cleaning_template || "").trim().toLowerCase();
    if (!templateId || templateId === "default") return nextParams;
    const template = cleaningTemplates.byId.get(templateId);
    if (!template) return nextParams;
    const withSpec = template.cleaning_spec_v2 ? applyCleaningSpecToParams(nextParams, template.cleaning_spec_v2) : nextParams;
    if (!String(withSpec.template_expected_profile || "").trim() && template.template_expected_profile) {
      withSpec.template_expected_profile = String(template.template_expected_profile).trim().toLowerCase();
    }
    if (withSpec.blank_output_expected === undefined && template.blank_output_expected !== undefined) {
      withSpec.blank_output_expected = Boolean(template.blank_output_expected);
    }
    if (!template.rules) return withSpec;
    withSpec.rules = { ...template.rules, ...(withSpec.rules || {}) };
    return withSpec;
  }

  function listCleaningTemplates() {
    return { ok: true, templates: cleaningTemplates.list };
  }

  return {
    listCleaningTemplates,
    normalizeReportTitle,
    resolveCleaningTemplateParams,
    resolveOfficeLayout,
    resolveOfficeTheme,
  };
}

module.exports = {
  createOfflineEngineConfig,
};

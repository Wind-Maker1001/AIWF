const $ = (id)=>document.getElementById(id);
const statusEl=$("status"), logEl=$("log"), rowsEl=$("rows"), queueRowsEl=$("queueRows"), dz=$("dropzone");
const sampleDz=$("sampleDropzone"), sampleRowsEl=$("sampleRows"), sampleHintEl=$("sampleHint");
const encodingHintEl=$("encodingHint"), fontHintEl=$("fontHint"), ocrHintEl=$("ocrHint");
const taskStoreHintEl=$("taskStoreHint");
const routeDiagEl=$("routeDiag"), routeDiagHintEl=$("routeDiagHint");
const selfCheckEl=$("selfCheck"), selfCheckHintEl=$("selfCheckHint");
const precheckEl=$("precheck"), precheckHintEl=$("precheckHint");
const precheckIssuesEl=$("precheckIssues"), precheckDetailEl=$("precheckDetail");
const debatePreviewEl=$("debatePreview"), debatePreviewHintEl=$("debatePreviewHint");
const tplManageMetaEl=$("tplManageMeta"), tplManageRulesEl=$("tplManageRules");
const btnOpenNoiseEl=$("btnOpenNoise");
const gateLogEl=$("gateLog"), gateRowsEl=$("gateRows");
const gateReleaseStatusEl=$("gateReleaseStatus");
const buildReleaseStatusEl=$("buildReleaseStatus");
const buildArtifactStatusEl=$("buildArtifactStatus");
const shellTabHintEl=$("shellTabHint");
const queuePaths = [];
let latestEncodingSummary = { uncertainCount: 0, gbCount: 0, utfCount: 0 };
let latestRouteSummaryMeta = null;
let latestArtifacts = [];
let templateCatalog = [];
let gateLogUnsub = null;
let buildLogUnsub = null;
let gateRunning = false;
let buildRunning = false;
window.__aiwfHomeReady = false;
window.__disabledTemplates = [];
window.__customTemplates = [];

const setStatus=(m,ok=true)=>{statusEl.className="status "+(ok?"ok":"bad");statusEl.textContent=m;};
const show=(o)=>{logEl.textContent=JSON.stringify(o,null,2);};
if (shellTabHintEl) {
  shellTabHintEl.textContent = "当前为作业助手视图，适合拖文件、一键运行与验收操作。Legacy Workflow Studio 仅保留为显式兼容入口。";
}
function normalizeTitleInput(raw, fallback="辩论资料包"){
  const t = String(raw || "").trim();
  if(!t) return fallback;
  const bad = (t.match(/[?\uFFFD]/g) || []).length;
  const cjk = (t.match(/[\u4E00-\u9FFF]/g) || []).length;
  if(bad >= 2 && (bad / Math.max(1, t.length) >= 0.2 || cjk === 0)) return fallback;
  return t;
}
const cfgFromUi=()=>({
  mode:$("mode").value,
  baseUrl:$("baseUrl").value.trim(),
  apiKey:$("apiKey").value.trim(),
  outputRoot:$("outputRoot").value.trim(),
  samplePoolDir:$("samplePoolDir").value.trim(),
});
const uiCfgFromUi=()=>({
  outputRoot:$("outputRoot").value.trim(),
  samplePoolDir:$("samplePoolDir").value.trim(),
  xlsxTemplatePath:$("xlsxTemplatePath").value.trim(),
  reportTitle:normalizeTitleInput($("reportTitle").value.trim()),
  officeLang:$("officeLang").value,
  officeTheme:$("officeTheme").value,
  officeVariant:$("officeVariant").value,
  cleaningTemplate:$("cleaningTemplate").value,
  debateBattlefieldRules:$("debateBattlefieldRules").value,
  debateSourcePriority:$("debateSourcePriority").value.trim(),
  officeQualityMode:$("officeQualityMode").value,
  ocrEnabled:$("ocrEnabled").checked,
  mdOnly:$("mdOnly").checked,
  ocrLang:$("ocrLang").value.trim(),
  ocrConfig:$("ocrConfig").value.trim(),
  autoNormalizeEncoding:$("autoNormalizeEncoding").checked,
  strictEncodingMode:$("strictEncodingMode").checked,
  precheckAmountRateMin:$("precheckAmountRateMin").value,
  maxInvalidRatio:$("maxInvalidRatio").value,
  minOutputRows:$("minOutputRows").value,
  minContentScore:$("minContentScore").value,
  contentQualityGateEnabled:$("contentQualityGateEnabled").checked,
  minOfficeQualityScore:$("minOfficeQualityScore").value,
  officeQualityGateEnabled:$("officeQualityGateEnabled").checked,
  disabledTemplates: Array.isArray(window.__disabledTemplates) ? window.__disabledTemplates : [],
  customTemplates: Array.isArray(window.__customTemplates) ? window.__customTemplates : []
});
const saveCfgFromUi=()=>Object.assign({}, cfgFromUi(), uiCfgFromUi());

function applyUiCfg(cfg){
  const c = cfg || {};
  const defaultDebateRules = "谣言=内容差\n成瘾=能力减损\n社交=社交与表达";
  $("outputRoot").value = c.outputRoot || "E:\\Desktop_Real\\AIWF";
  $("samplePoolDir").value = c.samplePoolDir || "";
  $("xlsxTemplatePath").value = c.xlsxTemplatePath || "";
  $("reportTitle").value = normalizeTitleInput(c.reportTitle || "");
  $("officeLang").value = c.officeLang || "zh";
  $("officeTheme").value = "fluent_ms";
  $("officeVariant").value = c.officeVariant || "light";
  $("cleaningTemplate").value = c.cleaningTemplate || "debate_evidence_v1";
  $("debateBattlefieldRules").value = c.debateBattlefieldRules || defaultDebateRules;
  $("debateSourcePriority").value = c.debateSourcePriority || "source_org,publisher,source,author,source_type";
  $("officeQualityMode").value = c.officeQualityMode || "high";
  $("ocrEnabled").checked = c.ocrEnabled !== false;
  $("mdOnly").checked = c.mdOnly !== false;
  $("ocrLang").value = c.ocrLang || "chi_sim+eng";
  $("ocrConfig").value = c.ocrConfig || "--oem 1 --psm 6";
  $("autoNormalizeEncoding").checked = c.autoNormalizeEncoding !== false;
  $("strictEncodingMode").checked = c.strictEncodingMode !== false;
  $("precheckAmountRateMin").value = c.precheckAmountRateMin || "0.90";
  $("maxInvalidRatio").value = c.maxInvalidRatio || "0.01";
  $("minOutputRows").value = c.minOutputRows || "1";
  $("minContentScore").value = c.minContentScore || "60";
  $("contentQualityGateEnabled").checked = c.contentQualityGateEnabled !== false;
  $("minOfficeQualityScore").value = c.minOfficeQualityScore || "65";
  $("officeQualityGateEnabled").checked = c.officeQualityGateEnabled !== false;
  window.__disabledTemplates = Array.isArray(c.disabledTemplates) ? c.disabledTemplates.map(x=>String(x)) : [];
  window.__customTemplates = Array.isArray(c.customTemplates) ? c.customTemplates : [];
}

function toNumberOrNull(v){
  const s = String(v ?? "").trim();
  if(!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function getDisabledTemplateSet(){
  const arr = Array.isArray(window.__disabledTemplates) ? window.__disabledTemplates : [];
  return new Set(arr.map(x=>String(x).trim().toLowerCase()).filter(Boolean));
}

function parseManualInputFiles(){
  return String($("inputFiles").value||"")
    .split(/\r?\n/)
    .map(s=>s.trim())
    .filter(Boolean);
}

function combinedInputFiles(){
  const all=[...parseManualInputFiles(), ...queuePaths];
  return Array.from(new Set(all));
}

function findTemplateById(id){
  const key = String(id || "").trim().toLowerCase();
  if(!key) return null;
  const all = Array.isArray(templateCatalog) ? templateCatalog : [];
  return all.find(t => String(t.id || "").trim().toLowerCase() === key) || null;
}

const payloadFromUi=()=>{
  const selectedTemplate = $("cleaningTemplate").value;
  const tpl = findTemplateById(selectedTemplate);
  const officeVariant = String($("officeVariant").value || "light").toLowerCase();
  const mappedTheme = officeVariant === "strong"
    ? "fluent_ms_strong"
    : (officeVariant === "vibrant" ? "fluent_ms_vibrant" : "fluent_ms_light");
  const p = {
    owner:"desktop",
    actor:"desktop",
    ruleset_version:"v1",
    params:{
      report_title:normalizeTitleInput($("reportTitle").value.trim()),
      input_path:$("inputPath").value.trim(),
      input_files:combinedInputFiles().join("\n"),
      office_lang:$("officeLang").value,
      office_theme:mappedTheme,
      xlsx_template_path:$("xlsxTemplatePath").value.trim(),
      cleaning_template:selectedTemplate,
      debate_battlefield_rules:$("debateBattlefieldRules").value,
      debate_source_priority:$("debateSourcePriority").value.trim(),
      office_quality_mode:$("officeQualityMode").value,
      md_only:$("mdOnly").checked,
      ocr_enabled:$("ocrEnabled").checked,
      ocr_lang:$("ocrLang").value.trim(),
      ocr_config:$("ocrConfig").value.trim(),
      precheck_amount_convert_rate_min: toNumberOrNull($("precheckAmountRateMin").value),
      min_content_score: toNumberOrNull($("minContentScore").value),
      content_quality_gate_enabled: $("contentQualityGateEnabled").checked,
      min_office_quality_score: toNumberOrNull($("minOfficeQualityScore").value),
      office_quality_gate_enabled: $("officeQualityGateEnabled").checked,
      rules: {
        max_invalid_ratio: toNumberOrNull($("maxInvalidRatio").value),
        min_output_rows: toNumberOrNull($("minOutputRows").value),
      }
    }
  };
  if (tpl && String(tpl.file || "") === "user_import") {
    if (tpl.cleaning_spec_v2 && typeof tpl.cleaning_spec_v2 === "object") {
      p.params.cleaning_spec_v2 = JSON.parse(JSON.stringify(tpl.cleaning_spec_v2));
    }
    if (tpl.rules && typeof tpl.rules === "object") {
      p.params.rules = { ...tpl.rules, ...(p.params.rules || {}) };
    }
  }
  Object.keys(p.params.rules || {}).forEach((k)=>{
    if (p.params.rules[k] === null || p.params.rules[k] === undefined || p.params.rules[k] === "") {
      delete p.params.rules[k];
    }
  });
  return p;
};

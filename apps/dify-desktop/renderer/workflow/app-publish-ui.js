import { formatWorkflowContractError } from "./workflow-contract.js";

function createWorkflowAppPublishUi(els, deps = {}) {
  const {
    setStatus = () => {},
    graphPayload = () => ({}),
    runWorkflowPreflight = async () => ({ ok: true, issues: [] }),
    collectAppSchemaFromForm = () => ({}),
    normalizeAppSchemaObject = (obj) => obj,
    currentTemplateGovernance = () => ({}),
    parseRunParamsLoose = () => ({}),
    getLastPreflightReport = () => null,
    getLastTemplateAcceptanceReport = () => null,
    renderAppRows = () => {},
    appSchemaRowsFromObject = () => [],
    renderAppSchemaForm = () => {},
    syncAppSchemaJsonFromForm = () => {},
    syncAppSchemaFormFromJson = () => {},
    syncRunParamsJsonFromForm = () => {},
    syncRunParamsFormFromJson = () => {},
  } = deps;

  function issueStatusText(issue = {}) {
    const message = String(issue?.message || "").trim();
    const resolution = String(issue?.resolution_hint || "").trim();
    return resolution ? `${message}（${resolution}）` : message;
  }

  function handleAppSchemaAdd() {
    const existing = appSchemaRowsFromObject(collectAppSchemaFromForm());
    existing.push({ key: "", type: "string", required: false, defaultText: "", description: "" });
    renderAppSchemaForm(existing);
    syncAppSchemaJsonFromForm();
    syncRunParamsFormFromJson();
  }

  function handleAppSchemaSyncJson() {
    syncAppSchemaJsonFromForm();
    syncRunParamsFormFromJson();
    setStatus("参数 Schema 已同步到 JSON", true);
  }

  function handleAppSchemaFromJson() {
    syncAppSchemaFormFromJson();
    syncRunParamsFormFromJson();
    setStatus("已从 JSON 回填参数 Schema", true);
  }

  function handleAppRunSyncJson() {
    syncRunParamsJsonFromForm();
    setStatus("运行参数已同步到 JSON", true);
  }

  function handleAppRunFromJson() {
    syncRunParamsFormFromJson();
    setStatus("已从 JSON 回填运行参数", true);
  }

  async function refreshApps() {
    try {
      const out = await window.aiwfDesktop.listWorkflowApps({ limit: 120 });
      renderAppRows(out?.items || []);
    } catch {
      renderAppRows([]);
    }
  }

  async function publishApp() {
    const graph = graphPayload();
    const name = String(els.appPublishName?.value || graph?.name || "").trim();
    if (!name) {
      setStatus("应用名称不能为空", false);
      return;
    }
    if (els.publishRequirePreflight?.checked) {
      const pre = await runWorkflowPreflight();
      if (!pre?.ok) {
        const errs = (pre.issues || [])
          .filter((x) => String(x.level || "") === "error")
          .map((x) => issueStatusText(x));
        setStatus(`发布阻断：预检未通过 (${errs.join(" | ")})`, false);
        return;
      }
    }
    let schema = {};
    try {
      const fromForm = collectAppSchemaFromForm();
      if (fromForm && Object.keys(fromForm).length) schema = fromForm;
      else if (String(els.appSchemaJson?.value || "").trim()) {
        schema = normalizeAppSchemaObject(JSON.parse(String(els.appSchemaJson.value || "{}")));
      }
    } catch (e) {
      setStatus(`参数 Schema 非法: ${e}`, false);
      return;
    }
    const out = await window.aiwfDesktop.publishWorkflowApp({
      name,
      graph,
      params_schema: schema,
      template_policy: {
        version: 1,
        governance: currentTemplateGovernance(),
        runtime_defaults: parseRunParamsLoose(),
      },
    });
    setStatus(out?.ok ? "流程应用已发布" : `发布失败: ${formatWorkflowContractError(out)}`, !!out?.ok);
    await refreshApps();
  }

  return {
    handleAppSchemaAdd,
    handleAppSchemaSyncJson,
    handleAppSchemaFromJson,
    handleAppRunSyncJson,
    handleAppRunFromJson,
    publishApp,
    refreshApps,
  };
}

export { createWorkflowAppPublishUi };

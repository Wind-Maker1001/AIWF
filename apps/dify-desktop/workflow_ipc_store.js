const {
  NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY,
  WORKFLOW_CONTRACT_AUTHORITY,
  assertWorkflowContract,
  createWorkflowContractError,
  normalizeWorkflowContract,
} = require("./workflow_contract");
const { TEMPLATE_PACK_ENTRY_SCHEMA_VERSION } = require("./workflow_ipc_state");
const {
  exportTemplatePackArtifact,
  normalizeTemplatePackArtifact,
} = require("./workflow_template_pack_contract");

function registerWorkflowStoreIpc(ctx, deps) {
  const {
    ipcMain,
    dialog,
    app,
    fs,
    path,
  } = ctx;
  const {
    appendAudit,
    isMockIoAllowed,
    listTemplateMarketplace,
    nowIso,
    qualityRuleSetSupport,
    resolveMockFilePath,
    saveTemplateMarketplace,
    workflowVersionStore,
  } = deps;

  function safeWorkflowName(name) {
    return String(name || "workflow")
      .replace(/[\\/:*?"<>|]/g, "_")
      .replace(/\s+/g, "_")
      .slice(0, 80) || "workflow";
  }

  function workflowContractFailure(error) {
    const details = error && typeof error === "object" && error.details && typeof error.details === "object"
      ? error.details
      : {};
    return {
      ok: false,
      canceled: false,
      error: String(error?.message || error || "workflow contract invalid"),
      error_code: String(error?.code || "workflow_contract_invalid"),
      graph_contract: String(details.graph_contract || WORKFLOW_CONTRACT_AUTHORITY),
      error_item_contract: String(details.error_item_contract || NODE_CONFIG_VALIDATION_ERROR_CONTRACT_AUTHORITY),
      error_items: Array.isArray(details.error_items) ? details.error_items : [],
    };
  }

  function workflowLoadFailure(error) {
    const isSyntaxError = error instanceof SyntaxError;
    return {
      ok: false,
      canceled: false,
      error: String(error?.message || error || "workflow load failed"),
      error_code: isSyntaxError ? "workflow_load_invalid_json" : "workflow_load_failed",
    };
  }

  ipcMain.handle("aiwf:listTemplateMarketplace", async (_evt, req) => {
    const limit = Number(req?.limit || 500);
    return { ok: true, items: listTemplateMarketplace(limit) };
  });

  ipcMain.handle("aiwf:installTemplatePack", async (_evt, req) => {
    try {
      const inlinePack = req?.pack && typeof req.pack === "object" ? req.pack : null;
      const fromPath = String(req?.path || "").trim();
      let pack = inlinePack;
      if (!pack && fromPath) {
        const parsed = JSON.parse(fs.readFileSync(fromPath, "utf8"));
        pack = parsed && typeof parsed === "object" ? parsed : null;
      }
      if (!pack) return { ok: false, error: "pack or path required" };
      const normalizedPack = normalizeTemplatePackArtifact(pack, {
        allowVersionMigration: true,
        source: fromPath || "inline",
      });
      const templates = Array.isArray(normalizedPack.templates) ? normalizedPack.templates : [];
      if (!templates.length) return { ok: false, error: "templates required" };
      const id = String(normalizedPack.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const item = {
        schema_version: TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
        id,
        name: String(normalizedPack.name || id),
        version: String(normalizedPack.version || "v1"),
        source: fromPath || "inline",
        templates,
        created_at: String(normalizedPack.created_at || nowIso()),
      };
      const items = listTemplateMarketplace(5000);
      const index = items.findIndex((row) => String(row?.id || "") === id);
      if (index >= 0) items[index] = item;
      else items.unshift(item);
      saveTemplateMarketplace(items);
      appendAudit("template_pack_install", {
        id,
        name: item.name,
        templates: templates.length,
        migrated: !!normalizedPack.migrated,
        notes: normalizedPack.notes || [],
      });
      return { ok: true, item, migrated: !!normalizedPack.migrated, notes: normalizedPack.notes || [] };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:removeTemplatePack", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      const items = listTemplateMarketplace(5000);
      saveTemplateMarketplace(items.filter((row) => String(row?.id || "") !== id));
      appendAudit("template_pack_remove", { id });
      return { ok: true };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:exportTemplatePack", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      const hit = listTemplateMarketplace(5000).find((row) => String(row?.id || "") === id);
      if (!hit) return { ok: false, error: "template pack not found" };
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const pick = await dialog.showSaveDialog({
          title: "导出模板包",
          defaultPath: path.join(app.getPath("documents"), `template_pack_${id}.json`),
          filters: [{ name: "JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      const artifact = exportTemplatePackArtifact(hit, {
        source: "marketplace_export",
      });
      fs.writeFileSync(filePath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listQualityRuleSets", async () => {
    try {
      return await qualityRuleSetSupport.listQualityRuleSets();
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:saveQualityRuleSet", async (_evt, req) => {
    const out = await qualityRuleSetSupport.saveQualityRuleSet(req);
    if (out?.ok) {
      appendAudit("quality_rule_set_save", {
        id: String(out?.set?.id || ""),
        name: String(out?.set?.name || ""),
        scope: String(out?.set?.scope || ""),
        provider: String(out?.provider || ""),
      });
    }
    return out;
  });

  ipcMain.handle("aiwf:removeQualityRuleSet", async (_evt, req) => {
    const out = await qualityRuleSetSupport.removeQualityRuleSet(req);
    if (out?.ok) {
      appendAudit("quality_rule_set_remove", {
        id: String(out?.id || ""),
        provider: String(out?.provider || ""),
      });
    }
    return out;
  });

  ipcMain.handle("aiwf:saveWorkflow", async (_evt, graph, name, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = isMockIoAllowed();
      const safeName = safeWorkflowName(name);
      let canceled = false;
      let filePath = "";
      if (options.mock && options.path && allowMockIo) {
        const safe = resolveMockFilePath(options.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const out = await dialog.showSaveDialog({
          title: "保存流程",
          defaultPath: path.join(app.getPath("documents"), `${safeName}.json`),
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        canceled = !!out.canceled;
        filePath = out.filePath || "";
      }
      if (canceled || !filePath) return { ok: false, canceled: true };
      const payload = graph && typeof graph === "object" ? graph : {};
      assertWorkflowContract(payload, { requireNonEmptyNodes: true });
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      const versionItem = {
        version_id: `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
        ts: nowIso(),
        workflow_name: String(payload?.name || safeName),
        workflow_id: String(payload?.workflow_id || "custom"),
        path: filePath,
        graph: payload,
      };
      const versionOut = await workflowVersionStore.recordVersion(versionItem);
      if (!versionOut?.ok) {
        appendAudit("workflow_version_record_failed", {
          path: filePath,
          workflow_name: String(payload?.name || safeName),
          error: String(versionOut?.error || "unknown"),
        });
        return {
          ok: false,
          canceled: false,
          saved_local: true,
          path: filePath,
          error: `workflow version record failed: ${versionOut?.error || "unknown"}`,
        };
      }
      appendAudit("workflow_save", { path: filePath, workflow_name: String(payload?.name || safeName) });
      return { ok: true, path: filePath };
    } catch (error) {
      if (error && typeof error === "object" && String(error.code || "") === "workflow_contract_invalid") {
        return workflowContractFailure(error);
      }
      return { ok: false, canceled: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:loadWorkflow", async (_evt, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = isMockIoAllowed();
      const validateGraphContract = options.validateGraphContract !== false;
      let canceled = false;
      let filePaths = [];
      if (options.mock && options.path && allowMockIo) {
        const safe = resolveMockFilePath(options.path);
        if (!safe.ok) return safe;
        filePaths = [safe.path];
      } else {
        const out = await dialog.showOpenDialog({
          title: "加载流程",
          filters: [{ name: "Workflow JSON", extensions: ["json"] }],
          properties: ["openFile"],
        });
        canceled = !!out.canceled;
        filePaths = out.filePaths || [];
      }
      if (canceled || !filePaths || !filePaths.length) return { ok: false, canceled: true };
      const filePath = filePaths[0];
      const graph = JSON.parse(fs.readFileSync(filePath, "utf8"));
      if (validateGraphContract) {
        const contract = normalizeWorkflowContract(graph, { allowVersionMigration: true });
        if (!contract.ok) {
          return workflowContractFailure(createWorkflowContractError(contract.errors));
        }
      }
      appendAudit("workflow_load", { path: filePath });
      return { ok: true, canceled: false, path: filePath, graph };
    } catch (error) {
      if (error && typeof error === "object" && String(error.code || "") === "workflow_contract_invalid") {
        return workflowContractFailure(error);
      }
      return workflowLoadFailure(error);
    }
  });
}

module.exports = {
  registerWorkflowStoreIpc,
};

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
    appendWorkflowVersion,
    isMockIoAllowed,
    listQualityRuleCenter,
    listTemplateMarketplace,
    nowIso,
    resolveMockFilePath,
    saveQualityRuleCenter,
    saveTemplateMarketplace,
  } = deps;

  function safeWorkflowName(name) {
    return String(name || "workflow")
      .replace(/[\\/:*?"<>|]/g, "_")
      .replace(/\s+/g, "_")
      .slice(0, 80) || "workflow";
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
      const templates = Array.isArray(pack.templates) ? pack.templates : [];
      if (!templates.length) return { ok: false, error: "templates required" };
      const id = String(pack.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const item = {
        id,
        name: String(pack.name || id),
        version: String(pack.version || "v1"),
        source: fromPath || "inline",
        templates,
        created_at: nowIso(),
      };
      const items = listTemplateMarketplace(5000);
      const index = items.findIndex((row) => String(row?.id || "") === id);
      if (index >= 0) items[index] = item;
      else items.unshift(item);
      saveTemplateMarketplace(items);
      appendAudit("template_pack_install", { id, name: item.name, templates: templates.length });
      return { ok: true, item };
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
      fs.writeFileSync(filePath, `${JSON.stringify(hit, null, 2)}\n`, "utf8");
      return { ok: true, path: filePath };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listQualityRuleSets", async () => {
    return { ok: true, sets: listQualityRuleCenter() };
  });

  ipcMain.handle("aiwf:saveQualityRuleSet", async (_evt, req) => {
    try {
      const set = req?.set && typeof req.set === "object" ? req.set : null;
      if (!set) return { ok: false, error: "set required" };
      const id = String(set.id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const row = {
        id,
        name: String(set.name || id),
        version: String(set.version || "v1"),
        rules: set.rules && typeof set.rules === "object" ? set.rules : {},
        scope: String(set.scope || "generic"),
        updated_at: nowIso(),
      };
      const sets = listQualityRuleCenter();
      const index = sets.findIndex((item) => String(item?.id || "") === id);
      if (index >= 0) sets[index] = row;
      else sets.unshift(row);
      saveQualityRuleCenter(sets);
      appendAudit("quality_rule_set_save", { id, name: row.name, scope: row.scope });
      return { ok: true, set: row };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:removeQualityRuleSet", async (_evt, req) => {
    try {
      const id = String(req?.id || "").trim();
      if (!id) return { ok: false, error: "id required" };
      saveQualityRuleCenter(listQualityRuleCenter().filter((item) => String(item?.id || "") !== id));
      appendAudit("quality_rule_set_remove", { id });
      return { ok: true };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
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
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      appendWorkflowVersion({
        version_id: `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
        ts: nowIso(),
        workflow_name: String(payload?.name || safeName),
        workflow_id: String(payload?.workflow_id || "custom"),
        path: filePath,
        graph: payload,
      });
      appendAudit("workflow_save", { path: filePath, workflow_name: String(payload?.name || safeName) });
      return { ok: true, path: filePath };
    } catch (error) {
      return { ok: false, canceled: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:loadWorkflow", async (_evt, opts) => {
    try {
      const options = opts && typeof opts === "object" ? opts : {};
      const allowMockIo = isMockIoAllowed();
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
      appendAudit("workflow_load", { path: filePath });
      return { ok: true, canceled: false, path: filePath, graph };
    } catch (error) {
      return { ok: false, canceled: false, error: String(error) };
    }
  });
}

module.exports = {
  registerWorkflowStoreIpc,
};

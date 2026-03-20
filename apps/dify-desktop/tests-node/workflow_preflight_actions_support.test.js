const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightActionsSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-actions-support.js")).href;
  return import(file);
}

test("workflow preflight actions support normalizes export format and template lookup", async () => {
  const {
    exportFormatFromValue,
    findTemplateById,
  } = await loadPreflightActionsSupportModule();

  assert.equal(exportFormatFromValue("json"), "json");
  assert.equal(exportFormatFromValue("MD"), "md");
  assert.equal(exportFormatFromValue(""), "md");

  const tpl = findTemplateById([
    { id: "tpl_1", name: "One" },
    { id: "tpl_2", name: "Two" },
  ], "tpl_2");
  assert.deepEqual(tpl, { id: "tpl_2", name: "Two" });
  assert.equal(findTemplateById([], "tpl_x"), null);
});

test("workflow preflight actions support builds acceptance report", async () => {
  const { buildTemplateAcceptanceReport } = await loadPreflightActionsSupportModule();
  const report = buildTemplateAcceptanceReport({
    templateId: "tpl_1",
    templateName: "Template One",
    accepted: true,
    governance: { mode: "strict" },
    before: { ok: false },
    autoFix: { changed: true },
    after: { ok: true },
    now: () => "2026-03-19T00:00:00.000Z",
  });

  assert.deepEqual(report, {
    ts: "2026-03-19T00:00:00.000Z",
    template_id: "tpl_1",
    template_name: "Template One",
    accepted: true,
    governance: { mode: "strict" },
    before: { ok: false },
    auto_fix: { changed: true },
    after: { ok: true },
  });
});

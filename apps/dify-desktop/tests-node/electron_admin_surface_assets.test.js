const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron admin-only workflow surfaces require explicit legacyAdmin mode", () => {
  const initJs = readText("apps/dify-desktop/renderer/fluent-init.js");
  const css = readText("apps/dify-desktop/renderer/fluent-shell.css");
  const workflowHtml = readText("apps/dify-desktop/renderer/workflow.html");

  assert.match(initJs, /legacyAdmin/);
  assert.match(initJs, /compat-admin-mode/);
  assert.match(css, /\.compat-admin-only/);
  assert.match(css, /body\.compat-admin-mode \.compat-admin-only/);
  assert.match(workflowHtml, /兼容管理面默认隐藏/);
  assert.match(workflowHtml, /btnTemplatePackInstall" class="btn secondary compat-admin-only"/);
  assert.match(workflowHtml, /btnQueuePause" class="btn secondary dev-only compat-admin-only"/);
  assert.match(workflowHtml, /<table class="dev-only compat-admin-only">\s*<thead><tr><th>Chiplet/);
  assert.match(workflowHtml, /<div class="cfg-form dev-only compat-admin-only" style="margin-top:8px">\s*<div class="cfg-help">运行时间线与失败概览<\/div>/);
  assert.match(workflowHtml, /<div class="cfg-form dev-only compat-admin-only">\s*<div class="cfg-help">运行对比<\/div>/);
  assert.match(workflowHtml, /btnPublishApp/);
  assert.match(workflowHtml, /reviewHistoryRows/);
});

test("electron admin mode has explicit compatibility entrypoints", () => {
  const mainWindowSupport = readText("apps/dify-desktop/main_window_support.js");
  const runFrontend = readText("ops/scripts/run_aiwf_frontend.ps1");
  const runDesktop = readText("ops/scripts/run_dify_desktop.ps1");

  assert.match(mainWindowSupport, /打开 Legacy Workflow 管理面/);
  assert.match(mainWindowSupport, /--workflow-admin/);
  assert.match(runFrontend, /\[switch\]\$WorkflowAdmin/);
  assert.match(runDesktop, /\[switch\]\$WorkflowAdmin/);
  assert.match(runDesktop, /--workflow-admin/);
});

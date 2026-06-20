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
  const homeShared = readText("apps/dify-desktop/renderer/home-shared.js");
  const indexHtml = readText("apps/dify-desktop/renderer/index.html");
  const workflowHtml = readText("apps/dify-desktop/renderer/workflow.html");

  assert.match(initJs, /legacyAdmin/);
  assert.match(initJs, /compatAdminAuthorized/);
  assert.match(initJs, /compat-admin-mode/);
  assert.match(css, /\.compat-admin-only/);
  assert.match(css, /body\.compat-admin-mode \.compat-admin-only/);
  assert.doesNotMatch(homeShared, /tabWorkflow/);
  assert.doesNotMatch(homeShared, /workflowEmbedFrame/);
  assert.doesNotMatch(indexHtml, /role="tablist"/);
  assert.doesNotMatch(indexHtml, /id="tabHome"/);
  assert.doesNotMatch(indexHtml, /id="homeShellPane"/);
  assert.match(workflowHtml, /兼容/);
  assert.match(workflowHtml, /Compatibility Canvas/);
  assert.match(workflowHtml, /btnTemplatePackInstall" class="btn secondary compat-admin-only"/);
  assert.match(workflowHtml, /btnQueuePause" class="btn secondary dev-only compat-admin-only"/);
  assert.match(workflowHtml, /<table class="dev-only compat-admin-only">\s*<thead><tr><th>Chiplet/);
  assert.match(workflowHtml, /btnPublishApp/);
  assert.match(workflowHtml, /reviewHistoryRows/);
});

test("electron admin mode keeps explicit script and argv compatibility entrypoints", () => {
  const mainWindowSupport = readText("apps/dify-desktop/main_window_support.js");
  const runFrontend = readText("ops/scripts/run_aiwf_frontend.ps1");
  const runDesktop = readText("ops/scripts/run_dify_desktop.ps1");

  assert.doesNotMatch(mainWindowSupport, /Legacy Workflow Studio/);
  assert.doesNotMatch(mainWindowSupport, /Legacy Workflow.*管理面/);
  assert.match(mainWindowSupport, /--workflow-admin/);
  assert.match(mainWindowSupport, /compatAdminAuthorized/);
  assert.match(mainWindowSupport, /--offline-home/);
  assert.match(runFrontend, /Electron runtime launch no longer runs through run_aiwf_frontend\.ps1/i);
  assert.match(runFrontend, /run_dify_desktop\.ps1 with -Workflow or -WorkflowAdmin/i);
  assert.match(runDesktop, /-OfflineHome, -Workflow, or -WorkflowAdmin/i);
  assert.match(runDesktop, /\[switch\]\$OfflineHome/);
  assert.doesNotMatch(runFrontend, /\[switch\]\$WorkflowAdmin/);
  assert.match(runDesktop, /\[switch\]\$WorkflowAdmin/);
  assert.match(runDesktop, /--workflow-admin/);
});

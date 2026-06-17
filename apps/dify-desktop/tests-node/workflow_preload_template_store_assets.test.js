const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("preload exposes shared local template IPC helpers to workflow renderer", () => {
  const preload = readText("apps/dify-desktop/preload.js");
  const marketplace = readText("apps/dify-desktop/renderer/workflow/template-ui-marketplace.js");

  assert.match(preload, /listLocalTemplates:\s*\(req\)\s*=>\s*ipcRenderer\.invoke\("aiwf:listLocalTemplates", req\)/);
  assert.match(preload, /saveLocalTemplate:\s*\(req\)\s*=>\s*ipcRenderer\.invoke\("aiwf:saveLocalTemplate", req\)/);
  assert.match(marketplace, /window\.aiwfDesktop\.listLocalTemplates/);
  assert.match(marketplace, /window\.aiwfDesktop\.saveLocalTemplate/);
});

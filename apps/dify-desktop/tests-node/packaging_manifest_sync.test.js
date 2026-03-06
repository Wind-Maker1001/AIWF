const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

function readJson(relPath) {
  const fullPath = path.join(__dirname, "..", relPath);
  return JSON.parse(fs.readFileSync(fullPath, "utf8").replace(/^\uFEFF/, ""));
}

test("lite packaging manifest keeps workflow runtime files in sync", () => {
  const pkg = readJson("package.json");
  const lite = readJson(path.join("build", "electron-builder.lite.json"));
  const fullFiles = new Set(Array.isArray(pkg?.build?.files) ? pkg.build.files : []);
  const liteFiles = new Set(Array.isArray(lite?.files) ? lite.files : []);

  const required = [...fullFiles].filter((entry) => (
    entry === "main_ipc_workflow.js"
      || entry === "workflow_governance.js"
      || entry.startsWith("workflow_")
      || entry.startsWith("workflow_chiplets/")
  ));

  const missing = required.filter((entry) => !liteFiles.has(entry));
  assert.deepEqual(
    missing,
    [],
    `lite packaging manifest is missing workflow runtime entries: ${missing.join(", ")}`
  );
});

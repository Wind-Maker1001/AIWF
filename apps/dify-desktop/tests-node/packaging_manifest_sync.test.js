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
  const fullExtraResources = Array.isArray(pkg?.build?.extraResources) ? pkg.build.extraResources : [];
  const liteExtraResources = Array.isArray(lite?.extraResources) ? lite.extraResources : [];

  const required = [...fullFiles].filter((entry) => (
    entry === "main_ipc_workflow.js"
      || entry === "workflow_contract.js"
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

  assert.ok(fullFiles.has("workflow_contract.js"), "full packaging manifest must include workflow_contract.js");
  assert.ok(liteFiles.has("workflow_contract.js"), "lite packaging manifest must include workflow_contract.js");

  const hasDesktopContracts = (entry) => (
    entry
    && String(entry.from || "").includes("contracts/desktop")
    && String(entry.to || "").includes("contracts/desktop")
  );
  const hasWorkflowContracts = (entry) => (
    entry
    && String(entry.from || "").includes("contracts/workflow")
    && String(entry.to || "").includes("contracts/workflow")
  );
  const hasRustContracts = (entry) => (
    entry
    && String(entry.from || "").includes("contracts/rust")
    && String(entry.to || "").includes("contracts/rust")
  );
  assert.ok(fullExtraResources.some(hasDesktopContracts), "full packaging manifest must include desktop contract schemas");
  assert.ok(liteExtraResources.some(hasDesktopContracts), "lite packaging manifest must include desktop contract schemas");
  assert.ok(fullExtraResources.some(hasWorkflowContracts), "full packaging manifest must include workflow contracts");
  assert.ok(liteExtraResources.some(hasWorkflowContracts), "lite packaging manifest must include workflow contracts");
  assert.ok(fullExtraResources.some(hasRustContracts), "full packaging manifest must include rust contracts");
  assert.ok(liteExtraResources.some(hasRustContracts), "lite packaging manifest must include rust contracts");
});

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron main bootstrap writes staged boot markers and defers heavy workflow loading", () => {
  const mainJs = readText("apps/dify-desktop/main.js");
  const packaged = readText("ops/scripts/check_desktop_packaged_startup.ps1");
  const lite = readText("ops/scripts/check_desktop_lite_packaged_startup.ps1");
  const desktopPackage = JSON.parse(readText("apps/dify-desktop/package.json"));
  const desktopLitePackage = JSON.parse(readText("apps/dify-desktop/build/electron-builder.lite.json"));
  const buildFiles = Array.isArray(desktopPackage?.build?.files) ? desktopPackage.build.files : [];
  const liteBuildFiles = Array.isArray(desktopLitePackage?.files) ? desktopLitePackage.files : [];

  assert.match(mainJs, /function listCleaningTemplatesProxy/);
  assert.match(mainJs, /function runMinimalWorkflowProxy/);
  assert.match(mainJs, /writeBootMarker\(\{ stage: "app_ready" \}\)/);
  assert.match(mainJs, /writeBootMarker\(\{ stage: "bootstrapped" \}\)/);
  assert.match(mainJs, /writeBootMarker\(\{ stage: "boot_failed"/);
  assert.match(mainJs, /writeBootMarker\(\{ stage: "uncaught_exception"/);
  assert.match(mainJs, /function initializeServices\(\)/);
  assert.match(mainJs, /writeBootMarker\(\{ stage: "process_boot" \}\)/);
  for (const requiredFile of [
    "workflow_validation_service.js",
    "workflow_execution_service.js",
    "workflow_store_remote_error.js",
    "workflow_template_pack_contract.js",
    "offline_template_catalog_contract.js",
  ]) {
    assert.ok(buildFiles.includes(requiredFile), `${requiredFile} must be included in electron build.files`);
    assert.ok(liteBuildFiles.includes(requiredFile), `${requiredFile} must be included in lite electron build.files`);
  }

  for (const scriptText of [packaged, lite]) {
    assert.match(scriptText, /\$boot = Get-Content -Raw -Encoding UTF8 \$bootMarker \| ConvertFrom-Json/);
    assert.match(scriptText, /\$stage = \[string\]\(\$boot\.stage\)/);
    assert.match(scriptText, /boot_failed/);
    assert.match(scriptText, /uncaught_exception/);
    assert.match(scriptText, /boot marker stage is unexpected/);
  }
});

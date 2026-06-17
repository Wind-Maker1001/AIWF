const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron docs are no longer positioned as primary onboarding", () => {
  const readme = readText("README.md");
  const quickstart = readText("docs/quickstart.md");
  const offlineDelivery = readText("docs/offline_delivery_minimal.md");
  const desktopDoc = readText("docs/dify_desktop_app.md");
  const nativeDelivery = readText("docs/offline_delivery_native_winui.md");
  const retirement = readText("docs/electron_compatibility_retirement_plan_20260321.md");
  const winuiQuickstart = readText("docs/quickstart_native_winui.md");
  const electronHome = readText("apps/dify-desktop/renderer/index.html");

  assert.match(readme, /## Compatibility Paths/);
  assert.ok(
    readme.indexOf("## Compatibility Paths") < readme.indexOf("release_electron_compatibility.ps1"),
    "compatibility packaging path should only be referenced under the README compatibility section",
  );
  assert.doesNotMatch(readme, /run_dify_desktop\.ps1 -Workflow/);
  assert.match(quickstart, /## Compatibility/);
  assert.match(offlineDelivery, /secondary Electron compatibility frontend/i);
  assert.match(electronHome, /WinUI 是主前端/);
  assert.doesNotMatch(desktopDoc, /advanced diagnostics/i);
  assert.match(desktopDoc, /admin-gated compatibility panels/i);
  assert.match(nativeDelivery, /admin-gated panels/i);
  assert.doesNotMatch(retirement, /not yet ported/i);
  assert.match(retirement, /still exist behind explicit entrypoints/i);
  assert.doesNotMatch(winuiQuickstart, /advanced diagnostics/i);
  assert.doesNotMatch(winuiQuickstart, /run_dify_desktop\.ps1 -Workflow/);
});

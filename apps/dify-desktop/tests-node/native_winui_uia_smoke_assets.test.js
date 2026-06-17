const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("native winui UIA smoke covers workflow template canvas controls", () => {
  const script = readText("ops/scripts/check_native_winui_uia_smoke.ps1");
  const readme = readText("apps/dify-native-winui/README.md");

  assert.match(script, /WorkflowTemplateSelectComboBox/);
  assert.match(script, /SaveCurrentWorkflowAsTemplateButton/);
  assert.match(script, /TemplateRequirePreflightCheckBox/);
  assert.match(script, /WorkflowTemplateStatusTextBlock/);
  assert.match(script, /workflow template canvas section is visible/i);
  assert.match(script, /Reveal-ElementByPageDown/);
  assert.match(script, /Find-ScrollableAncestorPattern/);
  assert.match(script, /ScrollPattern/);
  assert.match(script, /LargeIncrement/);
  assert.match(script, /NodeTitleTextBox/);
  assert.match(script, /mouse_event\(0x0800/);

  assert.match(readme, /workflow template canvas section visibility/i);
  assert.match(readme, /check_native_winui_uia_smoke\.ps1/i);
});

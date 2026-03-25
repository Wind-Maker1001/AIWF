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

  assert.match(readme, /## Compatibility Paths/);
  assert.match(quickstart, /## Compatibility/);
  assert.match(offlineDelivery, /secondary Electron compatibility frontend/i);
});

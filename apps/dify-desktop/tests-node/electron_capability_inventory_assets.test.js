const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron capability inventory is present and linked from retirement plan", () => {
  const inventory = readText("docs/electron_capability_inventory_20260321.md");
  const retirement = readText("docs/electron_compatibility_retirement_plan_20260321.md");

  assert.match(inventory, /2026-03-21/);
  assert.match(inventory, /covered/);
  assert.match(inventory, /partial/);
  assert.match(inventory, /missing/);
  assert.match(inventory, /compat-hidden/);
  assert.match(inventory, /technically heavy semantics/);
  assert.match(inventory, /frequent manual adjustment/);
  assert.match(inventory, /backendize rule storage\/evaluation\/versioning/);
  assert.match(inventory, /keep in frontend; this is human-curated workflow authoring/);
  assert.match(inventory, /--workflow-admin/);
  assert.match(inventory, /\?legacyAdmin=1/);
  assert.match(inventory, /2026-04-05/);
  assert.match(inventory, /2026-04-19/);
  assert.match(inventory, /2026-05-19/);
  assert.match(inventory, /2026-06-18/);
  assert.match(retirement, /electron_capability_inventory_20260321\.md/);
  assert.match(retirement, /--workflow-admin/);
  assert.match(retirement, /technically heavy behavior should move behind backend-owned contracts/);
});

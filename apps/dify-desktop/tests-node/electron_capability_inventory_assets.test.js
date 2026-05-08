const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron capability inventory is present and linked from retirement plan", () => {
  const inventory = readText("docs/electron_capability_inventory_20260321.md");
  const inventoryContract = JSON.parse(readText("contracts/desktop/electron_compatibility_inventory.v1.json"));
  const retirement = readText("docs/electron_compatibility_retirement_plan_20260321.md");

  assert.match(inventory, /2026-03-21/);
  assert.match(inventory, /2026-05-08 review update/i);
  assert.match(inventory, /electron_compatibility_inventory\.v1\.json/);
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
  assert.match(inventory, /2026-06-18/);
  assert.equal(inventoryContract.schema_version, "electron_compatibility_inventory.v1");
  assert.equal(inventoryContract.authority_doc, "docs/electron_capability_inventory_20260321.md");
  assert.equal(inventoryContract.reviewed_at, "2026-05-08");
  assert.equal(inventoryContract.next_review_by, "2026-06-18");
  assert.ok(Array.isArray(inventoryContract.electron_only_capabilities));
  assert.ok(inventoryContract.electron_only_capabilities.length >= 10);
  assert.ok(Array.isArray(inventoryContract.retained_compatibility_surfaces));
  assert.ok(inventoryContract.retained_compatibility_surfaces.length >= 3);
  assert.match(retirement, /electron_capability_inventory_20260321\.md/);
  assert.match(retirement, /electron_compatibility_inventory\.v1\.json/);
  assert.match(retirement, /--workflow-admin/);
  assert.match(retirement, /technically heavy behavior should move behind backend-owned contracts/);
});

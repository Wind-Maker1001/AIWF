const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("node config contract assets are present and exported", () => {
  const contractJson = readText("contracts/desktop/node_config_contracts.v1.json");
  const exportScript = readText("ops/scripts/export_node_config_contracts.ps1");
  const supportScript = readText("ops/scripts/node_config_contract_support.js");
  const cjsModule = readText("apps/dify-desktop/workflow_node_config_contract.generated.js");
  const esmModule = readText("apps/dify-desktop/renderer/workflow/node_config_contract.generated.js");

  assert.match(contractJson, /node_config_contracts\.v1/);
  assert.match(contractJson, /ingest_files/);
  assert.match(contractJson, /load_rows_v3/);
  assert.match(contractJson, /quality_check_v4/);
  assert.match(contractJson, /join_rows_v4/);
  assert.match(contractJson, /aggregate_rows_v4/);
  assert.match(contractJson, /window_rows_v1/);
  assert.match(contractJson, /plugin_registry_v1/);

  assert.match(exportScript, /node_config_contracts\.v1\.json/);
  assert.match(exportScript, /workflow_node_config_contract\.generated\.js/);
  assert.match(exportScript, /node_config_contract\.generated\.js/);

  assert.match(supportScript, /loadNodeConfigContractSet/);
  assert.match(supportScript, /renderCommonJsModule/);
  assert.match(supportScript, /renderEsmModule/);
  assert.match(supportScript, /NODE_CONFIG_CONTRACT_TYPES/);

  assert.match(cjsModule, /NODE_CONFIG_CONTRACT_TYPES/);
  assert.match(cjsModule, /join_rows_v4/);
  assert.match(cjsModule, /plugin_registry_v1/);
  assert.match(esmModule, /NODE_CONFIG_CONTRACT_TYPES/);
  assert.match(esmModule, /aggregate_rows_v4/);
  assert.match(esmModule, /plugin_registry_v1/);
});

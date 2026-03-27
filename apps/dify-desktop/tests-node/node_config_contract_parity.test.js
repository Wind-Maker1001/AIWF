const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const repoRoot = path.resolve(__dirname, "../../..");
const fixturePath = path.resolve(repoRoot, "contracts/desktop/node_config_contract_fixtures.v1.json");

async function loadWorkflowContractModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/workflow-contract.js")).href;
  return import(file);
}

function loadFixtures() {
  return JSON.parse(fs.readFileSync(fixturePath, "utf8"));
}

function flattenCases(fixtures) {
  const entries = Array.isArray(fixtures?.nodes) ? fixtures.nodes : [];
  const out = [];
  for (const entry of entries) {
    const nodeType = String(entry?.type || "").trim();
    for (const item of Array.isArray(entry?.valid_cases) ? entry.valid_cases : []) {
      out.push({
        id: String(item?.id || "").trim(),
        nodeType,
        expectedOk: true,
        config: item?.config || {},
        expectedErrorContains: [],
      });
    }
    for (const item of Array.isArray(entry?.invalid_cases) ? entry.invalid_cases : []) {
      out.push({
        id: String(item?.id || "").trim(),
        nodeType,
        expectedOk: false,
        config: item?.config || {},
        expectedErrorContains: Array.isArray(item?.expected_error_contains) ? item.expected_error_contains : [],
        expectedErrorItems: Array.isArray(item?.expected_error_items) ? item.expected_error_items : [],
      });
    }
  }
  return out;
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function evaluateCase(assertWorkflowContract, workflowSchemaVersion, nodeType, config) {
  try {
    const result = assertWorkflowContract({
      workflow_id: "wf_parity_fixture",
      version: workflowSchemaVersion,
      nodes: [{ id: "n1", type: nodeType, config }],
      edges: [],
    });
    return { ok: !!result?.ok, errors: [], errorItems: [] };
  } catch (error) {
    const details = Array.isArray(error?.details?.errors) ? error.details.errors : [];
    const errorItems = Array.isArray(error?.details?.error_items) ? error.details.error_items : [];
    return {
      ok: false,
      errors: details.map((item) => String(item || "").trim()).filter(Boolean),
      errorItems,
    };
  }
}

test("node config contract fixtures stay valid against desktop runtime", async () => {
  const fixtures = loadFixtures();
  assert.equal(fixtures.schema_version, "node_config_contract_fixtures.v1");
  assert.equal(fixtures.authority, "contracts/desktop/node_config_contracts.v1.json");
  assert.ok(Array.isArray(fixtures.required_node_types));
  assert.equal(fixtures.required_node_types.length, 30);

  const cases = flattenCases(fixtures);
  assert.equal(cases.length, 60);

  const { WORKFLOW_SCHEMA_VERSION, assertWorkflowContract } = await loadWorkflowContractModule();

  for (const fixtureCase of cases) {
    const result = evaluateCase(assertWorkflowContract, WORKFLOW_SCHEMA_VERSION, fixtureCase.nodeType, fixtureCase.config);
    assert.equal(result.ok, fixtureCase.expectedOk, fixtureCase.id);
    for (const expected of fixtureCase.expectedErrorContains) {
      assert.match(result.errors.join("\n"), new RegExp(escapeRegExp(expected)), fixtureCase.id);
    }
    for (const expectedItem of fixtureCase.expectedErrorItems || []) {
      assert.deepEqual(
        result.errorItems.some((item) => item.path === expectedItem.path && item.code === expectedItem.code),
        true,
        fixtureCase.id,
      );
    }
  }
});

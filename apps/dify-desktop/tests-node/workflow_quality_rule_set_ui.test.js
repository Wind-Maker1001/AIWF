const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadQualityRuleSetUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/quality-rule-set-ui.js")).href;
  return import(file);
}

test("workflow quality rule set ui refreshes selectable rule sets and preserves current id", async () => {
  const { createWorkflowQualityRuleSetUi } = await loadQualityRuleSetUiModule();
  const appended = [];
  global.window = {
    aiwfDesktop: {
      listQualityRuleSets: async () => ({
        sets: [
          { id: "set_a", name: "Set A", version: "v2" },
          { id: "set_b", name: "Set B", version: "v1" },
        ],
      }),
    },
  };

  const select = {
    innerHTML: "",
    value: "",
    appendChild(node) {
      appended.push({ value: node.value, textContent: node.textContent });
    },
  };

  try {
    const ui = createWorkflowQualityRuleSetUi({
      qualityRuleSetSelect: select,
      qualityRuleSetId: { value: "set_b" },
    }, {
      createOptionElement: () => ({ value: "", textContent: "" }),
    });
    await ui.refreshQualityRuleSets();
  } finally {
    delete global.window;
  }

  assert.equal(select.innerHTML, '<option value="">选择规则集...</option>');
  assert.equal(select.value, "set_b");
  assert.deepEqual(appended, [
    { value: "set_a", textContent: "Set A (v2)" },
    { value: "set_b", textContent: "Set B (v1)" },
  ]);
});

test("workflow quality rule set ui saves graph rules and refreshes list", async () => {
  const { createWorkflowQualityRuleSetUi } = await loadQualityRuleSetUiModule();
  const statuses = [];
  const saved = [];
  let refreshCount = 0;
  global.window = {
    aiwfDesktop: {
      listQualityRuleSets: async () => {
        refreshCount += 1;
        return { sets: [] };
      },
      saveQualityRuleSet: async (payload) => {
        saved.push(payload);
        return { ok: true };
      },
    },
  };

  try {
    const ui = createWorkflowQualityRuleSetUi({
      qualityRuleSetId: { value: "finance-default" },
      qualityRuleSetSelect: { innerHTML: "", value: "", appendChild() {} },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      exportGraph: () => ({
        nodes: [
          { id: "n1", type: "quality_check_v3", config: { rules: { required_columns: ["amount"] } } },
        ],
      }),
      createOptionElement: () => ({ value: "", textContent: "" }),
    });
    await ui.saveQualityRuleSetFromGraph();
  } finally {
    delete global.window;
  }

  assert.deepEqual(saved, [{
    set: {
      id: "finance-default",
      name: "finance-default",
      version: "v1",
      scope: "workflow",
      rules: { required_columns: ["amount"] },
    },
  }]);
  assert.equal(refreshCount, 1);
  assert.deepEqual(statuses, [{ text: "质量规则集已保存: finance-default", ok: true }]);
});

test("workflow quality rule set ui removes current set and syncs selected id", async () => {
  const { createWorkflowQualityRuleSetUi } = await loadQualityRuleSetUiModule();
  const statuses = [];
  const removals = [];
  const idInput = { value: "" };
  const select = { value: "set_b", innerHTML: "", appendChild() {} };
  global.window = {
    aiwfDesktop: {
      listQualityRuleSets: async () => ({ sets: [] }),
      removeQualityRuleSet: async ({ id }) => {
        removals.push(id);
        return { ok: true };
      },
    },
  };

  try {
    const ui = createWorkflowQualityRuleSetUi({
      qualityRuleSetId: idInput,
      qualityRuleSetSelect: select,
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      createOptionElement: () => ({ value: "", textContent: "" }),
    });
    ui.handleQualityRuleSetSelectChange();
    await ui.removeQualityRuleSetCurrent();
  } finally {
    delete global.window;
  }

  assert.equal(idInput.value, "");
  assert.deepEqual(removals, ["set_b"]);
  assert.deepEqual(statuses, [{ text: "质量规则集已删除: set_b", ok: true }]);
});

test("workflow quality rule set ui formats structured save failure", async () => {
  const { createWorkflowQualityRuleSetUi } = await loadQualityRuleSetUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveQualityRuleSet: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowQualityRuleSetUi({
      qualityRuleSetId: { value: "finance-default" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      exportGraph: () => ({ nodes: [], edges: [] }),
    });
    await ui.saveQualityRuleSetFromGraph();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[required\]/);
  assert.match(statuses[0].text, /workflow\.version/);
});

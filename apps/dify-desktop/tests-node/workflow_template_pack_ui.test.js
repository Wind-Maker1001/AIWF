const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-ui.js")).href;
  return import(file);
}

function createSelectStub() {
  return {
    value: "",
    innerHTML: "",
    appended: [],
    appendChild(node) {
      this.appended.push({ value: node.value, textContent: node.textContent });
    },
  };
}

test("workflow template ui installs template pack and refreshes template select", async () => {
  const { createWorkflowTemplateUi } = await loadTemplateUiModule();
  const statuses = [];
  const select = createSelectStub();
  global.document = {
    createElement: () => ({ value: "", textContent: "" }),
  };
  global.window = {
    localStorage: {
      getItem: () => null,
      setItem: () => {},
    },
    aiwfDesktop: {
      loadWorkflow: async () => ({ ok: true, path: "D:/packs/sample-template-pack.json" }),
      installTemplatePack: async (payload) => {
        assert.deepEqual(payload, { path: "D:/packs/sample-template-pack.json" });
        return { ok: true, item: { id: "pack_1", name: "Finance Pack" } };
      },
      listTemplateMarketplace: async () => ({
        items: [
          {
            id: "pack_1",
            name: "Finance Pack",
            templates: [{ id: "tpl_1", name: "Finance Template" }],
          },
        ],
      }),
    },
  };

  try {
    const ui = createWorkflowTemplateUi({
      templateSelect: select,
      templateParamsForm: null,
      templateParams: { value: "" },
    }, {
      builtinTemplates: [],
      store: {},
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.installTemplatePack();
  } finally {
    delete global.window;
    delete global.document;
  }

  assert.equal(select.innerHTML, '<option value="">选择模板...</option>');
  assert.deepEqual(select.appended, [{ value: "tpl_1", textContent: "Finance Template" }]);
  assert.deepEqual(statuses, [{ text: "模板包已安装: Finance Pack", ok: true }]);
});

test("workflow template ui removes and exports current template pack", async () => {
  const { createWorkflowTemplateUi } = await loadTemplateUiModule();
  const statuses = [];
  const select = createSelectStub();
  select.value = "tpl_1";
  global.document = {
    createElement: () => ({ value: "", textContent: "" }),
  };
  global.window = {
    localStorage: {
      getItem: () => null,
      setItem: () => {},
    },
    aiwfDesktop: {
      removeTemplatePack: async ({ id }) => {
        assert.equal(id, "pack_1");
        return { ok: true };
      },
      exportTemplatePack: async ({ id }) => {
        assert.equal(id, "pack_1");
        return { ok: true, path: "D:/exports/pack_1.json" };
      },
      listTemplateMarketplace: async () => ({ items: [] }),
    },
  };

  try {
    const ui = createWorkflowTemplateUi({
      templateSelect: select,
      templateParamsForm: null,
      templateParams: { value: "" },
    }, {
      builtinTemplates: [{ id: "tpl_1", name: "Finance Template", __pack_id: "pack_1" }],
      store: {},
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.removeTemplatePackByCurrentTemplate();
    select.value = "tpl_1";
    await ui.exportTemplatePackByCurrentTemplate();
  } finally {
    delete global.window;
    delete global.document;
  }

  assert.deepEqual(statuses, [
    { text: "模板包已移除: pack_1", ok: true },
    { text: "模板包已导出: D:/exports/pack_1.json", ok: true },
  ]);
});

test("workflow template ui guards template pack actions for non-pack templates", async () => {
  const { createWorkflowTemplateUi } = await loadTemplateUiModule();
  const statuses = [];
  global.window = {
    localStorage: {
      getItem: () => null,
      setItem: () => {},
    },
  };

  try {
    const ui = createWorkflowTemplateUi({
      templateSelect: { value: "tpl_local" },
      templateParamsForm: null,
      templateParams: { value: "" },
    }, {
      builtinTemplates: [{ id: "tpl_local", name: "Local Template" }],
      store: {},
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.removeTemplatePackByCurrentTemplate();
    await ui.exportTemplatePackByCurrentTemplate();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [
    { text: "当前模板不是模板包来源，无法移除", ok: false },
    { text: "当前模板不是模板包来源，无法导出", ok: false },
  ]);
});

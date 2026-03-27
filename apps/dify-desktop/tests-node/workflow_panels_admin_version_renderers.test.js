const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

class FakeElement {
  constructor(tagName = "div") {
    this.tagName = String(tagName).toUpperCase();
    this.children = [];
    this.style = {};
    this.className = "";
    this.textContent = "";
    this.value = "";
    this.innerHTML = "";
  }

  append(...nodes) {
    this.children.push(...nodes);
  }

  appendChild(node) {
    this.children.push(node);
  }
}

async function loadModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui-admin-version-renderers.js")).href;
  return import(file);
}

test("workflow admin version renderers surface apply-graph failure on restore", async () => {
  const { createWorkflowPanelsAdminVersionRenderers } = await loadModule();
  const statuses = [];
  const prevDocument = global.document;
  const prevWindow = global.window;
  global.document = {
    createElement(tagName) {
      return new FakeElement(tagName);
    },
  };
  global.window = {
    aiwfDesktop: {
      restoreWorkflowVersion: async () => ({
        ok: true,
        graph: { workflow_id: "wf_bad", version: "1.0.0", nodes: [{ id: "n1", type: "unknown_future_node" }], edges: [] },
      }),
      __applyRestoredWorkflowGraph() {
        const error = new Error("workflow contract invalid: workflow contains unregistered node types: unknown_future_node");
        error.code = "workflow_contract_invalid";
        error.details = {
          errors: ["workflow contains unregistered node types: unknown_future_node"],
          error_items: [{ path: "workflow.nodes", code: "unknown_node_type", message: "workflow contains unregistered node types: unknown_future_node" }],
        };
        throw error;
      },
    },
  };

  try {
    const els = {
      versionRows: new FakeElement("tbody"),
    };
    const renderers = createWorkflowPanelsAdminVersionRenderers(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });

    renderers.renderVersionRows([{ version_id: "ver_bad", workflow_name: "Bad Flow", ts: "2026-03-26T00:00:00Z" }]);
    const restoreBtn = els.versionRows.children[0].children[2].children[0];
    await restoreBtn.onclick();

    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].ok, false);
    assert.match(statuses[0].text, /恢复失败/);
    assert.match(statuses[0].text, /unknown_node_type/);
  } finally {
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
  }
});

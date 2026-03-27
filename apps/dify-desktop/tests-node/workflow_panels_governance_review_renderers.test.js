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
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui-governance-review-renderers.js")).href;
  return import(file);
}

test("workflow governance review renderers surface partial success when resume fails", async () => {
  const { createWorkflowPanelsGovernanceReviewRenderers } = await loadModule();
  const statuses = [];
  const prevDocument = global.document;
  const prevWindow = global.window;
  const prevPrompt = global.prompt;
  global.document = {
    createElement(tagName) {
      return new FakeElement(tagName);
    },
  };
  global.prompt = () => "";
  global.window = {
    aiwfDesktop: {
      submitManualReview: async () => ({
        ok: false,
        review_saved: true,
        item: { approved: true },
        resumed: {
          ok: false,
          error: "workflow contract invalid: workflow.version is required",
          error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
        },
      }),
    },
  };

  try {
    const els = {
      reviewRows: new FakeElement("tbody"),
      log: { textContent: "" },
    };
    const renderers = createWorkflowPanelsGovernanceReviewRenderers(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      refreshRunHistory: async () => {},
      refreshReviewQueue: async () => {},
      refreshReviewHistory: async () => {},
    });

    renderers.renderReviewRows([{ run_id: "run_1", review_key: "gate_a", status: "pending", reviewer: "reviewer" }]);
    const approveBtn = els.reviewRows.children[0].children[2].children[0];
    await approveBtn.onclick();

    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].ok, false);
    assert.match(statuses[0].text, /审核已批准，但自动续跑失败/);
    assert.match(statuses[0].text, /\[required\] workflow\.version/);
  } finally {
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
    if (typeof prevPrompt === "undefined") delete global.prompt;
    else global.prompt = prevPrompt;
  }
});

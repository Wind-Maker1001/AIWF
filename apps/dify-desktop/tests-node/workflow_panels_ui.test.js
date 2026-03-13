const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelsUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui.js")).href;
  return import(file);
}

class FakeElement {
  constructor(tagName) {
    this.tagName = String(tagName || "").toUpperCase();
    this.children = [];
    this.style = {};
    this.className = "";
    this.textContent = "";
    this.value = "";
    this.onclick = null;
    this._innerHTML = "";
  }

  append(...nodes) {
    this.children.push(...nodes);
  }

  appendChild(node) {
    this.children.push(node);
  }

  set innerHTML(value) {
    this._innerHTML = String(value || "");
    this.children = [];
  }

  get innerHTML() {
    return this._innerHTML;
  }
}

async function withFakeDocument(fn) {
  const prevDocument = global.document;
  global.document = {
    createElement(tag) {
      return new FakeElement(tag);
    },
  };
  try {
    await fn();
  } finally {
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
  }
}

test("workflow panels ui colors run history statuses", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  await withFakeDocument(async () => {
    const runHistoryRows = new FakeElement("tbody");
    const ui = createWorkflowPanelsUi({ runHistoryRows });
    ui.renderRunHistoryRows([
      { run_id: "run_ok", status: "passed", result: { node_runs: [] } },
      { run_id: "run_review", status: "pending_review", result: { node_runs: [] } },
      { run_id: "run_blocked", status: "quality_blocked", result: { node_runs: [] } },
    ]);

    assert.equal(runHistoryRows.children.length, 3);
    assert.equal(runHistoryRows.children[0].children[1].style.color, "#087443");
    assert.equal(runHistoryRows.children[1].children[1].style.color, "#b54708");
    assert.equal(runHistoryRows.children[2].children[1].style.color, "#b54708");
  });
});

test("workflow panels ui colors queue statuses", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  await withFakeDocument(async () => {
    const queueRows = new FakeElement("tbody");
    const ui = createWorkflowPanelsUi({ queueRows });
    ui.renderQueueRows([
      { task_id: "task_done", label: "done", status: "done" },
      { task_id: "task_running", label: "running", status: "running" },
      { task_id: "task_cancel", label: "cancel", status: "canceled" },
      { task_id: "task_fail", label: "fail", status: "failed" },
    ]);

    assert.equal(queueRows.children.length, 4);
    assert.equal(queueRows.children[0].children[1].style.color, "#087443");
    assert.equal(queueRows.children[1].children[1].style.color, "#1d4ed8");
    assert.equal(queueRows.children[2].children[1].style.color, "#5c6b7a");
    assert.equal(queueRows.children[3].children[1].style.color, "#b42318");
  });
});

test("workflow panels ui routes pending_review runs to review queue instead of replay", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  await withFakeDocument(async () => {
    const runHistoryRows = new FakeElement("tbody");
    const statuses = [];
    let reviewQueueOpens = 0;
    const ui = createWorkflowPanelsUi({ runHistoryRows }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      showReviewQueue: async () => { reviewQueueOpens += 1; },
    });
    ui.renderRunHistoryRows([
      { run_id: "run_review", status: "pending_review", result: { node_runs: [] } },
    ]);

    const row = runHistoryRows.children[0];
    const opCell = row.children[2];
    const select = opCell.children[0];
    const retryFailedBtn = opCell.children[3];
    const resumeBtn = opCell.children[4];
    assert.equal(select.disabled, true);
    assert.equal(retryFailedBtn.disabled, true);
    assert.equal(resumeBtn.disabled, false);
    await resumeBtn.onclick();
    assert.equal(reviewQueueOpens, 1);
    assert.equal(statuses.length, 1);
  });
});

test("workflow panels ui routes quality_blocked runs to quality gate instead of replay", async () => {
  const { createWorkflowPanelsUi } = await loadPanelsUiModule();
  await withFakeDocument(async () => {
    const runHistoryRows = new FakeElement("tbody");
    const statuses = [];
    let qualityGateOpens = 0;
    const ui = createWorkflowPanelsUi({ runHistoryRows }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      showQualityGate: async () => { qualityGateOpens += 1; },
    });
    ui.renderRunHistoryRows([
      { run_id: "run_blocked", status: "quality_blocked", result: { node_runs: [] } },
    ]);

    const row = runHistoryRows.children[0];
    const opCell = row.children[2];
    const select = opCell.children[0];
    const retryFailedBtn = opCell.children[3];
    const actionBtn = opCell.children[4];
    assert.equal(select.disabled, true);
    assert.equal(retryFailedBtn.disabled, true);
    assert.equal(actionBtn.disabled, false);
    await actionBtn.onclick();
    assert.equal(qualityGateOpens, 1);
    assert.equal(statuses.length, 1);
  });
});

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadDebugApiUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/debug-api-ui.js")).href;
  return import(file);
}

test("workflow debug api ui installs debug helpers when debug=1", async () => {
  const { setupWorkflowDebugApi } = await loadDebugApiUiModule();
  const calls = [];
  const state = { graph: { nodes: [], edges: [] }, selected: [] };
  const enabled = setupWorkflowDebugApi({
    location: { search: "?debug=1" },
  }, {
    store: {
      hasEdge: (from, to) => from === "n1" && to === "n2",
      unlink: (from, to) => calls.push({ unlink: [from, to] }),
      linkToFrom: (from, to) => {
        calls.push({ link: [from, to] });
        return { ok: true };
      },
      exportGraph: () => state.graph,
      importGraph: (graph) => { state.graph = graph; },
    },
    canvas: {
      getRouteMetrics: () => ({ edges: 3 }),
      setSelectedIds: (ids) => { state.selected = ids; },
      getSelectedIds: () => state.selected,
    },
    renderAll: () => calls.push("render"),
  });

  assert.equal(enabled, true);
  assert.equal(typeof globalThis.Object.getPrototypeOf, "function");
});

test("workflow debug api ui exposes working methods and disables without debug flag", async () => {
  const { setupWorkflowDebugApi } = await loadDebugApiUiModule();
  const calls = [];
  const win = { location: { search: "?debug=1" } };
  const state = { graph: { nodes: [{ id: "n1" }], edges: [] }, selected: [] };
  setupWorkflowDebugApi(win, {
    store: {
      hasEdge: () => false,
      unlink: () => {},
      linkToFrom: () => ({ ok: true }),
      exportGraph: () => state.graph,
      importGraph: (graph) => { state.graph = graph; calls.push("import"); },
    },
    canvas: {
      getRouteMetrics: () => ({ edges: 1 }),
      setSelectedIds: (ids) => { state.selected = ids; calls.push("select"); },
      getSelectedIds: () => state.selected,
    },
    renderAll: () => calls.push("render"),
  });

  assert.deepEqual(win.__aiwfDebug.graph(), { nodes: [{ id: "n1" }], edges: [] });
  assert.deepEqual(win.__aiwfDebug.routeStats(), { edges: 1 });
  assert.deepEqual(win.__aiwfDebug.selectNodes(["n1"]), ["n1"]);
  assert.deepEqual(win.__aiwfDebug.setGraph({ nodes: [{ id: "n2" }], edges: [] }), { nodes: [{ id: "n2" }], edges: [] });
  assert.deepEqual(win.__aiwfDebug.tryLink("n1", "n2"), { ok: true });

  const winNoDebug = { location: { search: "" }, __aiwfDebug: { old: true } };
  const disabled = setupWorkflowDebugApi(winNoDebug, {
    store: { hasEdge: () => false, unlink: () => {}, linkToFrom: () => ({ ok: true }), exportGraph: () => ({}), importGraph: () => {} },
    canvas: { getRouteMetrics: () => ({}), setSelectedIds: () => {}, getSelectedIds: () => [] },
  });
  assert.equal(disabled, false);
  assert.equal(Object.prototype.hasOwnProperty.call(winNoDebug, "__aiwfDebug"), false);
  assert.deepEqual(calls, ["select", "render", "import", "render", "render"]);
});

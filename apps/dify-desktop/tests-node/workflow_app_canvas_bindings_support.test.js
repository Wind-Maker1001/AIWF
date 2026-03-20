const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadCanvasSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-canvas-bindings-support.js")).href;
  return import(file);
}

test("workflow canvas binding support appends input and output map entries", async () => {
  const { createUpdatedNodeMapConfig } = await loadCanvasSupportModule();
  const node = {
    id: "n1",
    config: {
      input_map: { target_1: "$prev.value" },
      output_map: { alias_1: "value" },
    },
  };

  assert.deepEqual(createUpdatedNodeMapConfig(node, "input"), {
    input_map: { target_1: "$prev.value", target_2: "$prev.ok" },
    output_map: { alias_1: "value" },
  });

  assert.deepEqual(createUpdatedNodeMapConfig(node, "output"), {
    input_map: { target_1: "$prev.value" },
    output_map: { alias_1: "value", alias_2: "ok" },
  });
});

test("workflow canvas binding support unlinks edges between selected nodes", async () => {
  const { unlinkSelectedGraphEdges } = await loadCanvasSupportModule();
  const out = unlinkSelectedGraphEdges([
    { from: "a", to: "b" },
    { from: "b", to: "c" },
    { from: "x", to: "a" },
  ], ["a", "b", "c"]);

  assert.equal(out.removed, 2);
  assert.deepEqual(out.edges, [{ from: "x", to: "a" }]);
});

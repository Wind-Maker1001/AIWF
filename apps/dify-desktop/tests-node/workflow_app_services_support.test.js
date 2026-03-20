const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppServicesSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-services-support.js")).href;
  return import(file);
}

test("workflow app services support computes snapped and raw drop positions", async () => {
  const { computeCanvasDropPosition } = await loadAppServicesSupportModule();
  const canvas = {
    clientToWorld: (x, y) => ({ x: x + 10, y: y + 20 }),
  };

  assert.deepEqual(
    computeCanvasDropPosition({ canvas, evt: { clientX: 100, clientY: 80 }, snapEnabled: false }),
    { x: 5, y: 57 }
  );

  assert.deepEqual(
    computeCanvasDropPosition({ canvas, evt: { clientX: 100, clientY: 80 }, snapEnabled: true }),
    { x: 0, y: 48 }
  );
});

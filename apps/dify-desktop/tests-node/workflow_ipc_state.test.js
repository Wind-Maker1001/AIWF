const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const { createWorkflowIpcStateSupport } = require("../workflow_ipc_state");

function makeSupport(options = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-workflow-ipc-state-"));
  const paths = {
    userData: path.join(root, "userData"),
    documents: path.join(root, "documents"),
    desktop: path.join(root, "desktop"),
  };
  Object.values(paths).forEach((dir) => fs.mkdirSync(dir, { recursive: true }));
  const loadConfig = options.loadConfig || (() => ({ outputRoot: path.join(root, "output") }));
  fs.mkdirSync(loadConfig().outputRoot, { recursive: true });
  const support = createWorkflowIpcStateSupport({
    app: {
      isPackaged: !!options.isPackaged,
      getPath(name) {
        return paths[name];
      },
    },
    fs,
    path,
    loadConfig,
    nowIso: () => "2026-03-13T22:50:00.000Z",
  });
  return { root, support, paths };
}

test("workflow ipc state resolves mock file paths inside allowed roots", () => {
  const { support, paths } = makeSupport();
  const allowed = path.join(paths.documents, "reports", "flow.json");
  fs.mkdirSync(path.dirname(allowed), { recursive: true });

  assert.deepEqual(
    support.resolveMockFilePath("relative.json"),
    { ok: false, error: "mock_path_must_be_absolute" }
  );

  const allowedResult = support.resolveMockFilePath(allowed);
  assert.equal(allowedResult.ok, true);
  assert.equal(allowedResult.path, path.resolve(allowed));

  const blocked = support.resolveMockFilePath(path.join("C:\\", "outside", "flow.json"));
  assert.equal(blocked.ok, false);
  assert.equal(blocked.error, "mock_path_not_allowed");
});

test("workflow ipc state normalizes queue control quotas into safe bounds", () => {
  const { support } = makeSupport();
  const normalized = support.normalizeQueueControl({
    paused: 1,
    quotas: {
      alpha: 0,
      beta: 2.7,
      gamma: 99,
      delta: "3",
    },
  });

  assert.deepEqual(normalized, {
    paused: true,
    quotas: {
      beta: 2,
      gamma: 8,
      delta: 3,
    },
  });
});

test("workflow ipc state node cache tracks hits misses and sets", () => {
  const { support } = makeSupport();
  const cache = support.createNodeCacheApi();

  cache.set("n1", { ok: true, score: 7 });
  assert.deepEqual(cache.get("n1"), { ok: true, score: 7 });
  assert.equal(cache.get("missing"), null);

  const stats = support.cacheStats();
  assert.equal(stats.entries, 1);
  assert.equal(stats.hits, 1);
  assert.equal(stats.misses, 1);
  assert.equal(stats.sets, 1);
  assert.equal(stats.hit_rate, 0.5);

  support.clearNodeCache();
  const cleared = support.cacheStats();
  assert.equal(cleared.entries, 0);
  assert.equal(cleared.hits, 0);
  assert.equal(cleared.misses, 0);
  assert.equal(cleared.sets, 0);
  assert.equal(cleared.last_reset_at, "2026-03-13T22:50:00.000Z");
});

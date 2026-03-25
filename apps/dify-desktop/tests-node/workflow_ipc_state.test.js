const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
  TEMPLATE_MARKETPLACE_SCHEMA_VERSION,
  WORKFLOW_NODE_CACHE_METRICS_SCHEMA_VERSION,
  WORKFLOW_NODE_CACHE_SCHEMA_VERSION,
  WORKFLOW_QUEUE_CONTROL_SCHEMA_VERSION,
  WORKFLOW_TASK_QUEUE_SCHEMA_VERSION,
  createWorkflowIpcStateSupport,
} = require("../workflow_ipc_state");

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

test("workflow ipc state persists versioned workflow_store containers and reads legacy payloads", () => {
  const { support } = makeSupport();

  support.saveWorkflowQueue([{ run_id: "run_1" }]);
  const queueJson = JSON.parse(fs.readFileSync(support.workflowQueuePath(), "utf8"));
  assert.equal(queueJson.schema_version, WORKFLOW_TASK_QUEUE_SCHEMA_VERSION);
  assert.deepEqual(queueJson.items, [{ run_id: "run_1" }]);

  support.saveQueueControl({ paused: true, quotas: { alpha: 3 } });
  const controlJson = JSON.parse(fs.readFileSync(support.workflowQueueControlPath(), "utf8"));
  assert.equal(controlJson.schema_version, WORKFLOW_QUEUE_CONTROL_SCHEMA_VERSION);
  assert.equal(controlJson.paused, true);
  assert.deepEqual(controlJson.quotas, { alpha: 3 });

  support.saveTemplateMarketplace([{ id: "pack_1", name: "Pack One", templates: [] }]);
  const marketplaceJson = JSON.parse(fs.readFileSync(support.templateMarketplacePath(), "utf8"));
  assert.equal(marketplaceJson.schema_version, TEMPLATE_MARKETPLACE_SCHEMA_VERSION);
  assert.equal(marketplaceJson.items.length, 1);
  assert.equal(marketplaceJson.items[0].schema_version, TEMPLATE_PACK_ENTRY_SCHEMA_VERSION);

  fs.writeFileSync(support.workflowQueuePath(), `${JSON.stringify({ items: [{ run_id: "legacy_run" }] }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.workflowQueueControlPath(), `${JSON.stringify({ paused: false, quotas: { beta: 2 } }, null, 2)}\n`, "utf8");
  fs.writeFileSync(support.templateMarketplacePath(), `${JSON.stringify({ items: [{ id: "legacy_pack", name: "Legacy", templates: [] }] }, null, 2)}\n`, "utf8");

  assert.deepEqual(support.loadWorkflowQueue(), [{ run_id: "legacy_run" }]);
  assert.deepEqual(support.loadQueueControl(), { paused: false, quotas: { beta: 2 } });
  const marketplaceItems = support.listTemplateMarketplace(20);
  assert.equal(marketplaceItems.length, 1);
  assert.equal(marketplaceItems[0].schema_version, TEMPLATE_PACK_ENTRY_SCHEMA_VERSION);
  assert.equal(marketplaceItems[0].id, "legacy_pack");
  assert.equal(marketplaceItems[0].name, "Legacy");
  assert.equal(marketplaceItems[0].version, "v1");
  assert.equal(marketplaceItems[0].source, "unknown");
  assert.deepEqual(marketplaceItems[0].templates, []);
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
  const cacheJson = JSON.parse(fs.readFileSync(support.nodeCachePath(), "utf8"));
  const metricsJson = JSON.parse(fs.readFileSync(support.nodeCacheMetricsPath(), "utf8"));
  assert.equal(cacheJson.schema_version, WORKFLOW_NODE_CACHE_SCHEMA_VERSION);
  assert.equal(metricsJson.schema_version, WORKFLOW_NODE_CACHE_METRICS_SCHEMA_VERSION);

  support.clearNodeCache();
  const cleared = support.cacheStats();
  assert.equal(cleared.entries, 0);
  assert.equal(cleared.hits, 0);
  assert.equal(cleared.misses, 0);
  assert.equal(cleared.sets, 0);
  assert.equal(cleared.last_reset_at, "2026-03-13T22:50:00.000Z");
  const clearedCacheJson = JSON.parse(fs.readFileSync(support.nodeCachePath(), "utf8"));
  const clearedMetricsJson = JSON.parse(fs.readFileSync(support.nodeCacheMetricsPath(), "utf8"));
  assert.equal(clearedCacheJson.schema_version, WORKFLOW_NODE_CACHE_SCHEMA_VERSION);
  assert.equal(clearedMetricsJson.schema_version, WORKFLOW_NODE_CACHE_METRICS_SCHEMA_VERSION);
});

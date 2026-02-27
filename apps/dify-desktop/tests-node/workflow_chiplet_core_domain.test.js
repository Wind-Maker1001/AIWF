const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerCoreDomainChiplets } = require("../workflow_chiplets/domains/core_domain");

test("core domain registers expected chiplets", () => {
  const registry = new WorkflowChipletRegistry();
  registerCoreDomainChiplets(
    registry,
    {
      fs,
      runOfflineCleaning: async () => ({ job_id: "j1", artifacts: [], warnings: [] }),
      collectFiles: () => [],
      readArtifactById: () => "",
      computeViaRust: async () => ({ mode: "rust_http", metrics: {}, started: false }),
      runIsolatedTask: undefined,
    },
    {
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  assert.equal(registry.has("ingest_files"), true);
  assert.equal(registry.has("compute_rust"), true);
  assert.equal(registry.has("manual_review"), true);
});

test("core ingest_files collects payload files", async () => {
  const registry = new WorkflowChipletRegistry();
  registerCoreDomainChiplets(
    registry,
    {
      fs,
      runOfflineCleaning: async () => ({ job_id: "j1", artifacts: [], warnings: [] }),
      collectFiles: () => ["D:/in/a.pdf", "D:/in/b.docx"],
      readArtifactById: () => "",
      computeViaRust: async () => ({ mode: "rust_http", metrics: {}, started: false }),
      runIsolatedTask: undefined,
    },
    {
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  const out = await registry.resolve("ingest_files").run({ payload: {} });
  assert.deepEqual(out, { input_files: ["D:/in/a.pdf", "D:/in/b.docx"], count: 2 });
});

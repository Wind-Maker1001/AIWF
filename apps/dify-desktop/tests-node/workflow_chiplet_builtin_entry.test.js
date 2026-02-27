const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerBuiltinWorkflowChiplets } = require("../workflow_chiplets/builtin_chiplets");

function makeDeps(overrides = {}) {
  return {
    fs,
    path,
    runOfflineCleaning: async () => ({ job_id: "j1", artifacts: [], warnings: [] }),
    collectFiles: () => [],
    readArtifactById: () => "",
    summarizeCorpus: () => ({ sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0, sha256: "x" }),
    computeViaRust: async () => ({ mode: "rust_http", started: false, metrics: { sections: 0, bullets: 0, chars: 0, cjk: 0, latin: 0, sha256: "x" } }),
    callExternalAi: async () => ({ reason: "ok", text: "" }),
    auditAiText: () => ({ passed: true, reasons: [] }),
    writeWorkflowSummary: () => {},
    sha256Text: () => "sha",
    nodeOutputByType: () => null,
    runIsolatedTask: undefined,
    ...overrides,
  };
}

test("builtin chiplets entry delegates to domains and returns same registry", () => {
  const registry = new WorkflowChipletRegistry();
  const out = registerBuiltinWorkflowChiplets(registry, makeDeps());
  assert.equal(out, registry);
  assert.equal(registry.has("ingest_files"), true);
  assert.equal(registry.has("transform_rows_v3"), true);
  assert.equal(registry.has("md_output"), true);
  assert.equal(registry.has("ai_refine"), true);
});

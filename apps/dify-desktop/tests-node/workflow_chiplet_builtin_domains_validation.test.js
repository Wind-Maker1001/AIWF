const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { registerBuiltinWorkflowDomains } = require("../workflow_chiplets/domains/builtin_domains");

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

test("builtin domains register succeeds with valid deps", () => {
  const registry = new WorkflowChipletRegistry();
  assert.doesNotThrow(() => registerBuiltinWorkflowDomains(registry, makeDeps()));
});

test("builtin domains fail fast when required dep is missing", () => {
  const registry = new WorkflowChipletRegistry();
  const deps = makeDeps({ callExternalAi: undefined });
  assert.throws(
    () => registerBuiltinWorkflowDomains(registry, deps),
    /deps\.callExternalAi must be a function/,
  );
});

const assert = require("node:assert/strict");
const { registerAiDomainChiplets } = require("./ai_domain");
const { registerOutputDomainChiplets } = require("./output_domain");
const { registerCoreDomainChiplets } = require("./core_domain");
const { registerRustOpsDomainChiplets } = require("./rust_ops_domain");
const { createRuntimeSharedHelpers } = require("./runtime_shared");
const { createAiGuardrailsHelpers } = require("./ai_guardrails_policy");

function assertFunction(value, name) {
  assert.equal(typeof value, "function", `${name} must be a function`);
}

function assertObject(value, name) {
  assert.ok(value && typeof value === "object", `${name} must be an object`);
}

function assertBuiltinDomainDeps(registry, deps = {}) {
  assertObject(registry, "registry");
  assertFunction(registry.register, "registry.register");
  assertObject(deps.fs, "deps.fs");
  assertObject(deps.path, "deps.path");
  assertFunction(deps.collectFiles, "deps.collectFiles");
  assertFunction(deps.summarizeCorpus, "deps.summarizeCorpus");
  assertFunction(deps.computeViaRust, "deps.computeViaRust");
  assertFunction(deps.runOfflineCleaning, "deps.runOfflineCleaning");
  assertFunction(deps.readArtifactById, "deps.readArtifactById");
  assertFunction(deps.writeWorkflowSummary, "deps.writeWorkflowSummary");
  assertFunction(deps.sha256Text, "deps.sha256Text");
  assertFunction(deps.nodeOutputByType, "deps.nodeOutputByType");
  assertFunction(deps.callExternalAi, "deps.callExternalAi");
  assertFunction(deps.auditAiText, "deps.auditAiText");
}

function splitBuiltinDomainDeps(deps = {}) {
  const shared = {
    fs: deps.fs,
    path: deps.path,
    collectFiles: deps.collectFiles,
    summarizeCorpus: deps.summarizeCorpus,
    computeViaRust: deps.computeViaRust,
    runIsolatedTask: deps.runIsolatedTask,
  };
  return {
    shared,
    coreDeps: {
      fs: shared.fs,
      runOfflineCleaning: deps.runOfflineCleaning,
      collectFiles: shared.collectFiles,
      readArtifactById: deps.readArtifactById,
      computeViaRust: shared.computeViaRust,
      runIsolatedTask: shared.runIsolatedTask,
    },
    rustDeps: {
      runIsolatedTask: shared.runIsolatedTask,
    },
    outputDeps: {
      fs: shared.fs,
      path: shared.path,
      summarizeCorpus: shared.summarizeCorpus,
      writeWorkflowSummary: deps.writeWorkflowSummary,
      sha256Text: deps.sha256Text,
      nodeOutputByType: deps.nodeOutputByType,
    },
    aiDeps: {
      callExternalAi: deps.callExternalAi,
      auditAiText: deps.auditAiText,
      summarizeCorpus: shared.summarizeCorpus,
      computeViaRust: shared.computeViaRust,
      runIsolatedTask: shared.runIsolatedTask,
    },
  };
}

function registerBuiltinWorkflowDomains(registry, deps) {
  assertBuiltinDomainDeps(registry, deps);
  const {
    shared,
    coreDeps,
    rustDeps,
    outputDeps,
    aiDeps,
  } = splitBuiltinDomainDeps(deps);

  const {
    rustBase,
    rustRequired,
    resolveIsolationLevel,
    resolveSandboxLimits,
  } = createRuntimeSharedHelpers({ runIsolatedTask: shared.runIsolatedTask });
  const {
    shouldBlockAiOnData,
    enforceAiBudgetBeforeCall,
    recordAiBudgetAfterCall,
    extractNumericTokens,
    hasCitationMarkers,
    compareMetricCore,
  } = createAiGuardrailsHelpers({ collectFiles: shared.collectFiles });

  registerCoreDomainChiplets(registry, coreDeps, {
    resolveIsolationLevel,
    resolveSandboxLimits,
  });
  registerRustOpsDomainChiplets(
    registry,
    rustDeps,
    {
      rustBase,
      rustRequired,
      resolveIsolationLevel,
      resolveSandboxLimits,
    },
  );
  registerOutputDomainChiplets(registry, outputDeps);
  registerAiDomainChiplets(
    registry,
    aiDeps,
    {
      shouldBlockAiOnData,
      enforceAiBudgetBeforeCall,
      recordAiBudgetAfterCall,
      resolveIsolationLevel,
      resolveSandboxLimits,
      extractNumericTokens,
      hasCitationMarkers,
      compareMetricCore,
    },
  );
}

module.exports = {
  registerBuiltinWorkflowDomains,
};

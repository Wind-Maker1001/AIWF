import { createWorkflowQualityGateUi } from "./quality-gate-ui.js";
import { createWorkflowSandboxUi } from "./sandbox-ui.js";
import { createWorkflowAuditUi } from "./audit-ui.js";
import { createWorkflowVersionCacheUi } from "./version-cache-ui.js";
import { createWorkflowRunQueueUi } from "./run-queue-ui.js";
import { createWorkflowReviewQueueUi } from "./review-queue-ui.js";
import { createWorkflowQualityRuleSetUi } from "./quality-rule-set-ui.js";

function createWorkflowPanelServices(ctx = {}) {
  const {
    els,
    store,
    qualityGatePrefsKey,
    setStatus = () => {},
    qualityGateFilterPayload = () => ({}),
    qualityGatePrefsPayload = () => ({}),
    renderQualityGateRows = () => {},
    sandboxThresholdsPayload = () => ({}),
    sandboxDedupWindowSec = () => 0,
    sandboxRulesPayloadFromUi = () => ({}),
    applySandboxRulesToUi = () => {},
    applySandboxPresetToUi = () => {},
    currentSandboxPresetPayload = () => ({}),
    applySandboxPresetPayload = () => {},
    renderSandboxRows = () => {},
    renderSandboxRuleVersionRows = () => {},
    renderSandboxAutoFixRows = () => {},
    renderTimelineRows = () => {},
    renderFailureRows = () => {},
    renderAuditRows = () => {},
    renderRunHistoryRows = () => {},
    renderQueueRows = () => {},
    renderQueueControl = () => {},
    renderReviewRows = () => {},
    renderVersionRows = () => {},
    renderVersionCompare = () => {},
    renderCacheStats = () => {},
  } = ctx;

  const qualityGateUi = createWorkflowQualityGateUi(els, {
    setStatus,
    prefsStorageKey: qualityGatePrefsKey,
    qualityGatePrefsPayload,
    qualityGateFilterPayload,
    renderQualityGateRows,
  });

  const sandboxUi = createWorkflowSandboxUi(els, {
    setStatus,
    sandboxThresholdsPayload,
    sandboxDedupWindowSec,
    sandboxRulesPayloadFromUi,
    applySandboxRulesToUi,
    applySandboxPresetToUi,
    currentSandboxPresetPayload,
    applySandboxPresetPayload,
    renderSandboxRows,
    renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows,
  });

  const auditUi = createWorkflowAuditUi(els, {
    setStatus,
    renderTimelineRows,
    renderFailureRows,
    renderAuditRows,
  });

  const runQueueUi = createWorkflowRunQueueUi({
    setStatus,
    renderRunHistoryRows,
    renderQueueRows,
    renderQueueControl,
  });

  const reviewQueueUi = createWorkflowReviewQueueUi({
    renderReviewRows,
  });

  const versionCacheUi = createWorkflowVersionCacheUi(els, {
    setStatus,
    renderVersionRows,
    renderVersionCompare,
    renderCacheStats,
  });

  const qualityRuleSetUi = createWorkflowQualityRuleSetUi(els, {
    setStatus,
    exportGraph: () => store.exportGraph(),
  });

  return {
    ...qualityGateUi,
    ...sandboxUi,
    ...auditUi,
    ...runQueueUi,
    ...reviewQueueUi,
    ...versionCacheUi,
    ...qualityRuleSetUi,
  };
}

export { createWorkflowPanelServices };

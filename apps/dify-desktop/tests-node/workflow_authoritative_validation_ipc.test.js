const test = require("node:test");
const assert = require("node:assert/strict");

const { registerWorkflowRunIpc } = require("../workflow_ipc_run");
const { registerWorkflowReviewIpc } = require("../workflow_ipc_review");
const { createWorkflowStoreRemoteError } = require("../workflow_store_remote_error");

test("workflow run ipc fails closed when Rust-authoritative validation is unavailable", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      createWorkflowWindow: () => {},
      loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
      runMinimalWorkflow: async () => ({ ok: true, status: "done" }),
    },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => null,
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async () => {
          throw createWorkflowStoreRemoteError({
            ok: false,
            error: "workflow validation unavailable: connection refused",
            error_code: "workflow_validation_unavailable",
            validation_scope: "run",
          });
        },
      },
    },
  );

  const out = await handlers["aiwf:runWorkflow"]({}, {
    workflow: {
      workflow_id: "wf_fail_closed",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  }, {});

  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_validation_unavailable");
  assert.match(String(out.error || ""), /workflow validation unavailable/i);
});

test("workflow run ipc replays reference-backed runs via version lookup instead of stored workflow payload", async () => {
  const handlers = {};
  const runPayloads = [];
  registerWorkflowRunIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      createWorkflowWindow: () => {},
      loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
      runMinimalWorkflow: async ({ payload }) => {
        runPayloads.push(payload);
        return { ok: true, run_id: "run_replayed", status: "done", workflow_id: payload?.workflow?.workflow_id || "" };
      },
    },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => ({
        run_id: "run_ref_1",
        workflow_id: "wf_finance",
        run_request_kind: "reference",
        version_id: "ver_finance_001",
        published_version_id: "ver_finance_001",
        payload: {
          run_request_kind: "reference",
          version_id: "ver_finance_001",
          published_version_id: "ver_finance_001",
        },
        result: { node_outputs: { n1: { ok: true } } },
        config: {},
      }),
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowVersionStore: {
        getVersion: async (versionId) => ({
          version_id: versionId,
          workflow_definition: {
            workflow_id: "wf_finance",
            version: "workflow.v1",
            nodes: [{ id: "n1", type: "ingest_files" }],
            edges: [],
          },
        }),
      },
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
          ok: true,
          normalized_workflow_definition: workflowDefinition,
          notes: [],
        }),
      },
      workflowExecutionSupport: {
        executeReferenceWorkflowAuthoritatively: async ({ payload }) => {
          runPayloads.push(payload);
          return {
            ok: true,
            run_id: "run_replayed",
            status: "done",
            workflow_id: payload?.workflow?.workflow_id || "",
            node_runs: [{ id: "n1", type: "ingest_files", status: "done" }],
            node_outputs: {},
            artifacts: [],
            pending_reviews: [],
          };
        },
      },
    },
  );

  const out = await handlers["aiwf:replayWorkflowRun"]({}, { run_id: "run_ref_1", node_id: "n1" }, {});

  assert.equal(out.ok, true);
  assert.equal(runPayloads.length, 1);
  assert.equal(runPayloads[0].run_request_kind, "reference");
  assert.equal(runPayloads[0].version_id, "ver_finance_001");
  assert.equal(runPayloads[0].published_version_id, "ver_finance_001");
  assert.equal(runPayloads[0].workflow.workflow_id, "wf_finance");
});

test("workflow review ipc auto-resumes reference-backed runs via version lookup", async () => {
  const handlers = {};
  const runPayloads = [];
  registerWorkflowReviewIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      dialog: {},
      app: {},
      fs: {},
      path: {},
      loadConfig: () => ({}),
      runMinimalWorkflow: async ({ payload }) => {
        runPayloads.push(payload);
        return { ok: true, run_id: "run_resumed", status: "done", workflow_id: payload?.workflow?.workflow_id || "" };
      },
    },
    {
      isMockIoAllowed: () => true,
      resolveMockFilePath: () => ({ ok: true, path: "D:/tmp/out.json" }),
      getRun: async () => ({
        run_id: "run_ref_review_1",
        workflow_id: "wf_finance",
        run_request_kind: "reference",
        version_id: "ver_finance_001",
        published_version_id: "ver_finance_001",
        payload: {
          run_request_kind: "reference",
          version_id: "ver_finance_001",
          published_version_id: "ver_finance_001",
        },
        result: { node_outputs: { n1: { ok: true } } },
        config: {},
      }),
      normalizeWorkflowConfig: (cfg) => cfg,
      applyQualityRuleSetToPayload: async (payload) => payload,
      applySandboxAutoFixPayload: async (payload) => payload,
      attachQualityGate: (value) => value,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendSandboxViolationAudit: () => {},
      maybeApplySandboxAutoFix: async () => ({ ok: true }),
      enqueueReviews: async () => ({ ok: true }),
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowManualReviewStore: {
        listQueue: async () => ({ ok: true, items: [] }),
        listHistory: async () => ({ ok: true, items: [] }),
        submit: async () => ({ ok: true, item: { approved: true, reviewer: "reviewer", comment: "", node_id: "n1" }, remaining: 0 }),
      },
      workflowVersionStore: {
        getVersion: async (versionId) => ({
          version_id: versionId,
          workflow_definition: {
            workflow_id: "wf_finance",
            version: "workflow.v1",
            nodes: [{ id: "n1", type: "ingest_files" }],
            edges: [],
          },
        }),
      },
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
          ok: true,
          normalized_workflow_definition: workflowDefinition,
          notes: [],
        }),
      },
      workflowExecutionSupport: {
        executeReferenceWorkflowAuthoritatively: async ({ payload }) => {
          runPayloads.push(payload);
          return {
            ok: true,
            run_id: "run_resumed",
            status: "done",
            workflow_id: payload?.workflow?.workflow_id || "",
            node_runs: [{ id: "n1", type: "ingest_files", status: "done" }],
            node_outputs: {},
            artifacts: [],
            pending_reviews: [],
          };
        },
      },
    },
  );

  const out = await handlers["aiwf:submitManualReview"]({}, { run_id: "run_ref_review_1", review_key: "gate_a", approved: true, auto_resume: true }, {});

  assert.equal(out.ok, true);
  assert.equal(runPayloads.length, 1);
  assert.equal(runPayloads[0].run_request_kind, "reference");
  assert.equal(runPayloads[0].version_id, "ver_finance_001");
  assert.equal(runPayloads[0].published_version_id, "ver_finance_001");
  assert.equal(runPayloads[0].workflow.workflow_id, "wf_finance");
});

test("workflow run ipc prefers Rust draft execution surface over JS local engine", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      createWorkflowWindow: () => {},
      loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
      runMinimalWorkflow: async () => {
        throw new Error("js local engine should not be used");
      },
    },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => null,
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowExecutionSupport: {
        executeDraftWorkflowAuthoritatively: async ({ payload }) => ({
          ok: true,
          run_id: "run_rust_draft_1",
          status: "passed",
          workflow_id: payload?.workflow?.workflow_id || "",
          node_runs: [{ id: "n1", type: "ingest_files", status: "done" }],
          node_outputs: {},
          artifacts: [],
          pending_reviews: [],
        }),
      },
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
          ok: true,
          normalized_workflow_definition: workflowDefinition,
          notes: [],
        }),
      },
    },
  );

  const out = await handlers["aiwf:runWorkflow"]({}, {
    workflow: {
      workflow_id: "wf_rust_draft",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  }, {});

  assert.equal(out.ok, true);
  assert.equal(out.run_id, "run_rust_draft_1");
  assert.equal(out.node_runs[0].type, "ingest_files");
});

test("workflow run ipc replays draft runs through Rust draft execution surface", async () => {
  const handlers = {};
  registerWorkflowRunIpc(
    {
      ipcMain: { handle: (name, fn) => { handlers[name] = fn; } },
      createWorkflowWindow: () => {},
      loadConfig: () => ({ accelUrl: "http://127.0.0.1:18082" }),
      runMinimalWorkflow: async () => {
        throw new Error("js local engine should not be used for draft replay");
      },
    },
    {
      normalizeWorkflowConfig: (cfg) => cfg,
      resolveOutputRoot: () => "D:/tmp",
      createNodeCacheApi: () => ({}),
      appendDiagnostics: () => {},
      appendRunHistory: () => {},
      extractSandboxViolations: () => [],
      appendAudit: () => {},
      getRun: async () => ({
        run_id: "run_draft_1",
        run_request_kind: "draft",
        payload: {
          run_request_kind: "draft",
          workflow_definition_source: "draft_inline",
          workflow: {
            workflow_id: "wf_draft_replay",
            version: "1.0.0",
            nodes: [{ id: "n1", type: "ingest_files" }],
            edges: [],
          },
        },
        result: { node_outputs: {} },
        config: {},
      }),
      enqueueReviews: async () => ({ ok: true }),
      reportSupport: { applyQualityRuleSetToPayload: async (payload) => payload },
      sandboxSupport: { attachQualityGate: (value) => value, appendSandboxViolationAudit: () => {} },
      sandboxRuleStore: { getRuntimeRules: async () => ({ ok: true, rules: {} }) },
      sandboxAutoFixStore: { applyPayload: async (payload) => payload, processRunAutoFix: async () => ({ ok: true }) },
      workflowExecutionSupport: {
        executeDraftWorkflowAuthoritatively: async ({ payload }) => ({
          ok: true,
          run_id: "run_draft_replayed",
          status: "passed",
          workflow_id: payload?.workflow?.workflow_id || "",
          node_runs: [{ id: "n1", type: "ingest_files", status: "done" }],
          node_outputs: {},
          artifacts: [],
          pending_reviews: [],
        }),
      },
      workflowValidationSupport: {
        validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
          ok: true,
          normalized_workflow_definition: workflowDefinition,
          notes: [],
        }),
      },
    },
  );

  const out = await handlers["aiwf:replayWorkflowRun"]({}, { run_id: "run_draft_1", node_id: "n1" }, {});

  assert.equal(out.ok, true);
  assert.equal(out.run_id, "run_draft_replayed");
  assert.equal(out.run_request_kind, "draft");
});

function registerWorkflowRunIpc(ctx, deps) {
  const {
    ipcMain,
    createWorkflowWindow,
    loadConfig,
    runMinimalWorkflow,
  } = ctx;
  const {
    createWorkflowStoreRemoteError,
    workflowStoreRemoteErrorResult,
  } = require("./workflow_store_remote_error");
  const {
    normalizeWorkflowPayloadShape,
    resolveWorkflowDefinitionPayload,
  } = require("./workflow_graph");
  const {
    normalizeWorkflowConfig,
    resolveOutputRoot,
    createNodeCacheApi,
    appendDiagnostics,
    appendRunHistory,
    extractSandboxViolations,
    appendAudit,
    getRun,
    enqueueReviews,
    reportSupport,
    sandboxSupport,
    sandboxRuleStore,
    sandboxAutoFixStore,
    workflowVersionStore = {
      getVersion: async () => null,
    },
    workflowExecutionSupport = null,
    workflowValidationSupport = {
      validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
        ok: true,
        normalized_workflow_definition: workflowDefinition,
        notes: [],
      }),
    },
  } = deps;

  function applyPendingReviewEnqueueResult(out, enqueueOut) {
    if (!enqueueOut?.ok) {
      const reviewEnqueue = {
        ok: false,
        error: String(enqueueOut?.error || "manual review enqueue failed"),
        error_code: String(enqueueOut?.error_code || "manual_review_enqueue_failed"),
        error_item_contract: String(enqueueOut?.error_item_contract || ""),
        graph_contract: String(enqueueOut?.graph_contract || ""),
        error_items: Array.isArray(enqueueOut?.error_items) ? enqueueOut.error_items : [],
      };
      return {
        ...(out && typeof out === "object" ? out : {}),
        review_enqueue_failed: true,
        review_enqueue: reviewEnqueue,
      };
    }
    return out;
  }

  function normalizePendingReviews(items, out, payload = null) {
    const workflow = resolveWorkflowDefinitionPayload(payload) || {};
    const fallbackRunId = String(out?.run_id || "").trim();
    const fallbackWorkflowId = String(out?.workflow_id || workflow.workflow_id || "").trim();
    return (Array.isArray(items) ? items : []).map((item) => {
      const source = item && typeof item === "object" ? item : {};
      const reviewKey = String(source.review_key || source.node_id || "").trim();
      return {
        ...source,
        run_id: String(source.run_id || fallbackRunId).trim(),
        workflow_id: String(source.workflow_id || fallbackWorkflowId).trim(),
        node_id: String(source.node_id || reviewKey).trim(),
        review_key: reviewKey,
        status: String(source.status || "pending").trim().toLowerCase() || "pending",
      };
    });
  }

  async function validateWorkflowDefinition(graph, options = {}) {
    return await workflowValidationSupport.validateWorkflowDefinitionAuthoritatively({
      workflowDefinition: graph,
      cfg: options?.cfg || null,
      allowVersionMigration: options?.allowVersionMigration === true,
      requireNonEmptyNodes: options?.requireNonEmptyNodes === true,
      validationScope: String(options?.validationScope || "run"),
    });
  }

  async function executeWorkflowPayloadAuthoritatively(effectivePayload, merged) {
    const runRequestKind = String(effectivePayload?.run_request_kind || "draft").trim().toLowerCase();
    if (
      workflowExecutionSupport
      && typeof workflowExecutionSupport === "object"
      && (
        (runRequestKind === "reference" && typeof workflowExecutionSupport.executeReferenceWorkflowAuthoritatively === "function")
        || (runRequestKind !== "reference" && typeof workflowExecutionSupport.executeDraftWorkflowAuthoritatively === "function")
      )
    ) {
      try {
        if (runRequestKind === "reference") {
          return await workflowExecutionSupport.executeReferenceWorkflowAuthoritatively({
            payload: effectivePayload,
            cfg: merged,
          });
        }
        return await workflowExecutionSupport.executeDraftWorkflowAuthoritatively({
          payload: effectivePayload,
          cfg: merged,
        });
      }
      catch (error) {
        throw error;
      }
    }
    throw createWorkflowStoreRemoteError({
      ok: false,
      error: "workflow authoritative execution unavailable: accel-rust draft/reference execution surface is required",
      error_code: "workflow_authoritative_execution_unavailable",
      run_request_kind: runRequestKind || "draft",
    });
  }

  function normalizeDraftRunPayload(payload) {
    const source = normalizeWorkflowPayloadShape(payload);
    return {
      ...source,
      run_request_kind: "draft",
      workflow_definition_source: String(source.workflow_definition_source || "draft_inline"),
    };
  }

  async function buildReplayPayload(found, req, cfg) {
    const basePayload = found?.payload && typeof found.payload === "object" ? { ...found.payload } : {};
    const publishedVersionId = String(found?.published_version_id || basePayload.published_version_id || "").trim();
    const versionId = String(found?.version_id || basePayload.version_id || publishedVersionId).trim();
    const runRequestKind = String(found?.run_request_kind || basePayload.run_request_kind || "").trim().toLowerCase();
    const replayPayload = {
      ...basePayload,
      manual_review: req?.manual_review && typeof req.manual_review === "object"
        ? req.manual_review
        : (basePayload.manual_review || {}),
      resume: {
        run_id: String(req?.run_id || "").trim(),
        node_id: String(req?.node_id || "").trim(),
        outputs: found?.result?.node_outputs || {},
      },
    };
    if (versionId && (runRequestKind === "reference" || !resolveWorkflowDefinitionPayload(basePayload))) {
      const versionItem = await workflowVersionStore.getVersion(versionId, cfg);
      if (!versionItem?.workflow_definition || typeof versionItem.workflow_definition !== "object") {
        return {
          ok: false,
          error: `workflow version not found for replay: ${versionId}`,
          error_code: "workflow_run_version_missing",
          replay_of: String(req?.run_id || "").trim(),
          version_id: versionId,
          published_version_id: publishedVersionId,
        };
      }
      replayPayload.run_request_kind = "reference";
      replayPayload.workflow_definition_source = "version_reference";
      replayPayload.version_id = versionId;
      if (publishedVersionId) replayPayload.published_version_id = publishedVersionId;
      replayPayload.workflow_definition = versionItem.workflow_definition;
      delete replayPayload.workflow;
      return { ok: true, payload: replayPayload };
    }
    if (resolveWorkflowDefinitionPayload(replayPayload)) {
      replayPayload.run_request_kind = "draft";
      replayPayload.workflow_definition_source = String(replayPayload.workflow_definition_source || "draft_inline");
      return { ok: true, payload: normalizeWorkflowPayloadShape(replayPayload) };
    }
    return {
      ok: false,
      error: `workflow replay payload missing for run: ${String(req?.run_id || "").trim()}`,
      error_code: "workflow_replay_payload_missing",
      replay_of: String(req?.run_id || "").trim(),
    };
  }

  ipcMain.handle("aiwf:openWorkflowStudio", async () => {
    createWorkflowWindow();
    return { ok: true };
  });

  ipcMain.handle("aiwf:runWorkflow", async (_evt, payload, cfg) => {
    try {
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}) });
      const sourcePayload = payload && typeof payload === "object" ? payload : {};
      if (!resolveWorkflowDefinitionPayload(sourcePayload) && (sourcePayload.version_id || sourcePayload.published_version_id)) {
        return {
          ok: false,
          error: "reference-backed workflow run must use publication/version-backed run path",
          error_code: "workflow_reference_run_not_supported",
        };
      }
      const rulesOut = await sandboxRuleStore.getRuntimeRules(merged);
      if (!rulesOut?.ok) return rulesOut;
      let effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(normalizeDraftRunPayload(sourcePayload), merged),
        merged
      );
      effectivePayload = normalizeWorkflowPayloadShape(effectivePayload);
      effectivePayload.run_request_kind = "draft";
      effectivePayload.workflow_definition_source = String(effectivePayload.workflow_definition_source || "draft_inline");
      if (effectivePayload.workflow_definition && typeof effectivePayload.workflow_definition === "object") {
        const validated = await validateWorkflowDefinition(effectivePayload.workflow_definition, {
          cfg: merged,
          requireNonEmptyNodes: true,
          validationScope: "run",
        });
        effectivePayload = normalizeWorkflowPayloadShape(effectivePayload, validated.normalized_workflow_definition);
      }
      let out = sandboxSupport.attachQualityGate(
        await executeWorkflowPayloadAuthoritatively(effectivePayload, merged),
        effectivePayload || {}
      );
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), merged);
        out = applyPendingReviewEnqueueResult(out, enqueueOut);
      }
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, merged);
      extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
      if (out?.review_enqueue_failed) {
        appendAudit("run_workflow", {
          run_id: String(out?.run_id || ""),
          workflow_id: String(out?.workflow_id || ""),
          ok: !!out?.ok,
          status: String(out?.status || ""),
        });
        return out;
      }
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, merged);
      appendAudit("run_workflow", {
        run_id: String(out?.run_id || ""),
        workflow_id: String(out?.workflow_id || ""),
        ok: !!out?.ok,
        status: String(out?.status || ""),
      });
      return out;
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  });

  ipcMain.handle("aiwf:replayWorkflowRun", async (_evt, req, cfg) => {
    try {
      const runId = String(req?.run_id || "").trim();
      const nodeId = String(req?.node_id || "").trim();
      const merged = normalizeWorkflowConfig({ ...loadConfig(), ...(cfg || {}) });
      const found = await getRun(runId, merged);
      if (!found) return { ok: false, error: `run not found: ${runId}` };
      const replayMerged = normalizeWorkflowConfig({ ...loadConfig(), ...(found.config || {}), ...(cfg || {}) });
      const replayPayloadOut = await buildReplayPayload(found, req, replayMerged);
      if (!replayPayloadOut?.ok) return replayPayloadOut;
      const replayPayload = replayPayloadOut.payload;
      const rulesOut = await sandboxRuleStore.getRuntimeRules(replayMerged);
      if (!rulesOut?.ok) return rulesOut;
      let effectivePayload = await reportSupport.applyQualityRuleSetToPayload(
        await sandboxAutoFixStore.applyPayload(replayPayload, replayMerged),
        replayMerged
      );
      effectivePayload = normalizeWorkflowPayloadShape(effectivePayload);
      effectivePayload.run_request_kind = String(replayPayload.run_request_kind || effectivePayload.run_request_kind || "draft");
      effectivePayload.workflow_definition_source = String(
        replayPayload.workflow_definition_source
        || effectivePayload.workflow_definition_source
        || (effectivePayload.run_request_kind === "reference" ? "version_reference" : "draft_inline")
      );
      if (replayPayload.version_id) effectivePayload.version_id = String(replayPayload.version_id);
      if (replayPayload.published_version_id) effectivePayload.published_version_id = String(replayPayload.published_version_id);
      if (effectivePayload.workflow_definition && typeof effectivePayload.workflow_definition === "object") {
        const validated = await validateWorkflowDefinition(effectivePayload.workflow_definition, {
          cfg: replayMerged,
          requireNonEmptyNodes: true,
          validationScope: "run",
        });
        effectivePayload = normalizeWorkflowPayloadShape(effectivePayload, validated.normalized_workflow_definition);
      }
      let out = sandboxSupport.attachQualityGate(
        await executeWorkflowPayloadAuthoritatively(effectivePayload, replayMerged),
        effectivePayload || {}
      );
      if (Array.isArray(out?.pending_reviews) && out.pending_reviews.length) {
        const enqueueOut = await enqueueReviews(normalizePendingReviews(out.pending_reviews, out, effectivePayload), replayMerged);
        out = applyPendingReviewEnqueueResult(out, enqueueOut);
      }
      appendDiagnostics(out);
      appendRunHistory(out, effectivePayload, replayMerged);
      extractSandboxViolations(out).forEach((item) => sandboxSupport.appendSandboxViolationAudit(item, effectivePayload || {}, rulesOut.rules || {}));
      if (out?.review_enqueue_failed) {
        return {
          ok: false,
          replay_of: runId,
          resumed_from: nodeId || null,
          run_request_kind: String(effectivePayload?.run_request_kind || ""),
          version_id: String(effectivePayload?.version_id || ""),
          published_version_id: String(effectivePayload?.published_version_id || ""),
          result: out,
          run_id: String(out?.run_id || ""),
          status: String(out?.status || ""),
          error: String(out?.error || ""),
          error_code: String(out?.error_code || ""),
          error_item_contract: String(out?.error_item_contract || ""),
          graph_contract: String(out?.graph_contract || ""),
          error_items: Array.isArray(out?.error_items) ? out.error_items : [],
          review_enqueue_failed: !!out?.review_enqueue_failed,
          review_enqueue: out?.review_enqueue && typeof out.review_enqueue === "object" ? out.review_enqueue : null,
        };
      }
      await sandboxAutoFixStore.processRunAutoFix(out, effectivePayload || {}, replayMerged);
      return {
        ok: !!out?.ok,
        replay_of: runId,
        resumed_from: nodeId || null,
        run_request_kind: String(effectivePayload?.run_request_kind || ""),
        version_id: String(effectivePayload?.version_id || ""),
        published_version_id: String(effectivePayload?.published_version_id || ""),
        result: out,
        run_id: String(out?.run_id || ""),
        status: String(out?.status || ""),
        error: String(out?.error || ""),
        error_code: String(out?.error_code || ""),
        error_item_contract: String(out?.error_item_contract || ""),
        graph_contract: String(out?.graph_contract || ""),
        error_items: Array.isArray(out?.error_items) ? out.error_items : [],
        review_enqueue_failed: !!out?.review_enqueue_failed,
        review_enqueue: out?.review_enqueue && typeof out.review_enqueue === "object" ? out.review_enqueue : null,
      };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  });
}

module.exports = {
  registerWorkflowRunIpc,
};

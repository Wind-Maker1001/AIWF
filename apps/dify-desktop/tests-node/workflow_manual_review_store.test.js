const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GLUE_PROVIDER,
  createWorkflowManualReviewStore,
} = require("../workflow_manual_review_store");
const {
  jsonResponse,
  governanceBoundaryResponse,
} = require("./governance_test_support");

test("workflow manual review store uses glue provider for backend-owned queue and history", async () => {
  const remote = {
    queue: [],
    history: [],
  };
  const store = createWorkflowManualReviewStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("manual_reviews", "/governance/manual-reviews");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/manual-reviews?limit=")) {
        return jsonResponse(200, { ok: true, items: remote.queue });
      }
      if (method === "POST" && url.endsWith("/governance/manual-reviews/enqueue")) {
        const body = JSON.parse(String(init.body || "{}"));
        remote.queue = body.items.map((item) => ({
          ...item,
          schema_version: "manual_review_item.v1",
          owner: "glue-python",
          source_of_truth: "glue-python.governance.manual_reviews",
          status: "pending",
        }));
        return jsonResponse(200, { ok: true, items: remote.queue });
      }
      if (method === "GET" && url.includes("/governance/manual-reviews/history?")) {
        return jsonResponse(200, { ok: true, items: remote.history });
      }
      if (method === "POST" && url.endsWith("/governance/manual-reviews/submit")) {
        const body = JSON.parse(String(init.body || "{}"));
        const hit = remote.queue.find((item) => item.run_id === body.run_id && item.review_key === body.review_key);
        if (!hit) return jsonResponse(400, { ok: false, error: "review task not found" });
        remote.queue = remote.queue.filter((item) => !(item.run_id === body.run_id && item.review_key === body.review_key));
        const item = {
          ...hit,
          reviewer: body.reviewer,
          comment: body.comment,
          approved: !!body.approved,
          status: body.approved ? "approved" : "rejected",
          decided_at: "2026-03-21T01:00:00Z",
        };
        remote.history = [item, ...remote.history];
        return jsonResponse(200, { ok: true, item, remaining: remote.queue.length });
      }
      return jsonResponse(500, { ok: false, error: `unexpected request: ${method} ${url}` });
    },
  });

  const enqueued = await store.enqueue([{
    run_id: "run_2",
    review_key: "gate_b",
    workflow_id: "wf_finance",
    node_id: "n8",
    created_at: "2026-03-21T00:00:00Z",
  }], { mode: "base_api" });
  assert.equal(enqueued.provider, GLUE_PROVIDER);

  const queue = await store.listQueue(100, { mode: "base_api" });
  assert.equal(queue.items.length, 1);
  assert.equal(queue.items[0].owner, "glue-python");

  const submitted = await store.submit({
    run_id: "run_2",
    review_key: "gate_b",
    approved: false,
    reviewer: "bob",
    comment: "reject",
  }, { mode: "base_api" });
  assert.equal(submitted.provider, GLUE_PROVIDER);
  assert.equal(submitted.item.status, "rejected");

  const history = await store.listHistory({ limit: 100, filter: { run_id: "run_2" } }, { mode: "base_api" });
  assert.equal(history.items.length, 1);
  assert.equal(history.items[0].reviewer, "bob");
});

test("workflow manual review store honors glue env override in offline_local mode", () => {
  const store = createWorkflowManualReviewStore({
    loadConfig: () => ({ mode: "offline_local" }),
    env: {
      AIWF_MANUAL_REVIEW_PROVIDER: "glue_http",
    },
  });

  assert.equal(store.resolveProvider({ mode: "offline_local" }), GLUE_PROVIDER);
});

test("workflow manual review store rejects unsupported provider override", async () => {
  const store = createWorkflowManualReviewStore({
    loadConfig: () => ({ mode: "offline_local" }),
  });

  const listed = await store.listQueue(100, {
    mode: "offline_local",
    manualReviewProvider: "unsupported_provider",
  });
  assert.equal(listed.ok, false);
  assert.match(String(listed.error || ""), /unsupported|provider/i);

  const out = await store.enqueue([{
    run_id: "run_1",
    review_key: "gate_a",
  }], {
    mode: "offline_local",
    manualReviewProvider: "unsupported_provider",
  });

  assert.equal(out.ok, false);
  assert.match(String(out.error || ""), /unsupported|provider/i);
});

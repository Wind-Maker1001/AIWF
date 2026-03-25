const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GOVERNANCE_CONTROL_PLANE_META_ROUTE,
  createGovernanceControlPlaneSupport,
  resolveGovernanceOwnedRoutePrefix,
} = require("../workflow_governance");

function jsonResponse(status, payload) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() {
      return JSON.stringify(payload);
    },
  };
}

test("governance route helper resolves direct and owned prefixes", async () => {
  const calls = [];
  const support = createGovernanceControlPlaneSupport({
    loadConfig: () => ({ glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url) => {
      calls.push(url);
      assert.equal(url, `http://127.0.0.1:18081${GOVERNANCE_CONTROL_PLANE_META_ROUTE}`);
      return jsonResponse(200, {
        ok: true,
        boundary: {
          governance_surfaces: [
            {
              capability: "workflow_run_audit",
              route_prefix: "/governance/workflow-runs",
              owned_route_prefixes: ["/governance/workflow-runs", "/governance/workflow-audit-events-v2"],
            },
          ],
        },
      });
    },
  });

  const runRoute = await support.resolveRoutePrefix("workflow_run_audit");
  const auditRoute = await support.resolveRoutePrefix("workflow_run_audit", {
    preferredOwnedPrefix: "/governance/workflow-audit-events",
  });

  assert.equal(runRoute, "/governance/workflow-runs");
  assert.equal(auditRoute, "/governance/workflow-audit-events-v2");
  assert.equal(calls.length, 1);
});

test("governance route helper rejects missing capability", async () => {
  const support = createGovernanceControlPlaneSupport({
    loadConfig: () => ({ glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async () => jsonResponse(200, {
      ok: true,
      boundary: {
        governance_surfaces: [],
      },
    }),
  });

  await assert.rejects(
    () => support.resolveRoutePrefix("manual_reviews"),
    /missing capability/i,
  );
});

test("resolveGovernanceOwnedRoutePrefix matches by exact or versioned leaf", () => {
  assert.equal(
    resolveGovernanceOwnedRoutePrefix(
      {
        route_prefix: "/governance/workflow-runs",
        owned_route_prefixes: ["/governance/workflow-runs", "/governance/workflow-audit-events-v2"],
      },
      "/governance/workflow-audit-events",
    ),
    "/governance/workflow-audit-events-v2",
  );
});

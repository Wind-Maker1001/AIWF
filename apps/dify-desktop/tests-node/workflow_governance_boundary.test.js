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
              capability: "workflow_sandbox_rules",
              route_prefix: "/governance/workflow-sandbox/rules",
              owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions-v2"],
            },
          ],
        },
      });
    },
  });

  const runRoute = await support.resolveRoutePrefix("workflow_sandbox_rules");
  const auditRoute = await support.resolveRoutePrefix("workflow_sandbox_rules", {
    preferredOwnedPrefix: "/governance/workflow-sandbox/rule-versions",
  });

  assert.equal(runRoute, "/governance/workflow-sandbox/rules");
  assert.equal(auditRoute, "/governance/workflow-sandbox/rule-versions-v2");
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
        route_prefix: "/governance/workflow-sandbox/rules",
        owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions-v2"],
      },
      "/governance/workflow-sandbox/rule-versions",
    ),
    "/governance/workflow-sandbox/rule-versions-v2",
  );
});

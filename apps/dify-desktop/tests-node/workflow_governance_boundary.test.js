const test = require("node:test");
const assert = require("node:assert/strict");

const {
  GOVERNANCE_CONTROL_PLANE_META_ROUTE,
  createGovernanceControlPlaneSupport,
} = require("../workflow_governance");
const {
  governanceCapabilityOwnsRoutePrefix,
  resolveGovernanceCapabilityRoutePrefix,
} = require("../workflow_governance_capabilities.generated.js");

function jsonResponse(status, payload) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() {
      return JSON.stringify(payload);
    },
  };
}

test("generated governance route helper resolves primary and owned prefixes exactly", () => {
  assert.equal(
    resolveGovernanceCapabilityRoutePrefix("workflow_sandbox_rules"),
    "/governance/workflow-sandbox/rules",
  );
  assert.equal(
    resolveGovernanceCapabilityRoutePrefix("workflow_sandbox_rules", "/governance/workflow-sandbox/rule-versions"),
    "/governance/workflow-sandbox/rule-versions",
  );
  assert.equal(
    governanceCapabilityOwnsRoutePrefix("workflow_sandbox_rules", "/governance/workflow-sandbox/rule-versions"),
    true,
  );
});

test("governance route helper resolves exact generated prefixes against boundary", async () => {
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
              owned_route_prefixes: ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
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
  assert.equal(auditRoute, "/governance/workflow-sandbox/rule-versions");
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

test("governance route helper rejects boundary route drift from generated authority", async () => {
  const support = createGovernanceControlPlaneSupport({
    loadConfig: () => ({ glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async () => jsonResponse(200, {
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
    }),
  });

  await assert.rejects(
    () => support.resolveRoutePrefix("workflow_sandbox_rules", {
      preferredOwnedPrefix: "/governance/workflow-sandbox/rule-versions",
    }),
    /route prefix drift/i,
  );
});

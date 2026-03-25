using System.Net;
using System.Text;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceBridgeClientTests
{
    [Fact]
    public async Task GetGovernanceControlPlaneBoundaryAsync_ParsesBoundaryMetadata()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/meta/control-plane", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "boundary": {
                    "schema_version": "governance_surface.v1",
                    "status": "effective_second_control_plane",
                    "control_plane_role": "governance_state",
                    "governance_state_control_plane_owner": "glue-python",
                    "job_lifecycle_control_plane_owner": "base-java",
                    "operator_semantics_authority_owner": "accel-rust",
                    "workflow_authoring_surface_owner": "dify-desktop",
                    "meta_route": "/governance/meta/control-plane",
                    "governance_surfaces": [
                      {
                        "capability": "manual_reviews",
                        "route_prefix": "/governance/manual-reviews",
                        "owned_route_prefixes": ["/governance/manual-reviews"],
                        "state_owner": "glue-python",
                        "control_plane_role": "governance_state",
                        "lifecycle_mutation_allowed": false
                      }
                    ]
                  }
                }
                """);
        }, autoBoundary: false));

        var client = new GovernanceBridgeClient(http);
        var boundary = await client.GetGovernanceControlPlaneBoundaryAsync("http://127.0.0.1:18081", "");

        Assert.Equal("governance_surface.v1", boundary.SchemaVersion);
        Assert.Equal("governance_state", boundary.ControlPlaneRole);
        Assert.Equal("glue-python", boundary.GovernanceStateControlPlaneOwner);
        Assert.Equal("base-java", boundary.JobLifecycleControlPlaneOwner);
        Assert.Single(boundary.GovernanceSurfaces);
        Assert.Equal("/governance/manual-reviews", boundary.GovernanceSurfaces[0].RoutePrefix);
    }

    [Fact]
    public async Task ListManualReviewsAsync_PrimesBoundaryAndUsesBoundaryRoutePrefix()
    {
        var callIndex = 0;
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            callIndex += 1;
            if (callIndex == 1)
            {
                Assert.Equal("http://127.0.0.1:18081/governance/meta/control-plane", request.RequestUri!.AbsoluteUri);
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "boundary": {
                        "schema_version": "governance_surface.v1",
                        "status": "effective_second_control_plane",
                        "control_plane_role": "governance_state",
                        "governance_state_control_plane_owner": "glue-python",
                        "job_lifecycle_control_plane_owner": "base-java",
                        "operator_semantics_authority_owner": "accel-rust",
                        "workflow_authoring_surface_owner": "dify-desktop",
                        "meta_route": "/governance/meta/control-plane",
                        "governance_surfaces": [
                          {
                            "capability": "manual_reviews",
                            "route_prefix": "/governance/manual-reviews-v2",
                            "owned_route_prefixes": ["/governance/manual-reviews-v2"],
                            "state_owner": "glue-python",
                            "control_plane_role": "governance_state",
                            "lifecycle_mutation_allowed": false
                          }
                        ]
                      }
                    }
                    """);
            }

            Assert.Equal("http://127.0.0.1:18081/governance/manual-reviews-v2?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "run_id": "run_1",
                      "review_key": "gate_a",
                      "workflow_id": "wf_finance",
                      "node_id": "n7",
                      "status": "pending"
                    }
                  ]
                }
                """);
        }, autoBoundary: false));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListManualReviewsAsync("http://127.0.0.1:18081", "");

        Assert.Single(items);
        Assert.Equal(2, callIndex);
    }

    [Fact]
    public async Task ListWorkflowAuditEventsAsync_PrimesBoundaryAndUsesOwnedRoutePrefix()
    {
        var callIndex = 0;
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            callIndex += 1;
            if (callIndex == 1)
            {
                Assert.Equal("http://127.0.0.1:18081/governance/meta/control-plane", request.RequestUri!.AbsoluteUri);
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "boundary": {
                        "schema_version": "governance_surface.v1",
                        "status": "effective_second_control_plane",
                        "control_plane_role": "governance_state",
                        "governance_state_control_plane_owner": "glue-python",
                        "job_lifecycle_control_plane_owner": "base-java",
                        "operator_semantics_authority_owner": "accel-rust",
                        "workflow_authoring_surface_owner": "dify-desktop",
                        "meta_route": "/governance/meta/control-plane",
                        "governance_surfaces": [
                          {
                            "capability": "workflow_run_audit",
                            "route_prefix": "/governance/workflow-runs",
                            "owned_route_prefixes": ["/governance/workflow-runs", "/governance/workflow-audit-events-v2"],
                            "state_owner": "glue-python",
                            "control_plane_role": "governance_state",
                            "lifecycle_mutation_allowed": false
                          }
                        ]
                      }
                    }
                    """);
            }

            Assert.Equal("http://127.0.0.1:18081/governance/workflow-audit-events-v2?limit=80&action=run_workflow", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "ts": "2026-03-21T00:00:00Z",
                      "action": "run_workflow",
                      "detail": {
                        "run_id": "run_1"
                      }
                    }
                  ]
                }
                """);
        }, autoBoundary: false));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowAuditEventsAsync("http://127.0.0.1:18081", "", action: "run_workflow");

        Assert.Single(items);
        Assert.Equal(2, callIndex);
    }

    [Fact]
    public async Task ListManualReviewsAsync_BuildsExpectedRequestAndParsesItems()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/manual-reviews?limit=120", request.RequestUri!.AbsoluteUri);
            Assert.Equal("token", request.Headers.GetValues("X-API-Key").Single());
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "run_id": "run_1",
                      "review_key": "gate_a",
                      "workflow_id": "wf_finance",
                      "node_id": "n7",
                      "status": "pending"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListManualReviewsAsync("http://127.0.0.1:18081", "token");

        var item = Assert.Single(items);
        Assert.Equal("run_1", item.RunId);
        Assert.Equal("gate_a", item.ReviewKey);
        Assert.Contains("wf_finance", item.DisplayText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ListManualReviewHistoryAsync_EncodesFilters()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Contains("run_id=run_1", request.RequestUri!.Query, StringComparison.Ordinal);
            Assert.Contains("reviewer=alice", request.RequestUri.Query, StringComparison.Ordinal);
            Assert.Contains("status=approved", request.RequestUri.Query, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """{"ok":true,"items":[]}""");
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListManualReviewHistoryAsync("http://127.0.0.1:18081", "", runId: "run_1", reviewer: "alice", status: "approved");

        Assert.Empty(items);
    }

    [Fact]
    public async Task SubmitManualReviewAsync_PostsDecisionPayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/manual-reviews/submit", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"run_id\":\"run_2\"", body, StringComparison.Ordinal);
            Assert.Contains("\"review_key\":\"gate_b\"", body, StringComparison.Ordinal);
            Assert.Contains("\"approved\":true", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "item": {
                    "run_id": "run_2",
                    "review_key": "gate_b",
                    "status": "approved",
                    "approved": true,
                    "reviewer": "alice"
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var item = await client.SubmitManualReviewAsync("http://127.0.0.1:18081", "", "run_2", "gate_b", true, "alice", "ok");

        Assert.Equal("approved", item.Status);
        Assert.True(item.Approved);
        Assert.Equal("alice", item.Reviewer);
    }

    [Fact]
    public async Task SubmitManualReviewAsync_ThrowsBackendError()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.BadRequest, """{"ok":false,"error":"review task not found"}""")));

        var client = new GovernanceBridgeClient(http);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.SubmitManualReviewAsync("http://127.0.0.1:18081", "", "missing", "gate_x", false, "alice", ""));

        Assert.Contains("review task not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListWorkflowRunsAsync_ParsesItems()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-runs?limit=80", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "run_id": "run_1",
                      "workflow_id": "wf_finance",
                      "status": "failed",
                      "ok": false,
                      "ts": "2026-03-21T00:00:00Z"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowRunsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("run_1", item.RunId);
        Assert.Equal("failed", item.Status);
    }

    [Fact]
    public async Task GetWorkflowRunTimelineAsync_ParsesEntries()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-runs/run_1/timeline", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "timeline": [
                    {
                      "node_id": "n1",
                      "type": "quality_check_v3",
                      "status": "failed",
                      "started_at": "2026-03-21T00:00:00Z",
                      "ended_at": "2026-03-21T00:00:01Z",
                      "seconds": 1.0
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.GetWorkflowRunTimelineAsync("http://127.0.0.1:18081", "", "run_1");

        var item = Assert.Single(items);
        Assert.Equal("n1", item.NodeId);
        Assert.Contains("quality_check_v3", item.DisplayText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetWorkflowFailureSummaryAsync_ParsesSummaryItems()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "by_node": {
                    "quality_check_v3": {
                      "failed": 2,
                      "samples": ["boom"]
                    }
                  }
                }
                """)));

        var client = new GovernanceBridgeClient(http);
        var items = await client.GetWorkflowFailureSummaryAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("quality_check_v3", item.NodeType);
        Assert.Equal(2, item.Failed);
        Assert.Contains("boom", item.Sample, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ListWorkflowAuditEventsAsync_ParsesEvents()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Contains("action=run_workflow", request.RequestUri!.Query, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "ts": "2026-03-21T00:00:00Z",
                      "action": "run_workflow",
                      "detail": {
                        "run_id": "run_1"
                      }
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowAuditEventsAsync("http://127.0.0.1:18081", "", action: "run_workflow");

        var item = Assert.Single(items);
        Assert.Equal("run_workflow", item.Action);
        Assert.Contains("run_1", item.DetailSummary, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ListQualityRuleSetsAsync_ParsesRuleSets()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "sets": [
                    {
                      "id": "finance_default",
                      "name": "Finance Default",
                      "version": "v1",
                      "scope": "workflow",
                      "rules": {
                        "required_columns": ["amount"]
                      }
                    }
                  ]
                }
                """)));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListQualityRuleSetsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("finance_default", item.Id);
        Assert.Contains("required_columns", item.RulesJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SaveQualityRuleSetAsync_PostsRulePayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/quality-rule-sets/finance_default", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"id\":\"finance_default\"", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "set": {
                    "id": "finance_default",
                    "name": "Finance Default",
                    "version": "v2",
                    "scope": "workflow",
                    "rules": {
                      "required_columns": ["amount"]
                    }
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var item = await client.SaveQualityRuleSetAsync(
            "http://127.0.0.1:18081",
            "",
            "finance_default",
            "Finance Default",
            "v2",
            "workflow",
            new System.Text.Json.Nodes.JsonObject
            {
                ["required_columns"] = new System.Text.Json.Nodes.JsonArray("amount")
            });

        Assert.Equal("v2", item.Version);
    }

    [Fact]
    public async Task SandboxGovernanceApis_ParseRulesAndVersions()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            var uri = request.RequestUri!.AbsoluteUri;
            if (uri.EndsWith("/governance/workflow-sandbox/rules", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "rules": {
                        "whitelist_codes": ["sandbox_limit_exceeded:output"]
                      }
                    }
                    """);
            }

            if (uri.Contains("/governance/workflow-sandbox/rule-versions?", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "items": [
                        {
                          "version_id": "ver_1",
                          "ts": "2026-03-22T00:00:00Z",
                          "meta": {
                            "reason": "set_rules"
                          },
                          "rules": {
                            "whitelist_codes": ["sandbox_limit_exceeded:output"]
                          }
                        }
                      ]
                    }
                    """);
            }

            if (uri.EndsWith("/governance/workflow-sandbox/rule-versions/ver_1/rollback", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true,"version_id":"ver_2"}""");
            }

            throw new InvalidOperationException($"unexpected uri: {uri}");
        }));

        var client = new GovernanceBridgeClient(http);
        var rules = await client.GetWorkflowSandboxRulesAsync("http://127.0.0.1:18081", "");
        var versions = await client.ListWorkflowSandboxRuleVersionsAsync("http://127.0.0.1:18081", "");
        var rollbackVersion = await client.RollbackWorkflowSandboxRuleVersionAsync("http://127.0.0.1:18081", "", "ver_1");

        Assert.Contains("sandbox_limit_exceeded:output", rules.ToJsonString(), StringComparison.Ordinal);
        Assert.Single(versions);
        Assert.Equal("set_rules", versions[0].Reason);
        Assert.Equal("ver_2", rollbackVersion);
    }

    [Fact]
    public async Task SandboxAutofixApis_ParseStateAndActions()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            var uri = request.RequestUri!.AbsoluteUri;
            if (uri.EndsWith("/governance/workflow-sandbox/autofix-state", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "state": {
                        "violation_events": [{"run_id":"run_1"}],
                        "forced_isolation_mode": "process",
                        "forced_until": "2026-03-22T01:00:00Z",
                        "last_actions": [{"ts":"2026-03-22T00:10:00Z","actions":["pause_queue"]}],
                        "green_streak": 2
                      }
                    }
                    """);
            }

            if (uri.Contains("/governance/workflow-sandbox/autofix-actions?", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "items": [
                        {
                          "ts": "2026-03-22T00:10:00Z",
                          "count": 3,
                          "actions": ["pause_queue","require_manual_review"]
                        }
                      ]
                    }
                    """);
            }

            throw new InvalidOperationException($"unexpected uri: {uri}");
        }));

        var client = new GovernanceBridgeClient(http);
        var state = await client.GetWorkflowSandboxAutoFixStateAsync("http://127.0.0.1:18081", "");
        var actions = await client.ListWorkflowSandboxAutoFixActionsAsync("http://127.0.0.1:18081", "");

        Assert.Equal("process", state.ForcedIsolationMode);
        Assert.Equal(2, state.GreenStreak);
        Assert.Single(state.ViolationEvents);
        Assert.Single(state.LastActions);
        var action = Assert.Single(actions);
        Assert.Equal(3, action.Count);
        Assert.Contains("pause_queue", action.ActionsText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SaveSandboxAutofixStateAsync_PostsFullState()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-sandbox/autofix-state", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"forced_isolation_mode\":\"process\"", body, StringComparison.Ordinal);
            Assert.Contains("\"forced_until\":\"2026-03-22T01:00:00Z\"", body, StringComparison.Ordinal);
            Assert.Contains("\"green_streak\":3", body, StringComparison.Ordinal);
            Assert.Contains("\"violation_events\":[{\"run_id\":\"run_1\"}]", body, StringComparison.Ordinal);
            Assert.Contains("\"last_actions\":[{\"ts\":\"2026-03-22T00:10:00Z\",\"actions\":[\"pause_queue\"]}]", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "state": {
                    "violation_events": [{"run_id":"run_1"}],
                    "forced_isolation_mode": "process",
                    "forced_until": "2026-03-22T01:00:00Z",
                    "last_actions": [{"ts":"2026-03-22T00:10:00Z","actions":["pause_queue"]}],
                    "green_streak": 3
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var state = await client.SaveWorkflowSandboxAutoFixStateAsync(
            "http://127.0.0.1:18081",
            "",
            new GovernanceSandboxAutoFixState(
                "process",
                "2026-03-22T01:00:00Z",
                3,
                new System.Text.Json.Nodes.JsonArray(
                    new System.Text.Json.Nodes.JsonObject { ["run_id"] = "run_1" }),
                new System.Text.Json.Nodes.JsonArray(
                    new System.Text.Json.Nodes.JsonObject
                    {
                        ["ts"] = "2026-03-22T00:10:00Z",
                        ["actions"] = new System.Text.Json.Nodes.JsonArray("pause_queue")
                    })));

        Assert.Equal("process", state.ForcedIsolationMode);
        Assert.Equal(3, state.GreenStreak);
        Assert.Single(state.LastActions);
    }

    [Fact]
    public async Task ListWorkflowRunsAsync_ParsesRecentRuns()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-runs?limit=80", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "run_id": "run_1",
                      "workflow_id": "wf_finance",
                      "status": "failed",
                      "ok": false,
                      "ts": "2026-03-21T00:00:00Z"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowRunsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("run_1", item.RunId);
        Assert.Equal("failed", item.Status);
    }

    [Fact]
    public async Task GetWorkflowRunTimelineAsync_ParsesTimelineRows()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-runs/run_1/timeline", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "timeline": [
                    {
                      "node_id": "n1",
                      "type": "quality_check_v3",
                      "status": "failed",
                      "started_at": "2026-03-21T00:00:00Z",
                      "ended_at": "2026-03-21T00:00:01Z",
                      "seconds": 1.0
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var rows = await client.GetWorkflowRunTimelineAsync("http://127.0.0.1:18081", "", "run_1");

        var row = Assert.Single(rows);
        Assert.Equal("n1", row.NodeId);
        Assert.Contains("quality_check_v3", row.DisplayText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetWorkflowFailureSummaryAsync_ParsesNodeFailures()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "by_node": {
                    "quality_check_v3": {
                      "failed": 2,
                      "samples": ["boom"]
                    }
                  }
                }
                """)));

        var client = new GovernanceBridgeClient(http);
        var rows = await client.GetWorkflowFailureSummaryAsync("http://127.0.0.1:18081", "");

        var row = Assert.Single(rows);
        Assert.Equal("quality_check_v3", row.NodeType);
        Assert.Equal(2, row.Failed);
    }

    [Fact]
    public async Task ListWorkflowAuditEventsAsync_ParsesAuditRows()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Contains("action=run_workflow", request.RequestUri!.Query, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "ts": "2026-03-21T00:00:00Z",
                      "action": "run_workflow",
                      "detail": {
                        "run_id": "run_1"
                      }
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var rows = await client.ListWorkflowAuditEventsAsync("http://127.0.0.1:18081", "", action: "run_workflow");

        var row = Assert.Single(rows);
        Assert.Equal("run_workflow", row.Action);
        Assert.Contains("run_1", row.DetailSummary, StringComparison.Ordinal);
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private static HttpResponseMessage DefaultGovernanceBoundaryResponse()
    {
        return Json(HttpStatusCode.OK, """
            {
              "ok": true,
              "boundary": {
                "schema_version": "governance_surface.v1",
                "status": "effective_second_control_plane",
                "control_plane_role": "governance_state",
                "governance_state_control_plane_owner": "glue-python",
                "job_lifecycle_control_plane_owner": "base-java",
                "operator_semantics_authority_owner": "accel-rust",
                "workflow_authoring_surface_owner": "dify-desktop",
                "meta_route": "/governance/meta/control-plane",
                "governance_surfaces": [
                  {
                    "capability": "quality_rule_sets",
                    "route_prefix": "/governance/quality-rule-sets",
                    "owned_route_prefixes": ["/governance/quality-rule-sets"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "workflow_sandbox_rules",
                    "route_prefix": "/governance/workflow-sandbox/rules",
                    "owned_route_prefixes": ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "workflow_sandbox_autofix",
                    "route_prefix": "/governance/workflow-sandbox/autofix-state",
                    "owned_route_prefixes": ["/governance/workflow-sandbox/autofix-state", "/governance/workflow-sandbox/autofix-actions"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "manual_reviews",
                    "route_prefix": "/governance/manual-reviews",
                    "owned_route_prefixes": ["/governance/manual-reviews"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "workflow_run_audit",
                    "route_prefix": "/governance/workflow-runs",
                    "owned_route_prefixes": ["/governance/workflow-runs", "/governance/workflow-audit-events"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  }
                ]
              }
            }
            """);
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _responder;
        private readonly bool _autoBoundary;

        public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder, bool autoBoundary = true)
        {
            _autoBoundary = autoBoundary;
            _responder = request => Task.FromResult(responder(request));
        }

        public StubHttpMessageHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder, bool autoBoundary = true)
        {
            _autoBoundary = autoBoundary;
            _responder = responder;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_autoBoundary && string.Equals(request.RequestUri?.AbsolutePath, "/governance/meta/control-plane", StringComparison.Ordinal))
            {
                return Task.FromResult(DefaultGovernanceBoundaryResponse());
            }
            return _responder(request);
        }
    }
}

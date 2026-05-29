using System.Net;
using System.Text;
using System.Text.Json.Nodes;
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
    public async Task ListManualReviewsAsync_PrimesBoundaryAndUsesGeneratedAuthorityRoutePrefix()
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
            }

            Assert.Equal("http://127.0.0.1:18081/governance/manual-reviews?limit=120", request.RequestUri!.AbsoluteUri);
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
    public async Task ListManualReviewsAsync_RejectsBoundaryRoutePrefixDrift()
    {
        var callIndex = 0;
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            callIndex += 1;
            if (callIndex == 1)
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

            throw new InvalidOperationException("unexpected manual review request after boundary drift");
        }, autoBoundary: false));

        var client = new GovernanceBridgeClient(http);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.ListManualReviewsAsync("http://127.0.0.1:18081", ""));

        Assert.Contains("route prefix drift", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListWorkflowAuditEventsAsync_UsesLifecycleAuditEndpoint()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/audit-events?limit=80&action=run_workflow", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "ts": "2026-03-21T00:00:00Z",
                    "action": "run_workflow",
                    "detail": {
                      "run_id": "run_1"
                    }
                  }
                ]
                """);
        }, autoBoundary: false));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowAuditEventsAsync("http://127.0.0.1:18081", "", action: "run_workflow");

        Assert.Single(items);
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
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/history?limit=80", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "run_id": "run_1",
                    "workflow_id": "wf_finance",
                    "status": "failed",
                    "ok": false,
                    "ts": "2026-03-21T00:00:00Z"
                  }
                ]
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowRunsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("run_1", item.RunId);
        Assert.Equal("failed", item.Status);
    }

    [Fact]
    public async Task ListWorkflowRunRecordsAsync_ParsesStepsAndDurations()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/history?limit=80", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "run_id": "run_1",
                    "workflow_id": "wf_finance",
                    "status": "failed",
                    "ok": false,
                    "ts": "2026-03-21T00:00:00Z",
                    "run_request_kind": "reference",
                    "version_id": "ver_1",
                    "published_version_id": "",
                    "workflow_definition_source": "version_reference",
                    "result": {
                      "steps": [
                        {
                          "step_id": "clean_md",
                          "status": "DONE",
                          "started_at": "2026-03-21T00:00:00Z",
                          "ended_at": "2026-03-21T00:00:01Z",
                          "error": ""
                        },
                        {
                          "step_id": "ai_refine",
                          "status": "FAILED",
                          "started_at": "2026-03-21T00:00:01Z",
                          "ended_at": "2026-03-21T00:00:03Z",
                          "error": "boom"
                        }
                      ]
                    }
                  }
                ]
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowRunRecordsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("reference", item.RunRequestKind);
        Assert.Equal("ver_1", item.VersionId);
        Assert.Equal(2, item.Steps.Count);
        Assert.Equal("clean_md", item.Steps[0].StepId);
        Assert.Equal(1, item.Steps[0].Seconds);
        Assert.Equal("boom", item.Steps[1].Error);
        Assert.Equal(2, item.Steps[1].Seconds);
    }

    [Fact]
    public async Task GetWorkflowRunTimelineAsync_ParsesEntries()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/run_1/timeline", request.RequestUri!.AbsoluteUri);
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
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/failure-summary?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "by_node": {
                    "quality_check_v3": {
                      "failed": 2,
                      "samples": ["boom"]
                    }
                  }
                }
                """);
        }));

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
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/audit-events?limit=80&action=run_workflow", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "ts": "2026-03-21T00:00:00Z",
                    "action": "run_workflow",
                    "detail": {
                      "run_id": "run_1"
                    }
                  }
                ]
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
    public async Task ListWorkflowVersionsAsync_UsesGeneratedWorkflowVersionsRoute()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-versions?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "version_id": "ver_a",
                      "workflow_name": "Workflow A",
                      "workflow_id": "wf_a",
                      "ts": "2026-05-19T10:00:00Z",
                      "provider": "glue-python",
                      "owner": "glue-python"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowVersionsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("ver_a", item.VersionId);
        Assert.Equal("Workflow A", item.WorkflowName);
        Assert.Equal("wf_a", item.WorkflowId);
    }

    [Fact]
    public async Task CompareWorkflowVersionsAsync_PostsBodyAndParsesSummaryAndNodeDiff()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-versions/compare", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"version_a\":\"ver_a\"", body, StringComparison.Ordinal);
            Assert.Contains("\"version_b\":\"ver_b\"", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "provider": "glue-python",
                  "summary": {
                    "version_a": "ver_a",
                    "version_b": "ver_b",
                    "changed_nodes": 1,
                    "added_edges": 1,
                    "removed_edges": 0
                  },
                  "node_diff": [
                    {
                      "id": "n1",
                      "change": "updated",
                      "type_a": "load_rows_v3",
                      "type_b": "load_rows_v3",
                      "config_changed": true
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var result = await client.CompareWorkflowVersionsAsync("http://127.0.0.1:18081", "", "ver_a", "ver_b");

        Assert.True(result.Ok);
        Assert.Equal("glue-python", result.Provider);
        Assert.Equal("ver_a", result.Summary.VersionA);
        Assert.Equal(1, result.Summary.ChangedNodes);
        var diff = Assert.Single(result.NodeDiff);
        Assert.Equal("n1", diff.NodeId);
        Assert.Equal("updated", diff.Change);
        Assert.True(diff.ConfigChanged);
        Assert.False(diff.StatusChanged);
    }

    [Fact]
    public async Task CompareWorkflowVersionsAsync_ThrowsStructuredFailure()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.BadRequest, """
                {
                  "ok": false,
                  "error": "version not found",
                  "error_code": "workflow_graph_invalid",
                  "error_items": [
                    {
                      "path": "request.version_a",
                      "code": "missing",
                      "message": "version not found"
                    }
                  ]
                }
                """)));

        var client = new GovernanceBridgeClient(http);
        var ex = await Assert.ThrowsAsync<GovernanceRequestFailureException>(() =>
            client.CompareWorkflowVersionsAsync("http://127.0.0.1:18081", "", "ver_x", "ver_y"));

        Assert.Equal("workflow_graph_invalid", ex.ErrorCode);
        Assert.Single(ex.ErrorItems);
        Assert.Equal("request.version_a", ex.ErrorItems[0].Path);
    }

    [Fact]
    public async Task ListWorkflowAppsAsync_UsesGeneratedWorkflowAppsRoute()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-apps?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "app_id": "finance_app",
                      "name": "Finance App",
                      "workflow_id": "wf_finance",
                      "published_version_id": "ver_finance_001",
                      "updated_at": "2026-05-23T10:00:00Z",
                      "provider": "glue-python",
                      "owner": "glue-python"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListWorkflowAppsAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("finance_app", item.AppId);
        Assert.Equal("ver_finance_001", item.PublishedVersionId);
    }

    [Fact]
    public async Task SaveWorkflowVersionAsync_PostsCanonicalVersionPayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-versions/ver_finance_001", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"version_id\":\"ver_finance_001\"", body, StringComparison.Ordinal);
            Assert.Contains("\"workflow_id\":\"wf_finance\"", body, StringComparison.Ordinal);
            Assert.Contains("\"workflow_name\":\"Finance Flow\"", body, StringComparison.Ordinal);
            Assert.Contains("\"workflow_definition\"", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "item": {
                    "version_id": "ver_finance_001",
                    "workflow_id": "wf_finance",
                    "workflow_name": "Finance Flow",
                    "ts": "2026-05-23T10:00:00Z"
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var saved = await client.SaveWorkflowVersionAsync(
            "http://127.0.0.1:18081",
            "",
            "ver_finance_001",
            "wf_finance",
            "Finance Flow",
            new JsonObject
            {
                ["workflow_id"] = "wf_finance",
                ["version"] = "1.0.0",
                ["nodes"] = new JsonArray(),
                ["edges"] = new JsonArray(),
            });

        Assert.Equal("ver_finance_001", saved.VersionId);
        Assert.Equal("wf_finance", saved.WorkflowId);
    }

    [Fact]
    public async Task SaveWorkflowAppAsync_PostsCanonicalPublishPayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/workflow-apps/finance_app", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"app_id\":\"finance_app\"", body, StringComparison.Ordinal);
            Assert.Contains("\"published_version_id\":\"ver_finance_001\"", body, StringComparison.Ordinal);
            Assert.Contains("\"params_schema\"", body, StringComparison.Ordinal);
            Assert.Contains("\"template_policy\"", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "item": {
                    "app_id": "finance_app",
                    "name": "Finance App",
                    "workflow_id": "wf_finance",
                    "published_version_id": "ver_finance_001",
                    "updated_at": "2026-05-23T10:00:00Z",
                    "provider": "glue-python",
                    "owner": "glue-python"
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var item = await client.SaveWorkflowAppAsync(
            "http://127.0.0.1:18081",
            "",
            "finance_app",
            "Finance App",
            "wf_finance",
            "ver_finance_001",
            new JsonObject { ["region"] = new JsonObject { ["type"] = "string" } },
            new JsonObject { ["version"] = 1 });

        Assert.Equal("finance_app", item.AppId);
        Assert.Equal("Finance App", item.Name);
    }

    [Fact]
    public async Task SaveWorkflowAppAsync_ThrowsStructuredFailure()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Json(HttpStatusCode.BadRequest, """
                {
                  "ok": false,
                  "error": "workflow app published_version_id not found: ver_missing",
                  "error_code": "governance_validation_invalid",
                  "error_items": [
                    {
                      "path": "published_version_id",
                      "code": "validation_error",
                      "message": "workflow app published_version_id not found: ver_missing"
                    }
                  ]
                }
                """)));

        var client = new GovernanceBridgeClient(http);
        var ex = await Assert.ThrowsAsync<GovernanceRequestFailureException>(() =>
            client.SaveWorkflowAppAsync(
                "http://127.0.0.1:18081",
                "",
                "finance_app",
                "Finance App",
                "wf_finance",
                "ver_missing",
                new JsonObject(),
                new JsonObject()));

        Assert.Equal("governance_validation_invalid", ex.ErrorCode);
        Assert.Single(ex.ErrorItems);
        Assert.Equal("published_version_id", ex.ErrorItems[0].Path);
    }

    [Fact]
    public async Task ListRunBaselinesAsync_UsesGeneratedRunBaselinesRoute()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/governance/run-baselines?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "provider": "glue-python",
                  "items": [
                    {
                      "baseline_id": "base_1",
                      "name": "Base One",
                      "run_id": "run_1",
                      "workflow_id": "wf_finance",
                      "created_at": "2026-05-29T00:00:00Z",
                      "notes": "seed",
                      "owner": "glue-python",
                      "source_of_truth": "glue-python.governance.run_baselines"
                    }
                  ]
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var items = await client.ListRunBaselinesAsync("http://127.0.0.1:18081", "");

        var item = Assert.Single(items);
        Assert.Equal("base_1", item.BaselineId);
        Assert.Equal("glue-python", item.Provider);
    }

    [Fact]
    public async Task SaveRunBaselineAsync_PostsCanonicalPayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("http://127.0.0.1:18081/governance/run-baselines/base_1", request.RequestUri!.AbsoluteUri);
            var body = await request.Content!.ReadAsStringAsync();
            Assert.Contains("\"baseline_id\":\"base_1\"", body, StringComparison.Ordinal);
            Assert.Contains("\"run_id\":\"run_1\"", body, StringComparison.Ordinal);
            Assert.Contains("\"workflow_id\":\"wf_finance\"", body, StringComparison.Ordinal);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "provider": "glue-python",
                  "item": {
                    "baseline_id": "base_1",
                    "name": "Base One",
                    "run_id": "run_1",
                    "workflow_id": "wf_finance",
                    "created_at": "2026-05-29T00:00:00Z",
                    "notes": "seed",
                    "owner": "glue-python",
                    "source_of_truth": "glue-python.governance.run_baselines"
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var item = await client.SaveRunBaselineAsync(
            "http://127.0.0.1:18081",
            "",
            "base_1",
            "Base One",
            "run_1",
            "wf_finance",
            "2026-05-29T00:00:00Z",
            "seed");

        Assert.Equal("base_1", item.BaselineId);
        Assert.Equal("run_1", item.RunId);
    }

    [Fact]
    public async Task GetWorkflowRunRecordAsync_ParsesDetailPayload()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/run_1/record", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "run_id": "run_1",
                  "workflow_id": "wf_finance",
                  "status": "failed",
                  "ok": false,
                  "ts": "2026-05-29T00:00:00Z",
                  "run_request_kind": "reference",
                  "version_id": "ver_1",
                  "published_version_id": "",
                  "workflow_definition_source": "version_reference",
                  "result": {
                    "lineage": {
                      "node_count": 2,
                      "edge_count": 1
                    },
                    "steps": [
                      {
                        "step_id": "clean_md",
                        "status": "DONE",
                        "started_at": "2026-05-29T00:00:00Z",
                        "ended_at": "2026-05-29T00:00:02Z",
                        "error": ""
                      }
                    ]
                  }
                }
                """);
        }));

        var client = new GovernanceBridgeClient(http);
        var item = await client.GetWorkflowRunRecordAsync("http://127.0.0.1:18081", "", "run_1");

        Assert.Equal("run_1", item.RunId);
        Assert.Equal("ver_1", item.VersionId);
        Assert.Single(item.Steps);
        Assert.Equal(2, item.Steps[0].Seconds);
        Assert.Equal(2, item.ResultPayload["lineage"]?["node_count"]?.GetValue<int>());
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
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/history?limit=80", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "run_id": "run_1",
                    "workflow_id": "wf_finance",
                    "status": "failed",
                    "ok": false,
                    "ts": "2026-03-21T00:00:00Z"
                  }
                ]
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
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/run_1/timeline", request.RequestUri!.AbsoluteUri);
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
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/failure-summary?limit=120", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "by_node": {
                    "quality_check_v3": {
                      "failed": 2,
                      "samples": ["boom"]
                    }
                  }
                }
                """);
        }));

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
            Assert.Equal("http://127.0.0.1:18081/api/v1/jobs/audit-events?limit=80&action=run_workflow", request.RequestUri!.AbsoluteUri);
            return Json(HttpStatusCode.OK, """
                [
                  {
                    "ts": "2026-03-21T00:00:00Z",
                    "action": "run_workflow",
                    "detail": {
                      "run_id": "run_1"
                    }
                  }
                ]
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
                    "capability": "workflow_versions",
                    "route_prefix": "/governance/workflow-versions",
                    "owned_route_prefixes": ["/governance/workflow-versions"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "workflow_apps",
                    "route_prefix": "/governance/workflow-apps",
                    "owned_route_prefixes": ["/governance/workflow-apps"],
                    "state_owner": "glue-python",
                    "control_plane_role": "governance_state",
                    "lifecycle_mutation_allowed": false
                  },
                  {
                    "capability": "run_baselines",
                    "route_prefix": "/governance/run-baselines",
                    "owned_route_prefixes": ["/governance/run-baselines"],
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

using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppPublishCoordinatorTests
{
    [Fact]
    public async Task PublishAsync_RunsPreflightAndPublishesVersionThenApp()
    {
        var calls = new List<string>();
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            var uri = request.RequestUri!.AbsoluteUri;
            calls.Add(uri);
            if (uri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
            {
                return BoundaryResponse();
            }
            if (uri.EndsWith("/operators/workflow_contract_v1/validate", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true,"valid":true,"status":"ok"}""");
            }
            if (uri.EndsWith("/operators/data_source_browser_v1", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true}""");
            }
            if (uri.Contains("/governance/workflow-versions/wf_finance_published_", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "item": {
                        "version_id": "wf_finance_published_20260523134000_deadbeef",
                        "workflow_id": "wf_finance",
                        "workflow_name": "Finance App",
                        "ts": "2026-05-23T13:40:00Z"
                      }
                    }
                    """);
            }
            if (uri.Contains("/governance/workflow-apps/finance_app_", StringComparison.Ordinal))
            {
                var body = await request.Content!.ReadAsStringAsync();
                Assert.Contains("\"published_version_id\":\"wf_finance_published_20260523134000_deadbeef\"", body, StringComparison.Ordinal);
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "item": {
                        "app_id": "finance_app_20260523134000_deadbeef",
                        "name": "Finance App",
                        "workflow_id": "wf_finance",
                        "published_version_id": "wf_finance_published_20260523134000_deadbeef",
                        "updated_at": "2026-05-23T13:40:00Z",
                        "provider": "glue-python",
                        "owner": "glue-python"
                      }
                    }
                    """);
            }
            if (uri.Contains("/governance/workflow-apps?limit=120", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "items": [
                        {
                          "app_id": "finance_app_20260523134000_deadbeef",
                          "name": "Finance App",
                          "workflow_id": "wf_finance",
                          "published_version_id": "wf_finance_published_20260523134000_deadbeef",
                          "updated_at": "2026-05-23T13:40:00Z",
                          "provider": "glue-python",
                          "owner": "glue-python"
                        }
                      ]
                    }
                    """);
            }
            return Json(HttpStatusCode.NotFound, """{"error":"unexpected"}""");
        }));

        var preflight = new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http), () => "2026-05-23T13:40:00Z");
        var coordinator = new WorkflowAppPublishCoordinator(
            new GovernanceBridgeClient(http),
            preflight,
            () => DateTimeOffset.Parse("2026-05-23T13:40:00Z"),
            () => "deadbeef");

        var result = await coordinator.PublishAsync(
            "http://127.0.0.1:18081",
            "",
            new WorkflowAppPublishSourceContext(
                WorkflowAppPublishSourceKind.Canvas,
                "http://127.0.0.1:18082",
                BuildDocument()),
            new WorkflowAppPublishFormState(
                "Finance App",
                "",
                true,
                new JsonObject { ["region"] = new JsonObject { ["type"] = "string" } },
                new JsonObject { ["region"] = "cn" },
                WorkflowAppRunParamsSupport.BuildTemplatePolicyPreview(true, new JsonObject { ["region"] = "cn" })));

        Assert.True(result.Ok);
        Assert.NotNull(result.PreflightReport);
        Assert.NotNull(result.PublishedVersion);
        Assert.NotNull(result.PublishedApp);
        Assert.Single(result.Items);
        Assert.Contains(calls, url => url.Contains("/operators/workflow_contract_v1/validate", StringComparison.Ordinal));
        Assert.Contains(calls, url => url.Contains("/governance/workflow-versions/", StringComparison.Ordinal));
        Assert.Contains(calls, url => url.Contains("/governance/workflow-apps/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PublishAsync_BlocksWhenPreflightFails()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            if (request.RequestUri!.AbsoluteUri.EndsWith("/operators/workflow_contract_v1/validate", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """
                    {
                      "ok": true,
                      "valid": false,
                      "status": "invalid",
                      "error_items": [
                        {
                          "path": "workflow.version",
                          "code": "required",
                          "message": "workflow.version is required"
                        }
                      ]
                    }
                    """);
            }
            if (request.RequestUri.AbsoluteUri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
            {
                return BoundaryResponse();
            }
            return Json(HttpStatusCode.OK, """{"ok":true}""");
        }));

        var preflight = new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http), () => "2026-05-23T13:40:00Z");
        var coordinator = new WorkflowAppPublishCoordinator(
            new GovernanceBridgeClient(http),
            preflight,
            () => DateTimeOffset.Parse("2026-05-23T13:40:00Z"),
            () => "deadbeef");

        var result = await coordinator.PublishAsync(
            "http://127.0.0.1:18081",
            "",
            new WorkflowAppPublishSourceContext(
                WorkflowAppPublishSourceKind.Canvas,
                "http://127.0.0.1:18082",
                BuildDocument()),
            new WorkflowAppPublishFormState(
                "Finance App",
                "",
                true,
                new JsonObject(),
                new JsonObject(),
                WorkflowAppRunParamsSupport.BuildTemplatePolicyPreview(true, new JsonObject())));

        Assert.False(result.Ok);
        Assert.NotNull(result.PreflightReport);
        Assert.Null(result.PublishedApp);
    }

    [Fact]
    public async Task RefreshAppsAsync_ReturnsList()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            if (request.RequestUri!.AbsoluteUri.EndsWith("/governance/meta/control-plane", StringComparison.Ordinal))
            {
                return BoundaryResponse();
            }
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "items": [
                    {
                      "app_id": "finance_app",
                      "name": "Finance App",
                      "workflow_id": "wf_finance",
                      "published_version_id": "ver_finance_001",
                      "updated_at": "2026-05-23T13:40:00Z",
                      "provider": "glue-python",
                      "owner": "glue-python"
                    }
                  ]
                }
                """);
        }));

        var coordinator = new WorkflowAppPublishCoordinator(
            new GovernanceBridgeClient(http),
            new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http)));

        var items = await coordinator.RefreshAppsAsync("http://127.0.0.1:18081", "");

        Assert.Single(items);
        Assert.Equal("finance_app", items[0].AppId);
    }

    private static WorkflowGraphDocument BuildDocument()
    {
        return new WorkflowGraphDocument(
            "wf_finance",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "load_1",
                    "load_rows_v3",
                    "Load",
                    "source",
                    10,
                    20,
                    new JsonObject
                    {
                        ["source_type"] = "sqlite",
                        ["source"] = "D:/demo.db",
                        ["query"] = "select * from data"
                    })
            ],
            [],
            WorkflowGraphViewportDocument.Default,
            WorkflowGraphSelectionDocument.Empty);
    }

    private static HttpResponseMessage BoundaryResponse()
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
                    "capability": "workflow_apps",
                    "route_prefix": "/governance/workflow-apps",
                    "owned_route_prefixes": ["/governance/workflow-apps"],
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
                  }
                ]
              }
            }
            """);
    }

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _responder;

        public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        {
            _responder = request => Task.FromResult(responder(request));
        }

        public StubHttpMessageHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder)
        {
            _responder = responder;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return _responder(request);
        }
    }
}

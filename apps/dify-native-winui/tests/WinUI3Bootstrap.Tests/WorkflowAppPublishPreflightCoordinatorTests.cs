using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppPublishPreflightCoordinatorTests
{
    [Fact]
    public async Task RunPublishPreflightAsync_PassesForCanvasSource()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            if (request.RequestUri!.AbsoluteUri.EndsWith("/operators/workflow_contract_v1/validate", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true,"valid":true,"status":"ok"}""");
            }
            if (request.RequestUri.AbsoluteUri.EndsWith("/operators/data_source_browser_v1", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true}""");
            }
            return Json(HttpStatusCode.NotFound, """{"error":"unexpected"}""");
        }));

        var coordinator = new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http), () => "2026-05-23T13:40:00Z");
        var report = await coordinator.RunPublishPreflightAsync(
            new WorkflowAppPublishSourceContext(
                WorkflowAppPublishSourceKind.Canvas,
                "http://127.0.0.1:18082",
                BuildDocument("wf_canvas")),
            apiKey: "");

        Assert.True(report.Ok);
        Assert.Equal(WorkflowAppPublishSourceKind.Canvas, report.SourceKind);
        Assert.Equal("wf_canvas", report.WorkflowId);
        Assert.Empty(report.Issues);
    }

    [Fact]
    public async Task RunPublishPreflightAsync_ReportsWorkflowContractFailure()
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
            return Json(HttpStatusCode.OK, """{"ok":true}""");
        }));

        var coordinator = new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http));
        var report = await coordinator.RunPublishPreflightAsync(
            new WorkflowAppPublishSourceContext(
                WorkflowAppPublishSourceKind.SqlStudio,
                "http://127.0.0.1:18082",
                BuildDocument("wf_sql")),
            apiKey: "");

        Assert.False(report.Ok);
        var issue = Assert.Single(report.Issues);
        Assert.Equal("workflow_contract", issue.Kind);
        Assert.Equal("workflow.version", issue.Path);
    }

    [Fact]
    public async Task RunPublishPreflightAsync_ReportsConnectionFailure()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(request =>
        {
            if (request.RequestUri!.AbsoluteUri.EndsWith("/operators/workflow_contract_v1/validate", StringComparison.Ordinal))
            {
                return Json(HttpStatusCode.OK, """{"ok":true,"valid":true,"status":"ok"}""");
            }
            return Json(HttpStatusCode.BadRequest, """{"error":"connection denied"}""");
        }));

        var coordinator = new WorkflowAppPublishPreflightCoordinator(new WorkflowRunnerAdapter(http));
        var report = await coordinator.RunPublishPreflightAsync(
            new WorkflowAppPublishSourceContext(
                WorkflowAppPublishSourceKind.Canvas,
                "http://127.0.0.1:18082",
                BuildDocument("wf_canvas")),
            apiKey: "");

        Assert.False(report.Ok);
        Assert.Contains(report.Issues, item => item.Kind == "connection");
    }

    private static WorkflowGraphDocument BuildDocument(string workflowId)
    {
        return new WorkflowGraphDocument(
            workflowId,
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

    private static HttpResponseMessage Json(HttpStatusCode statusCode, string json)
    {
        return new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _responder;

        public StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        {
            _responder = responder;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(_responder(request));
        }
    }
}

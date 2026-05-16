using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowDraftRunCoordinatorTests
{
    [Fact]
    public async Task ExecuteAsync_PostsCanonicalWorkflowDefinitionToDraftRunRoute()
    {
        HttpRequestMessage? capturedRequest = null;
        string? capturedBody = null;
        using var http = new HttpClient(new StubHttpMessageHandler(async request =>
        {
            capturedRequest = request;
            capturedBody = request.Content is null ? null : await request.Content.ReadAsStringAsync();
            return Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "job_id": "job_canvas",
                  "flow": "workflow",
                  "duration_ms": 42,
                  "final_output": {
                    "rows": [
                      { "value": 1 }
                    ]
                  },
                  "node_outputs": {
                    "load_1": {
                      "ok": true,
                      "status": "done",
                      "rows": [
                        { "value": 1 }
                      ]
                    }
                  }
                }
                """);
        }));
        var coordinator = new WorkflowDraftRunCoordinator(new WorkflowRunnerAdapter(http));

        var result = await coordinator.ExecuteAsync(
            "http://127.0.0.1:18082",
            apiKey: "",
            jobId: "job_canvas",
            document: BuildDocument());

        Assert.NotNull(capturedRequest);
        Assert.Equal("http://127.0.0.1:18082/operators/workflow_draft_run_v1", capturedRequest!.RequestUri!.AbsoluteUri);
        Assert.NotNull(capturedBody);
        var payload = JsonNode.Parse(capturedBody!)!.AsObject();
        Assert.Equal("job_canvas", payload["job_id"]?.GetValue<string>());
        Assert.NotNull(payload["workflow_definition"]);
        Assert.Null(payload["workflow"]);
        Assert.Equal("wf_canvas", payload["workflow_definition"]!["workflow_id"]?.GetValue<string>());

        Assert.True(result.ParsedBindingState);
        Assert.Equal("job_canvas", result.BindingState.PanelState.JobIdText);
        Assert.NotNull(result.FinalOutput);
        Assert.Single(result.NodeOutputPresentation.Items);
        Assert.NotNull(result.NodeOutputPresentation.FirstRowsOutput);
    }

    [Fact]
    public async Task ExecuteAsync_UnrecognizedResponseFallsBackToParseFailureBindingState()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(Json(HttpStatusCode.OK, """{"message":"not-a-run-shape"}"""))));
        var coordinator = new WorkflowDraftRunCoordinator(new WorkflowRunnerAdapter(http));

        var result = await coordinator.ExecuteAsync(
            "http://127.0.0.1:18082",
            apiKey: "",
            jobId: "job_canvas",
            document: BuildDocument());

        Assert.False(result.ParsedBindingState);
        Assert.Equal("Run completed, but the response could not be parsed.", result.BindingState.PanelState.RunResultText);
        Assert.Empty(result.NodeOutputPresentation.Items);
    }

    [Fact]
    public async Task ExecuteAsync_ExtractsChartAndNodeOutputPresentation()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(Json(HttpStatusCode.OK, """
                {
                  "ok": true,
                  "job_id": "job_canvas",
                  "flow": "workflow",
                  "node_outputs": {
                    "sql_chart_1": {
                      "ok": true,
                      "status": "done",
                      "chart_type": "bar",
                      "categories": ["A", "B"],
                      "series": [{ "name": "value", "data": [1, 2] }]
                    }
                  }
                }
                """))));
        var coordinator = new WorkflowDraftRunCoordinator(new WorkflowRunnerAdapter(http));

        var result = await coordinator.ExecuteAsync(
            "http://127.0.0.1:18082",
            apiKey: "",
            jobId: "job_canvas",
            document: BuildDocument());

        var node = Assert.Single(result.NodeOutputPresentation.Items);
        Assert.Equal("sql_chart_1", node.NodeKey);
        Assert.True(node.HasChartData);
        Assert.Equal("done (0 rows)", node.Subtitle);
        Assert.NotNull(result.NodeOutputPresentation.ChartSource);
        Assert.Equal("bar", result.NodeOutputPresentation.ChartSource!["chart_type"]?.GetValue<string>());
    }

    private static WorkflowGraphDocument BuildDocument()
    {
        return new WorkflowGraphDocument(
            "wf_canvas",
            "1.0.0",
            [
                new WorkflowGraphNodeDocument(
                    "load_1",
                    "load_rows_v3",
                    "Load",
                    "source",
                    40,
                    80,
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

    private sealed class StubHttpMessageHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return responder(request);
        }
    }
}

using System.Net;
using System.Text;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowDiagnosticsCoordinatorTests
{
    [Fact]
    public async Task RefreshAsync_BuildsAggregateDiagnosticsAndRustRows()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(Json(HttpStatusCode.OK, """
                {
                  "items": [
                    {
                      "operator": "sql_chart_v1",
                      "calls": 10,
                      "err": 2,
                      "p95_ms": 1200,
                      "avg_ms": 600
                    }
                  ]
                }
                """))));

        var coordinator = new WorkflowDiagnosticsCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceWorkflowRunRecordItem>>(
            [
                new GovernanceWorkflowRunRecordItem(
                    "run_1",
                    "wf_a",
                    "done",
                    true,
                    "2026-05-28T00:00:00Z",
                    "reference",
                    "",
                    "",
                    "version_reference",
                    [
                        new GovernanceWorkflowRunStepItem("clean_md", "DONE", "2026-05-28T00:00:00Z", "2026-05-28T00:00:01Z", 1, ""),
                        new GovernanceWorkflowRunStepItem("clean_md", "FAILED", "2026-05-28T00:00:02Z", "2026-05-28T00:00:05Z", 3, "boom"),
                        new GovernanceWorkflowRunStepItem("ai_refine", "DONE", "2026-05-28T00:00:05Z", "2026-05-28T00:00:07Z", 2, "")
                    ]),
                new GovernanceWorkflowRunRecordItem(
                    "run_2",
                    "wf_b",
                    "done",
                    true,
                    "2026-05-28T00:10:00Z",
                    "reference",
                    "",
                    "",
                    "version_reference",
                    [
                        new GovernanceWorkflowRunStepItem("clean_md", "DONE", "2026-05-28T00:10:00Z", "2026-05-28T00:10:02Z", 2, "")
                    ])
            ]),
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(
            [
                new GovernanceTimelineEntry("clean_md", "clean_md", "DONE", "2026-05-28T00:00:00Z", "2026-05-28T00:00:01Z", 1)
            ]),
            new WorkflowRunnerAdapter(http));

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "",
            "http://127.0.0.1:18082",
            "run_1");

        Assert.Equal("runs=2 | current_run_steps=1 | diagnostics=3", result.SummaryText);
        Assert.Single(result.CurrentRunTimeline);

        var clean = Assert.Single(result.AggregateItems, item => item.Chiplet == "clean_md");
        Assert.Equal(3, clean.Calls);
        Assert.Equal(1, clean.Failed);
        Assert.Equal(0.3333, clean.FailureRate);
        Assert.Equal(2, clean.P50Seconds);
        Assert.Equal(3, clean.P95Seconds);
        Assert.Equal(2, clean.AverageSeconds);

        var ai = Assert.Single(result.AggregateItems, item => item.Chiplet == "ai_refine");
        Assert.Equal(1, ai.Calls);
        Assert.Equal(2, ai.P95Seconds);

        var rust = Assert.Single(result.AggregateItems, item => item.Chiplet == "rust:sql_chart_v1");
        Assert.Equal(10, rust.Calls);
        Assert.Equal(2, rust.Failed);
        Assert.Equal(1.2, rust.P95Seconds);
    }

    [Fact]
    public async Task RefreshAsync_SwallowsRustStatsFailures()
    {
        using var http = new HttpClient(new StubHttpMessageHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new StringContent("""{"error":"runtime unavailable"}""", Encoding.UTF8, "application/json")
            })));

        var coordinator = new WorkflowDiagnosticsCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceWorkflowRunRecordItem>>(
            [
                new GovernanceWorkflowRunRecordItem(
                    "run_1",
                    "wf_a",
                    "done",
                    true,
                    "2026-05-28T00:00:00Z",
                    "reference",
                    "",
                    "",
                    "version_reference",
                    [
                        new GovernanceWorkflowRunStepItem("clean_md", "DONE", "2026-05-28T00:00:00Z", "2026-05-28T00:00:01Z", 1, "")
                    ])
            ]),
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(Array.Empty<GovernanceTimelineEntry>()),
            new WorkflowRunnerAdapter(http));

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "",
            "http://127.0.0.1:18082",
            "");

        Assert.Single(result.AggregateItems);
        Assert.Equal("clean_md", result.AggregateItems[0].Chiplet);
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

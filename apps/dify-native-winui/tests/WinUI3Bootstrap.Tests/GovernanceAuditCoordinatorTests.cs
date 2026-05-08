using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceAuditCoordinatorTests
{
    [Fact]
    public async Task RefreshAsync_UsesExpectedLimitsAndBuildsSummary()
    {
        string? seenBaseUrl = null;
        string? seenApiKey = null;
        int seenRunLimit = 0;
        int seenFailureLimit = 0;
        int seenAuditLimit = 0;
        string? seenAuditAction = null;
        string? seenTimelineRunId = null;

        var coordinator = new GovernanceAuditCoordinator(
            listRuns: (baseUrl, apiKey, limit, _) =>
            {
                seenBaseUrl = baseUrl;
                seenApiKey = apiKey;
                seenRunLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceWorkflowRunItem>>(
                [
                    new GovernanceWorkflowRunItem("run_1", "wf_1", "done", true, "2026-05-08T00:00:00Z"),
                    new GovernanceWorkflowRunItem("run_2", "wf_1", "failed", false, "2026-05-08T00:01:00Z"),
                ]);
            },
            getTimeline: (baseUrl, apiKey, runId, _) =>
            {
                seenTimelineRunId = runId;
                return Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(
                [
                    new GovernanceTimelineEntry("n1", "quality_check_v3", "failed", "s", "e", 1.5),
                ]);
            },
            getFailureSummary: (baseUrl, apiKey, limit, _) =>
            {
                seenFailureLimit = limit;
                return Task.FromResult<IReadOnlyList<GovernanceFailureSummaryEntry>>(
                [
                    new GovernanceFailureSummaryEntry("quality_check_v3", 3, "boom"),
                ]);
            },
            listAuditEvents: (baseUrl, apiKey, limit, action, _) =>
            {
                seenAuditLimit = limit;
                seenAuditAction = action;
                return Task.FromResult<IReadOnlyList<GovernanceAuditEventItem>>(
                [
                    new GovernanceAuditEventItem("2026-05-08T00:00:00Z", "run_workflow", "{\"run_id\":\"run_1\"}"),
                    new GovernanceAuditEventItem("2026-05-08T00:00:02Z", "run_workflow", "{\"run_id\":\"run_2\"}"),
                ]);
            });

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "token",
            "run_2",
            "run_workflow");

        Assert.Equal("http://127.0.0.1:18081", seenBaseUrl);
        Assert.Equal("token", seenApiKey);
        Assert.Equal(40, seenRunLimit);
        Assert.Equal(80, seenFailureLimit);
        Assert.Equal(60, seenAuditLimit);
        Assert.Equal("run_workflow", seenAuditAction);
        Assert.Equal("run_2", seenTimelineRunId);
        Assert.Equal("runs=2, failure_types=1, audit_events=2", result.SummaryText);
        Assert.Equal(2, result.Runs.Count);
        Assert.Single(result.Timeline);
        Assert.Single(result.Failures);
        Assert.Equal(2, result.AuditEvents.Count);
    }

    [Fact]
    public async Task RefreshAsync_SkipsTimelineQueryWhenRunIdBlank()
    {
        var timelineCalls = 0;
        var coordinator = new GovernanceAuditCoordinator(
            listRuns: (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceWorkflowRunItem>>(Array.Empty<GovernanceWorkflowRunItem>()),
            getTimeline: (_, _, _, _) =>
            {
                timelineCalls += 1;
                return Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(Array.Empty<GovernanceTimelineEntry>());
            },
            getFailureSummary: (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceFailureSummaryEntry>>(Array.Empty<GovernanceFailureSummaryEntry>()),
            listAuditEvents: (_, _, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceAuditEventItem>>(Array.Empty<GovernanceAuditEventItem>()));

        var result = await coordinator.RefreshAsync(
            "http://127.0.0.1:18081",
            "",
            "   ",
            "");

        Assert.Equal(0, timelineCalls);
        Assert.Empty(result.Timeline);
        Assert.Equal("runs=0, failure_types=0, audit_events=0", result.SummaryText);
    }
}

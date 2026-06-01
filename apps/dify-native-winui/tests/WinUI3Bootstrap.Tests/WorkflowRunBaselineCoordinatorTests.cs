using System.Text.Json.Nodes;
using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowRunBaselineCoordinatorTests
{
    [Fact]
    public async Task RefreshBaselinesAsync_ReturnsItems()
    {
        var coordinator = new WorkflowRunBaselineCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceRunBaselineItem>>(
            [
                new GovernanceRunBaselineItem("base_1", "Base One", "run_1", "wf_finance", "2026-05-29T00:00:00Z", "seed", "glue-python", "glue-python", "glue-python.governance.run_baselines")
            ]),
            (_, _, _, _, _, _, _, _, _) => throw new NotSupportedException(),
            (_, _, _, _) => throw new NotSupportedException());

        var baselines = await coordinator.RefreshBaselinesAsync("http://127.0.0.1:18081", "");

        var baseline = Assert.Single(baselines);
        Assert.Equal("base_1", baseline.BaselineId);
    }

    [Fact]
    public async Task SaveCurrentRunAsBaselineAsync_GeneratesIdAndUsesRunRecordWorkflowId()
    {
        string? capturedBaselineId = null;
        string? capturedName = null;
        string? capturedWorkflowId = null;

        var coordinator = new WorkflowRunBaselineCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceRunBaselineItem>>(Array.Empty<GovernanceRunBaselineItem>()),
            (_, _, baselineId, name, runId, workflowId, createdAt, notes, _) =>
            {
                capturedBaselineId = baselineId;
                capturedName = name;
                capturedWorkflowId = workflowId;
                return Task.FromResult(new GovernanceRunBaselineItem(baselineId, name, runId, workflowId, createdAt, notes, "glue-python", "glue-python", "glue-python.governance.run_baselines"));
            },
            (_, _, runId, _) => Task.FromResult(CreateRunRecord(runId)),
            () => DateTimeOffset.Parse("2026-05-29T08:00:00Z"),
            () => "deadbeef");

        var baseline = await coordinator.SaveCurrentRunAsBaselineAsync(
            "http://127.0.0.1:18081",
            "",
            "run_12345678");

        Assert.Equal("20260529080000_deadbeef", capturedBaselineId);
        Assert.Equal("baseline_run_1234", capturedName);
        Assert.Equal("wf_finance", capturedWorkflowId);
        Assert.Equal("20260529080000_deadbeef", baseline.BaselineId);
    }

    [Fact]
    public async Task CompareRunsAsync_BuildsNodeDiff()
    {
        var coordinator = new WorkflowRunBaselineCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceRunBaselineItem>>(Array.Empty<GovernanceRunBaselineItem>()),
            (_, _, _, _, _, _, _, _, _) => throw new NotSupportedException(),
            (_, _, runId, _) => Task.FromResult(runId == "run_a"
                ? CreateRunRecord("run_a", steps:
                [
                    new GovernanceWorkflowRunStepItem("clean_md", "DONE", "2026-05-29T00:00:00Z", "2026-05-29T00:00:01Z", 1, ""),
                    new GovernanceWorkflowRunStepItem("ai_refine", "DONE", "2026-05-29T00:00:01Z", "2026-05-29T00:00:03Z", 2, "")
                ])
                : CreateRunRecord("run_b", steps:
                [
                    new GovernanceWorkflowRunStepItem("clean_md", "FAILED", "2026-05-29T00:00:00Z", "2026-05-29T00:00:02Z", 2, "boom"),
                    new GovernanceWorkflowRunStepItem("ai_refine", "DONE", "2026-05-29T00:00:03Z", "2026-05-29T00:00:05Z", 2, "")
                ])));

        var compare = await coordinator.CompareRunsAsync("http://127.0.0.1:18081", "", "run_a", "run_b");

        Assert.True(compare.Ok);
        Assert.Equal(1, compare.Summary.ChangedNodes);
        var changed = Assert.Single(compare.NodeDiff, item => item.StatusChanged);
        Assert.Equal("clean_md", changed.Id);
        Assert.Equal(1, changed.SecondsDelta);
    }

    [Fact]
    public async Task CompareRunWithBaselineAsync_UsesSelectedOrLatestBaseline()
    {
        var coordinator = new WorkflowRunBaselineCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceRunBaselineItem>>(
            [
                new GovernanceRunBaselineItem("base_1", "Base One", "run_a", "wf_finance", "2026-05-29T00:00:00Z", "", "glue-python", "glue-python", "glue-python.governance.run_baselines")
            ]),
            (_, _, _, _, _, _, _, _, _) => throw new NotSupportedException(),
            (_, _, runId, _) => Task.FromResult(runId == "run_a"
                ? CreateRunRecord("run_a", steps:
                [
                    new GovernanceWorkflowRunStepItem("clean_md", "DONE", "2026-05-29T00:00:00Z", "2026-05-29T00:00:01Z", 1, "")
                ])
                : CreateRunRecord("run_b", steps:
                [
                    new GovernanceWorkflowRunStepItem("clean_md", "FAILED", "2026-05-29T00:00:00Z", "2026-05-29T00:00:02Z", 2, "boom")
                ])));

        var result = await coordinator.CompareRunWithBaselineAsync("http://127.0.0.1:18081", "", "run_b");

        Assert.True(result.Ok);
        Assert.Equal("base_1", result.BaselineId);
        Assert.Equal(1, result.ChangedNodes);
        Assert.Equal(1, result.StatusFlipNodes);
        Assert.Equal(1, result.PerfHotNodes);
    }

    [Fact]
    public async Task LoadLineageAsync_UsesRunRecordPayloadOrFallsBackToCurrentRawResponse()
    {
        var coordinator = new WorkflowRunBaselineCoordinator(
            (_, _, _, _) => Task.FromResult<IReadOnlyList<GovernanceRunBaselineItem>>(Array.Empty<GovernanceRunBaselineItem>()),
            (_, _, _, _, _, _, _, _, _) => throw new NotSupportedException(),
            (_, _, runId, _) => Task.FromResult(runId == "run_with_lineage"
                ? CreateRunRecord(runId, resultPayload: new JsonObject
                {
                    ["lineage"] = new JsonObject
                    {
                        ["node_count"] = 2,
                        ["edge_count"] = 1
                    }
                })
                : CreateRunRecord(runId)));

        var direct = await coordinator.LoadLineageAsync(
            "http://127.0.0.1:18081",
            "",
            "run_with_lineage",
            "{}",
            "");
        Assert.True(direct.Ok);
        Assert.Equal(2, direct.NodeCount);

        var fallback = await coordinator.LoadLineageAsync(
            "http://127.0.0.1:18081",
            "",
            "run_current",
            """
            {
              "lineage": {
                "nodes": [{ "id": "n1" }, { "id": "n2" }],
                "edges": [{ "from": "n1", "to": "n2" }]
              }
            }
            """,
            "run_current");
        Assert.True(fallback.Ok);
        Assert.Equal(2, fallback.NodeCount);
        Assert.Equal(1, fallback.EdgeCount);
    }

    private static GovernanceWorkflowRunRecordDetail CreateRunRecord(
        string runId,
        IReadOnlyList<GovernanceWorkflowRunStepItem>? steps = null,
        JsonObject? resultPayload = null)
    {
        return new GovernanceWorkflowRunRecordDetail(
            RunId: runId,
            WorkflowId: "wf_finance",
            Status: "done",
            Ok: true,
            Timestamp: "2026-05-29T00:00:00Z",
            RunRequestKind: "reference",
            VersionId: "ver_1",
            PublishedVersionId: string.Empty,
            WorkflowDefinitionSource: "version_reference",
            Payload: new JsonObject(),
            Steps: steps ?? Array.Empty<GovernanceWorkflowRunStepItem>(),
            ResultPayload: resultPayload ?? new JsonObject());
    }
}

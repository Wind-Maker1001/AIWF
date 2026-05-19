using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowVersionComparePresenterTests
{
    [Fact]
    public void CreateSuccess_BuildsSummaryAndDetailRows()
    {
        var state = WorkflowVersionComparePresenter.CreateSuccess(
            new GovernanceWorkflowVersionCompareResult(
                true,
                "glue-python",
                new GovernanceWorkflowVersionCompareSummary("ver_a", "ver_b", 2, 1, 0),
                [
                    new GovernanceWorkflowVersionCompareNodeDiffItem("n1", "load_rows_v3", "updated", true, false)
                ]));

        Assert.True(state.Ok);
        Assert.Contains("ver_a", state.SummaryText, StringComparison.Ordinal);
        Assert.Contains("changed=2", state.SummaryText, StringComparison.Ordinal);
        Assert.Single(state.DetailRows);
        Assert.Contains("n1 | load_rows_v3 | updated | config=true | status=false", state.DetailRows[0], StringComparison.Ordinal);
    }

    [Fact]
    public void CreateFailure_PrefersStructuredErrorCodeAndPath()
    {
        var state = WorkflowVersionComparePresenter.CreateFailure(
            "version not found",
            "workflow_graph_invalid",
            [new GovernanceErrorItem("request.version_a", "missing", "version not found")]);

        Assert.False(state.Ok);
        Assert.Equal("-", state.SummaryText);
        Assert.Single(state.DetailRows);
        Assert.Contains("[missing] request.version_a", state.DetailRows[0], StringComparison.Ordinal);
        Assert.Contains("[workflow_graph_invalid]", state.StatusText, StringComparison.Ordinal);
    }
}

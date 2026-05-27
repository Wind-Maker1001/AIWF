using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppPublishPreflightPresenterTests
{
    [Fact]
    public void Create_BuildsSummaryAndIssueRows()
    {
        var state = WorkflowAppPublishPreflightPresenter.Create(
            new WorkflowAppPublishPreflightReport(
                false,
                WorkflowAppPublishSourceKind.Canvas,
                "wf_finance",
                [new WorkflowAppPublishPreflightIssue("error", "workflow_contract", "n1", "workflow.version is required", "workflow.version", "required")],
                "2026-05-23T13:40:00Z"));

        Assert.False(state.Ok);
        Assert.Contains("workflow_id=wf_finance", state.SummaryText, StringComparison.Ordinal);
        Assert.Single(state.IssueRows);
        Assert.Contains("workflow_contract", state.IssueRows[0], StringComparison.Ordinal);
        Assert.Contains("workflow.version", state.IssueRows[0], StringComparison.Ordinal);
    }
}

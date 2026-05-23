using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class WorkflowAppPublishPresenterTests
{
    [Fact]
    public void CreateSuccess_FormatsStatusAndPublishedVersion()
    {
        var state = WorkflowAppPublishPresenter.CreateSuccess(
            new GovernanceWorkflowAppItem(
                "finance_app",
                "Finance App",
                "wf_finance",
                "ver_finance_001",
                "2026-05-23T13:40:00Z",
                "glue-python",
                "glue-python"));

        Assert.True(state.Ok);
        Assert.Contains("Finance App", state.StatusText, StringComparison.Ordinal);
        Assert.Equal("ver_finance_001", state.PublishedVersionText);
    }

    [Fact]
    public void CreateFailure_PrefersStructuredPath()
    {
        var state = WorkflowAppPublishPresenter.CreateFailure(
            "version missing",
            "governance_validation_invalid",
            [new GovernanceErrorItem("published_version_id", "validation_error", "missing")]);

        Assert.False(state.Ok);
        Assert.Contains("governance_validation_invalid", state.StatusText, StringComparison.Ordinal);
        Assert.Equal("-", state.PublishedVersionText);
    }
}

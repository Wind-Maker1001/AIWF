using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceManualReviewPresenterTests
{
    [Fact]
    public void BuildPendingHintText_FormatsZeroAndPositiveCounts()
    {
        Assert.Equal("No pending review items.", GovernanceManualReviewPresenter.BuildPendingHintText(0));
        Assert.Equal("Pending review items: 3", GovernanceManualReviewPresenter.BuildPendingHintText(3));
    }

    [Fact]
    public void BuildSelectionState_UsesEmptyStateWhenItemMissing()
    {
        var state = GovernanceManualReviewPresenter.BuildSelectionState(null);

        Assert.Equal("No review selected", state.SummaryText);
        Assert.Equal("-", state.DetailText);
        Assert.False(state.CanApprove);
        Assert.False(state.CanReject);
    }

    [Fact]
    public void BuildSelectionState_FormatsSelectedReview()
    {
        var state = GovernanceManualReviewPresenter.BuildSelectionState(
            new GovernanceManualReviewItem("run_1", "gate_a", "wf_1", "n1", "", "", "", "", "pending", false));

        Assert.Equal("run_1 / gate_a", state.SummaryText);
        Assert.Equal("workflow=wf_1 | node=n1 | status=pending", state.DetailText);
        Assert.True(state.CanApprove);
        Assert.True(state.CanReject);
    }

}

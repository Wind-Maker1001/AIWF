using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class GovernanceBoundaryPresenterTests
{
    [Fact]
    public void Build_UsesUnavailableStateWhenBoundaryMissing()
    {
        var state = GovernanceBoundaryPresenter.Build(null, "connection refused");

        Assert.Equal("Governance control-plane boundary unavailable.", state.HintText);
        Assert.Equal("connection refused", state.RoutesText);
    }

    [Fact]
    public void Build_FormatsBoundarySummary()
    {
        var boundary = new GovernanceControlPlaneBoundary(
            "governance_surface.v1",
            "effective_second_control_plane",
            "governance_state",
            "glue-python",
            "base-java",
            "accel-rust",
            "dify-native-winui",
            "/governance/meta/control-plane",
            [
                new GovernanceSurfaceItem("manual_reviews", "/governance/manual-reviews", ["/governance/manual-reviews"], "glue-python", "governance_state", false),
                new GovernanceSurfaceItem("quality_rule_sets", "/governance/quality-rule-sets", ["/governance/quality-rule-sets"], "glue-python", "governance_state", false),
            ]);

        var state = GovernanceBoundaryPresenter.Build(boundary);

        Assert.Equal("Governance state is owned by glue-python; job lifecycle remains owned by base-java.", state.HintText);
        Assert.Equal("role=governance_state | surfaces=2 | meta=/governance/meta/control-plane", state.RoutesText);
    }
}

namespace AIWF.Native.Runtime;

public sealed record GovernanceBoundaryViewState(
    string HintText,
    string RoutesText);

public static class GovernanceBoundaryPresenter
{
    public static GovernanceBoundaryViewState Build(GovernanceControlPlaneBoundary? boundary, string? unavailableDetail = null)
    {
        if (boundary is null)
        {
            return new GovernanceBoundaryViewState(
                HintText: "Governance control-plane boundary unavailable.",
                RoutesText: string.IsNullOrWhiteSpace(unavailableDetail) ? "-" : unavailableDetail);
        }

        return new GovernanceBoundaryViewState(
            HintText: $"Governance state is owned by {boundary.GovernanceStateControlPlaneOwner}; job lifecycle remains owned by {boundary.JobLifecycleControlPlaneOwner}.",
            RoutesText: $"role={boundary.ControlPlaneRole} | surfaces={boundary.GovernanceSurfaces.Count} | meta={boundary.MetaRoute}");
    }
}

namespace AIWF.Native.Runtime;

public sealed record GovernanceReviewSelectionState(
    string SummaryText,
    string DetailText,
    bool CanApprove,
    bool CanReject);

public static class GovernanceManualReviewPresenter
{
    public static string BuildPendingHintText(int count)
    {
        return count <= 0
            ? "No pending review items."
            : $"Pending review items: {count}";
    }

    public static GovernanceReviewSelectionState BuildSelectionState(GovernanceManualReviewItem? item)
    {
        if (item is null)
        {
            return new GovernanceReviewSelectionState(
                SummaryText: "No review selected",
                DetailText: "-",
                CanApprove: false,
                CanReject: false);
        }

        return new GovernanceReviewSelectionState(
            SummaryText: $"{item.RunId} / {item.ReviewKey}",
            DetailText: $"workflow={item.WorkflowId} | node={item.NodeId} | status={item.Status}",
            CanApprove: true,
            CanReject: true);
    }
}

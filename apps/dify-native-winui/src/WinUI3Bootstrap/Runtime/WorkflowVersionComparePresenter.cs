namespace AIWF.Native.Runtime;

public sealed record WorkflowVersionCompareViewState(
    bool Ok,
    string SummaryText,
    IReadOnlyList<string> DetailRows,
    string StatusText);

public static class WorkflowVersionComparePresenter
{
    public static WorkflowVersionCompareViewState CreateEmpty()
    {
        return new WorkflowVersionCompareViewState(
            Ok: false,
            SummaryText: "-",
            DetailRows: Array.Empty<string>(),
            StatusText: string.Empty);
    }

    public static WorkflowVersionCompareViewState CreateValidationFailure(string message)
    {
        return new WorkflowVersionCompareViewState(
            Ok: false,
            SummaryText: "-",
            DetailRows: Array.Empty<string>(),
            StatusText: string.IsNullOrWhiteSpace(message) ? "Workflow version compare requires A/B." : message);
    }

    public static WorkflowVersionCompareViewState CreateSuccess(GovernanceWorkflowVersionCompareResult result)
    {
        var summary = result.Summary;
        var summaryText = $"{summary.VersionA} -> {summary.VersionB} | changed={summary.ChangedNodes} | added_edges={summary.AddedEdges} | removed_edges={summary.RemovedEdges}";
        var details = result.NodeDiff
            .Select(static item => $"{item.NodeId} | {item.NodeType} | {item.Change} | config={item.ConfigChanged.ToString().ToLowerInvariant()} | status={item.StatusChanged.ToString().ToLowerInvariant()}")
            .ToArray();
        return new WorkflowVersionCompareViewState(
            Ok: true,
            SummaryText: summaryText,
            DetailRows: details,
            StatusText: "Workflow version compare completed.");
    }

    public static WorkflowVersionCompareViewState CreateFailure(
        string message,
        string? errorCode = null,
        IReadOnlyList<GovernanceErrorItem>? errorItems = null)
    {
        var detailRows = errorItems is { Count: > 0 }
            ? errorItems
                .Select(static item => $"[{item.Code}] {item.Path}")
                .ToArray()
            : Array.Empty<string>();

        var statusText = !string.IsNullOrWhiteSpace(errorCode) && detailRows.Length > 0
            ? $"Compare failed: [{errorCode}] {detailRows[0]}"
            : !string.IsNullOrWhiteSpace(message)
                ? $"Compare failed: {message}"
                : "Compare failed.";

        return new WorkflowVersionCompareViewState(
            Ok: false,
            SummaryText: "-",
            DetailRows: detailRows,
            StatusText: statusText);
    }
}

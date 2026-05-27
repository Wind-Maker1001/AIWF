namespace AIWF.Native.Runtime;

public sealed record WorkflowAppPublishPreflightViewState(
    bool Ok,
    string SummaryText,
    IReadOnlyList<string> IssueRows,
    string StatusText);

public static class WorkflowAppPublishPreflightPresenter
{
    public static WorkflowAppPublishPreflightViewState Empty()
    {
        return new WorkflowAppPublishPreflightViewState(false, "-", Array.Empty<string>(), string.Empty);
    }

    public static WorkflowAppPublishPreflightViewState Create(WorkflowAppPublishPreflightReport report)
    {
        var issueRows = report.Issues
            .Select(static issue =>
            {
                var path = string.IsNullOrWhiteSpace(issue.Path) ? string.Empty : $" | {issue.Path}";
                var node = string.IsNullOrWhiteSpace(issue.NodeId) ? string.Empty : $" | node={issue.NodeId}";
                var code = string.IsNullOrWhiteSpace(issue.Code) ? string.Empty : $"[{issue.Code}] ";
                return $"{issue.Kind} | {issue.Level}{node}{path} | {code}{issue.Message}";
            })
            .ToArray();
        var summaryText = $"{report.SourceKind} | workflow_id={report.WorkflowId} | issues={report.Issues.Count}";
        var statusText = report.Ok
            ? "Workflow app publish preflight passed."
            : $"Workflow app publish preflight failed: {issueRows.FirstOrDefault() ?? "unknown"}";
        return new WorkflowAppPublishPreflightViewState(report.Ok, summaryText, issueRows, statusText);
    }

    public static WorkflowAppPublishPreflightViewState CreateFailure(string message)
    {
        return new WorkflowAppPublishPreflightViewState(false, "-", Array.Empty<string>(), string.IsNullOrWhiteSpace(message) ? "Workflow app publish preflight failed." : message);
    }
}

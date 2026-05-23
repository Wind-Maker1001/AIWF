namespace AIWF.Native.Runtime;

public sealed record WorkflowAppPublishViewState(
    bool Ok,
    string StatusText,
    string PublishedVersionText);

public static class WorkflowAppPublishPresenter
{
    public static WorkflowAppPublishViewState CreateSuccess(GovernanceWorkflowAppItem item)
    {
        var publishedVersionId = string.IsNullOrWhiteSpace(item.PublishedVersionId) ? "-" : item.PublishedVersionId;
        return new WorkflowAppPublishViewState(
            true,
            $"Workflow app published: {item.Name} ({item.AppId})",
            publishedVersionId);
    }

    public static WorkflowAppPublishViewState CreateFailure(string message, string? errorCode = null, IReadOnlyList<GovernanceErrorItem>? errorItems = null)
    {
        var detail = errorItems is { Count: > 0 }
            ? $"{errorCode} | {errorItems[0].Path}"
            : errorCode;
        var statusText = !string.IsNullOrWhiteSpace(detail)
            ? $"Publish failed: {detail}"
            : $"Publish failed: {message}";
        return new WorkflowAppPublishViewState(false, statusText, "-");
    }
}

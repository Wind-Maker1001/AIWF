namespace AIWF.Native.Runtime;

public sealed record GovernanceAuditRefreshResult(
    IReadOnlyList<GovernanceWorkflowRunItem> Runs,
    IReadOnlyList<GovernanceTimelineEntry> Timeline,
    IReadOnlyList<GovernanceFailureSummaryEntry> Failures,
    IReadOnlyList<GovernanceAuditEventItem> AuditEvents,
    string SummaryText);

public sealed class GovernanceAuditCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceWorkflowRunItem>>> _listRuns;
    private readonly Func<string, string?, string, CancellationToken, Task<IReadOnlyList<GovernanceTimelineEntry>>> _getTimeline;
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceFailureSummaryEntry>>> _getFailureSummary;
    private readonly Func<string, string?, int, string?, CancellationToken, Task<IReadOnlyList<GovernanceAuditEventItem>>> _listAuditEvents;

    public GovernanceAuditCoordinator(GovernanceBridgeClient client)
        : this(
            client.ListWorkflowRunsAsync,
            client.GetWorkflowRunTimelineAsync,
            client.GetWorkflowFailureSummaryAsync,
            client.ListWorkflowAuditEventsAsync)
    {
    }

    public GovernanceAuditCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceWorkflowRunItem>>> listRuns,
        Func<string, string?, string, CancellationToken, Task<IReadOnlyList<GovernanceTimelineEntry>>> getTimeline,
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceFailureSummaryEntry>>> getFailureSummary,
        Func<string, string?, int, string?, CancellationToken, Task<IReadOnlyList<GovernanceAuditEventItem>>> listAuditEvents)
    {
        _listRuns = listRuns;
        _getTimeline = getTimeline;
        _getFailureSummary = getFailureSummary;
        _listAuditEvents = listAuditEvents;
    }

    public async Task<GovernanceAuditRefreshResult> RefreshAsync(
        string baseUrl,
        string? apiKey,
        string? runId,
        string? action,
        CancellationToken cancellationToken = default)
    {
        var normalizedRunId = (runId ?? string.Empty).Trim();
        var normalizedAction = (action ?? string.Empty).Trim();

        var runsTask = _listRuns(baseUrl, apiKey, 40, cancellationToken);
        var failuresTask = _getFailureSummary(baseUrl, apiKey, 80, cancellationToken);
        var auditsTask = _listAuditEvents(
            baseUrl,
            apiKey,
            60,
            string.IsNullOrWhiteSpace(normalizedAction) ? null : normalizedAction,
            cancellationToken);

        Task<IReadOnlyList<GovernanceTimelineEntry>> timelineTask =
            string.IsNullOrWhiteSpace(normalizedRunId)
                ? Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(Array.Empty<GovernanceTimelineEntry>())
                : _getTimeline(baseUrl, apiKey, normalizedRunId, cancellationToken);

        await Task.WhenAll(runsTask, timelineTask, failuresTask, auditsTask);

        var runs = await runsTask;
        var timeline = await timelineTask;
        var failures = await failuresTask;
        var audits = await auditsTask;

        return new GovernanceAuditRefreshResult(
            Runs: runs,
            Timeline: timeline,
            Failures: failures,
            AuditEvents: audits,
            SummaryText: $"runs={runs.Count}, failure_types={failures.Count}, audit_events={audits.Count}");
    }
}

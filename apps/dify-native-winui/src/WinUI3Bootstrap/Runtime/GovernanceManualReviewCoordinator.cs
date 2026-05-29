namespace AIWF.Native.Runtime;

public sealed record GovernancePendingReviewRefreshResult(
    IReadOnlyList<GovernanceManualReviewItem> Items,
    string HintText);

public sealed class GovernanceManualReviewCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> _listPendingReviews;
    private readonly Func<string, string?, int, string?, string?, string?, string?, string?, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> _listReviewHistory;
    private readonly Func<string, string?, string, string, bool, string, string, CancellationToken, Task<GovernanceManualReviewItem>> _submitReviewDecision;

    public GovernanceManualReviewCoordinator(GovernanceBridgeClient client)
        : this(
            client.ListManualReviewsAsync,
            client.ListManualReviewHistoryAsync,
            client.SubmitManualReviewAsync)
    {
    }

    public GovernanceManualReviewCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> listPendingReviews,
        Func<string, string?, int, string?, string?, string?, string?, string?, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> listReviewHistory,
        Func<string, string?, string, string, bool, string, string, CancellationToken, Task<GovernanceManualReviewItem>> submitReviewDecision)
    {
        _listPendingReviews = listPendingReviews;
        _listReviewHistory = listReviewHistory;
        _submitReviewDecision = submitReviewDecision;
    }

    public async Task<GovernancePendingReviewRefreshResult> RefreshPendingAsync(
        string baseUrl,
        string? apiKey,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var items = await _listPendingReviews(baseUrl, apiKey, limit, cancellationToken);
        var hintText = items.Count == 0
            ? "No pending review items."
            : $"Pending review items: {items.Count}";
        return new GovernancePendingReviewRefreshResult(items, hintText);
    }

    public Task<IReadOnlyList<GovernanceManualReviewItem>> RefreshHistoryAsync(
        string baseUrl,
        string? apiKey,
        string? runId,
        string? reviewer,
        string? status,
        string? dateFrom,
        string? dateTo,
        int limit = 120,
        CancellationToken cancellationToken = default)
    {
        var normalizedRunId = NormalizeOptional(runId);
        var normalizedReviewer = NormalizeOptional(reviewer);
        var normalizedStatus = NormalizeOptional(status);
        var normalizedDateFrom = NormalizeOptional(dateFrom);
        var normalizedDateTo = NormalizeOptional(dateTo);
        return _listReviewHistory(
            baseUrl,
            apiKey,
            limit,
            normalizedRunId,
            normalizedReviewer,
            normalizedStatus,
            normalizedDateFrom,
            normalizedDateTo,
            cancellationToken);
    }

    public Task<GovernanceManualReviewItem> SubmitDecisionAsync(
        string baseUrl,
        string? apiKey,
        GovernanceManualReviewItem selectedItem,
        bool approved,
        string? reviewer,
        string? comment,
        CancellationToken cancellationToken = default)
    {
        var runId = NormalizeRequired(selectedItem.RunId, "review run id is required");
        var reviewKey = NormalizeRequired(selectedItem.ReviewKey, "review key is required");
        return _submitReviewDecision(
            baseUrl,
            apiKey,
            runId,
            reviewKey,
            approved,
            NormalizeOptional(reviewer) ?? string.Empty,
            NormalizeOptional(comment) ?? string.Empty,
            cancellationToken);
    }

    private static string? NormalizeOptional(string? value)
    {
        var text = (value ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static string NormalizeRequired(string? value, string errorMessage)
    {
        var text = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new InvalidOperationException(errorMessage);
        }

        return text;
    }
}

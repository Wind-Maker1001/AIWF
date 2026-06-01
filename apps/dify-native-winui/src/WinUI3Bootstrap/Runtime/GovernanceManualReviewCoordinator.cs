using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernancePendingReviewRefreshResult(
    IReadOnlyList<GovernanceManualReviewItem> Items,
    string HintText);

public sealed record GovernanceManualReviewResumeResult(
    bool ReviewSaved,
    GovernanceManualReviewItem Item,
    WorkflowHttpResult? ResumedResponse,
    string StatusText,
    bool ResumeAttempted,
    bool ResumeSucceeded);

public sealed class GovernanceManualReviewCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> _listPendingReviews;
    private readonly Func<string, string?, int, string?, string?, string?, string?, string?, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> _listReviewHistory;
    private readonly Func<string, string?, string, string, bool, string, string, CancellationToken, Task<GovernanceManualReviewItem>> _submitReviewDecision;
    private readonly Func<string, string?, string, CancellationToken, Task<GovernanceWorkflowRunRecordDetail>> _getRunRecord;
    private readonly Func<string, string?, string, JsonObject, CancellationToken, Task<WorkflowHttpResult>> _runReference;
    private readonly Func<string, string?, string, string, JsonObject, CancellationToken, Task<WorkflowHttpResult>> _runFlow;

    public GovernanceManualReviewCoordinator(GovernanceBridgeClient client, WorkflowRunnerAdapter runnerAdapter)
        : this(
            client.ListManualReviewsAsync,
            client.ListManualReviewHistoryAsync,
            client.SubmitManualReviewAsync,
            client.GetWorkflowRunRecordAsync,
            runnerAdapter.RunWorkflowReferenceAsync,
            runnerAdapter.RunFlowAsync)
    {
    }

    public GovernanceManualReviewCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> listPendingReviews,
        Func<string, string?, int, string?, string?, string?, string?, string?, CancellationToken, Task<IReadOnlyList<GovernanceManualReviewItem>>> listReviewHistory,
        Func<string, string?, string, string, bool, string, string, CancellationToken, Task<GovernanceManualReviewItem>> submitReviewDecision,
        Func<string, string?, string, CancellationToken, Task<GovernanceWorkflowRunRecordDetail>> getRunRecord,
        Func<string, string?, string, JsonObject, CancellationToken, Task<WorkflowHttpResult>> runReference,
        Func<string, string?, string, string, JsonObject, CancellationToken, Task<WorkflowHttpResult>> runFlow)
    {
        _listPendingReviews = listPendingReviews;
        _listReviewHistory = listReviewHistory;
        _submitReviewDecision = submitReviewDecision;
        _getRunRecord = getRunRecord;
        _runReference = runReference;
        _runFlow = runFlow;
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

    public async Task<GovernanceManualReviewResumeResult> SubmitDecisionAndResumeAsync(
        string baseUrl,
        string? apiKey,
        GovernanceManualReviewItem selectedItem,
        bool approved,
        string? reviewer,
        string? comment,
        CancellationToken cancellationToken = default)
    {
        var item = await SubmitDecisionAsync(
            baseUrl,
            apiKey,
            selectedItem,
            approved,
            reviewer,
            comment,
            cancellationToken);

        var runId = NormalizeRequired(selectedItem.RunId, "review run id is required");
        GovernanceWorkflowRunRecordDetail record;
        try
        {
            record = await _getRunRecord(baseUrl, apiKey, runId, cancellationToken);
        }
        catch (Exception ex)
        {
            return new GovernanceManualReviewResumeResult(
                true,
                item,
                null,
                approved
                    ? $"Review approved, but automatic resume failed: {ex.Message}"
                    : $"Review rejected, but automatic resume failed: {ex.Message}",
                ResumeAttempted: true,
                ResumeSucceeded: false);
        }

        var replayPayload = BuildResumePayload(record, item, reviewer, comment);
        if (replayPayload is null)
        {
            return new GovernanceManualReviewResumeResult(
                true,
                item,
                null,
                approved
                    ? $"Review approved: {item.RunId} / {item.ReviewKey}"
                    : $"Review rejected: {item.RunId} / {item.ReviewKey}",
                ResumeAttempted: false,
                ResumeSucceeded: false);
        }

        WorkflowHttpResult resumed;
        if (string.Equals(record.RunRequestKind, "reference", StringComparison.OrdinalIgnoreCase))
        {
            resumed = await _runReference(
                baseUrl,
                apiKey,
                runId,
                replayPayload,
                cancellationToken);
        }
        else
        {
            var flow = (record.Payload["flow"]?.GetValue<string>() ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(flow))
            {
                return new GovernanceManualReviewResumeResult(
                    true,
                    item,
                    null,
                    approved
                        ? $"Review approved: {item.RunId} / {item.ReviewKey}"
                        : $"Review rejected: {item.RunId} / {item.ReviewKey}",
                    ResumeAttempted: false,
                    ResumeSucceeded: false);
            }

            resumed = await _runFlow(
                baseUrl,
                apiKey,
                runId,
                flow,
                replayPayload,
                cancellationToken);
        }

        return new GovernanceManualReviewResumeResult(
            true,
            item,
            resumed,
            resumed.IsSuccessStatusCode
                ? (approved ? "Review approved and run resumed." : "Review rejected and run resumed.")
                : (approved
                    ? $"Review approved, but automatic resume failed: {(int)resumed.StatusCode}"
                    : $"Review rejected, but automatic resume failed: {(int)resumed.StatusCode}"),
            ResumeAttempted: true,
            ResumeSucceeded: resumed.IsSuccessStatusCode);
    }

    private static JsonObject? BuildResumePayload(
        GovernanceWorkflowRunRecordDetail record,
        GovernanceManualReviewItem item,
        string? reviewer,
        string? comment)
    {
        var payload = CloneJsonObject(record.Payload);
        if (payload.Count == 0)
        {
            return null;
        }

        var paramsObject = payload["params"] as JsonObject is JsonObject existingParams
            ? CloneJsonObject(existingParams)
            : new JsonObject();
        var reviewBag = paramsObject["manual_review"] as JsonObject is JsonObject existingBag
            ? CloneJsonObject(existingBag)
            : new JsonObject();
        reviewBag[item.ReviewKey] = new JsonObject
        {
            ["approved"] = item.Approved,
            ["reviewer"] = string.IsNullOrWhiteSpace(reviewer) ? item.Reviewer : reviewer.Trim(),
            ["comment"] = string.IsNullOrWhiteSpace(comment) ? item.Comment : comment.Trim(),
        };
        paramsObject["manual_review"] = reviewBag;

        var actor = (payload["actor"]?.GetValue<string>() ?? string.Empty).Trim();
        var rulesetVersion = (payload["ruleset_version"]?.GetValue<string>() ?? string.Empty).Trim();
        if (string.Equals(record.RunRequestKind, "reference", StringComparison.OrdinalIgnoreCase))
        {
            var versionId = string.IsNullOrWhiteSpace(record.VersionId)
                ? record.PublishedVersionId
                : record.VersionId;
            if (string.IsNullOrWhiteSpace(versionId))
            {
                return null;
            }

            return new JsonObject
            {
                ["version_id"] = versionId,
                ["actor"] = string.IsNullOrWhiteSpace(actor) ? "reviewer" : actor,
                ["ruleset_version"] = string.IsNullOrWhiteSpace(rulesetVersion) ? "v1" : rulesetVersion,
                ["params"] = paramsObject,
            };
        }

        var flow = (payload["flow"]?.GetValue<string>() ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(flow))
        {
            return null;
        }

        return new JsonObject
        {
            ["actor"] = string.IsNullOrWhiteSpace(actor) ? "reviewer" : actor,
            ["ruleset_version"] = string.IsNullOrWhiteSpace(rulesetVersion) ? "v1" : rulesetVersion,
            ["params"] = paramsObject,
        };
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

    private static JsonObject CloneJsonObject(JsonObject? source)
    {
        return source is null
            ? new JsonObject()
            : JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }
}

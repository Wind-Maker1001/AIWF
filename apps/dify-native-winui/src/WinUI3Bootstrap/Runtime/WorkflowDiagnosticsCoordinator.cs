using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowDiagnosticsAggregateItem(
    string Chiplet,
    int Calls,
    int Failed,
    double FailureRate,
    double P50Seconds,
    double P95Seconds,
    double AverageSeconds,
    double RetryRate,
    double FallbackRate)
{
    public string DisplayText =>
        $"{Chiplet} | calls={Calls} | failed={Failed} ({FailureRate:P1}) | p95={P95Seconds:0.###}s | retry={RetryRate:P1} | fallback={FallbackRate:P1}";

    public override string ToString() => DisplayText;
}

public sealed record WorkflowDiagnosticsRefreshResult(
    IReadOnlyList<GovernanceTimelineEntry> CurrentRunTimeline,
    IReadOnlyList<WorkflowDiagnosticsAggregateItem> AggregateItems,
    string SummaryText,
    string StatusText);

public sealed class WorkflowDiagnosticsCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceWorkflowRunRecordItem>>> _listRunRecords;
    private readonly Func<string, string?, string, CancellationToken, Task<IReadOnlyList<GovernanceTimelineEntry>>> _getTimeline;
    private readonly WorkflowRunnerAdapter _runnerAdapter;

    public WorkflowDiagnosticsCoordinator(
        GovernanceBridgeClient client,
        WorkflowRunnerAdapter runnerAdapter)
        : this(client.ListWorkflowRunRecordsAsync, client.GetWorkflowRunTimelineAsync, runnerAdapter)
    {
    }

    public WorkflowDiagnosticsCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceWorkflowRunRecordItem>>> listRunRecords,
        Func<string, string?, string, CancellationToken, Task<IReadOnlyList<GovernanceTimelineEntry>>> getTimeline,
        WorkflowRunnerAdapter runnerAdapter)
    {
        _listRunRecords = listRunRecords;
        _getTimeline = getTimeline;
        _runnerAdapter = runnerAdapter;
    }

    public async Task<WorkflowDiagnosticsRefreshResult> RefreshAsync(
        string baseUrl,
        string? apiKey,
        string? accelUrl,
        string? runId,
        CancellationToken cancellationToken = default)
    {
        var normalizedRunId = (runId ?? string.Empty).Trim();
        var recordsTask = _listRunRecords(baseUrl, apiKey, 80, cancellationToken);
        var timelineTask = string.IsNullOrWhiteSpace(normalizedRunId)
            ? Task.FromResult<IReadOnlyList<GovernanceTimelineEntry>>(Array.Empty<GovernanceTimelineEntry>())
            : _getTimeline(baseUrl, apiKey, normalizedRunId, cancellationToken);
        var rustTask = FetchRustRuntimeItemsAsync(accelUrl, apiKey, cancellationToken);

        await Task.WhenAll(recordsTask, timelineTask, rustTask);

        var records = await recordsTask;
        var timeline = await timelineTask;
        var aggregated = BuildAggregateItems(records, await rustTask);
        var statusText = "Workflow diagnostics refreshed.";
        var summaryText = $"runs={records.Count} | current_run_steps={timeline.Count} | diagnostics={aggregated.Count}";
        return new WorkflowDiagnosticsRefreshResult(
            CurrentRunTimeline: timeline,
            AggregateItems: aggregated,
            SummaryText: summaryText,
            StatusText: statusText);
    }

    private async Task<IReadOnlyList<WorkflowDiagnosticsAggregateItem>> FetchRustRuntimeItemsAsync(
        string? accelUrl,
        string? apiKey,
        CancellationToken cancellationToken)
    {
        var endpoint = (accelUrl ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return Array.Empty<WorkflowDiagnosticsAggregateItem>();
        }

        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                endpoint,
                apiKey,
                "/operators/runtime_stats_v1",
                new JsonObject
                {
                    ["op"] = "summary",
                },
                cancellationToken);
            return ParseRustRuntimeItems(payload);
        }
        catch
        {
            return Array.Empty<WorkflowDiagnosticsAggregateItem>();
        }
    }

    internal static IReadOnlyList<WorkflowDiagnosticsAggregateItem> BuildAggregateItems(
        IReadOnlyList<GovernanceWorkflowRunRecordItem> records,
        IReadOnlyList<WorkflowDiagnosticsAggregateItem>? rustRuntimeItems = null)
    {
        var byChiplet = new Dictionary<string, List<double>>(StringComparer.Ordinal);
        var failedByChiplet = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            foreach (var step in record.Steps)
            {
                var key = (step.StepId ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(key))
                {
                    continue;
                }

                if (!byChiplet.TryGetValue(key, out var durations))
                {
                    durations = new List<double>();
                    byChiplet[key] = durations;
                }

                durations.Add(step.Seconds);
                if (!string.Equals(step.Status, "DONE", StringComparison.OrdinalIgnoreCase))
                {
                    failedByChiplet[key] = failedByChiplet.TryGetValue(key, out var current)
                        ? current + 1
                        : 1;
                }
            }
        }

        var items = byChiplet
            .OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
            .Select(pair =>
            {
                var durations = pair.Value
                    .Where(static value => double.IsFinite(value))
                    .OrderBy(static value => value)
                    .ToArray();
                var failed = failedByChiplet.TryGetValue(pair.Key, out var currentFailed) ? currentFailed : 0;
                var calls = durations.Length;
                return new WorkflowDiagnosticsAggregateItem(
                    Chiplet: pair.Key,
                    Calls: calls,
                    Failed: failed,
                    FailureRate: calls > 0 ? Math.Round((double)failed / calls, 4) : 0,
                    P50Seconds: Percentile(durations, 0.5),
                    P95Seconds: Percentile(durations, 0.95),
                    AverageSeconds: calls > 0 ? Math.Round(durations.Average(), 3) : 0,
                    RetryRate: 0,
                    FallbackRate: 0);
            })
            .ToList();

        if (rustRuntimeItems is { Count: > 0 })
        {
            items.AddRange(rustRuntimeItems.OrderBy(item => item.Chiplet, StringComparer.OrdinalIgnoreCase));
        }

        return items;
    }

    private static IReadOnlyList<WorkflowDiagnosticsAggregateItem> ParseRustRuntimeItems(JsonObject payload)
    {
        return (payload["items"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select(item =>
            {
                var operatorName = item["operator"]?.GetValue<string>() ?? string.Empty;
                var calls = ReadInt(item["calls"]);
                var errors = ReadInt(item["err"]);
                return new WorkflowDiagnosticsAggregateItem(
                    Chiplet: $"rust:{operatorName}",
                    Calls: calls,
                    Failed: errors,
                    FailureRate: calls > 0 ? Math.Round((double)errors / calls, 4) : 0,
                    P50Seconds: 0,
                    P95Seconds: Math.Round(ReadDouble(item["p95_ms"]) / 1000, 3),
                    AverageSeconds: Math.Round(ReadDouble(item["avg_ms"]) / 1000, 3),
                    RetryRate: 0,
                    FallbackRate: 0);
            })
            .Where(static item => !string.IsNullOrWhiteSpace(item.Chiplet))
            .ToArray() ?? Array.Empty<WorkflowDiagnosticsAggregateItem>();
    }

    private static double Percentile(double[] values, double q)
    {
        if (values.Length == 0)
        {
            return 0;
        }

        var position = Math.Clamp(q, 0, 1);
        var index = (int)Math.Ceiling(values.Length * position) - 1;
        return Math.Round(values[Math.Clamp(index, 0, values.Length - 1)], 3);
    }

    private static int ReadInt(JsonNode? value)
    {
        return value is JsonValue jsonValue && jsonValue.TryGetValue<int>(out var intValue)
            ? intValue
            : value is JsonValue longValue && longValue.TryGetValue<long>(out var asLong)
                ? (int)asLong
                : 0;
    }

    private static double ReadDouble(JsonNode? value)
    {
        if (value is not JsonValue jsonValue)
        {
            return 0;
        }

        if (jsonValue.TryGetValue<double>(out var doubleValue))
        {
            return doubleValue;
        }

        if (jsonValue.TryGetValue<int>(out var intValue))
        {
            return intValue;
        }

        if (jsonValue.TryGetValue<long>(out var longValue))
        {
            return longValue;
        }

        return 0;
    }
}

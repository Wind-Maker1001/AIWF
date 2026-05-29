using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowRunCompareSummary(
    string RunA,
    string RunB,
    string StatusA,
    string StatusB,
    bool OkA,
    bool OkB,
    int NodeCountA,
    int NodeCountB,
    int ChangedNodes);

public sealed record WorkflowRunCompareNodeDiffItem(
    string Id,
    string Type,
    string StatusA,
    string StatusB,
    bool StatusChanged,
    double SecondsA,
    double SecondsB,
    double SecondsDelta);

public sealed record WorkflowRunCompareResult(
    bool Ok,
    WorkflowRunCompareSummary Summary,
    IReadOnlyList<WorkflowRunCompareNodeDiffItem> NodeDiff);

public sealed record WorkflowRunBaselineRegressionResult(
    bool Ok,
    string BaselineId,
    string BaselineName,
    string BaselineRunId,
    string RunId,
    WorkflowRunCompareResult Compare,
    int ChangedNodes,
    int StatusFlipNodes,
    int PerfHotNodes);

public sealed record WorkflowRunLineageResult(
    bool Ok,
    string RunId,
    int NodeCount,
    int EdgeCount,
    string RawJson,
    string StatusMessage);

public sealed class WorkflowRunBaselineCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceRunBaselineItem>>> _listBaselines;
    private readonly Func<string, string?, string, string, string, string, string, string, CancellationToken, Task<GovernanceRunBaselineItem>> _saveBaseline;
    private readonly Func<string, string?, string, CancellationToken, Task<GovernanceWorkflowRunRecordDetail>> _getRunRecord;
    private readonly Func<DateTimeOffset> _now;
    private readonly Func<string> _randomHex;

    public WorkflowRunBaselineCoordinator(GovernanceBridgeClient client)
        : this(
            client.ListRunBaselinesAsync,
            client.SaveRunBaselineAsync,
            client.GetWorkflowRunRecordAsync)
    {
    }

    public WorkflowRunBaselineCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceRunBaselineItem>>> listBaselines,
        Func<string, string?, string, string, string, string, string, string, CancellationToken, Task<GovernanceRunBaselineItem>> saveBaseline,
        Func<string, string?, string, CancellationToken, Task<GovernanceWorkflowRunRecordDetail>> getRunRecord,
        Func<DateTimeOffset>? now = null,
        Func<string>? randomHex = null)
    {
        _listBaselines = listBaselines;
        _saveBaseline = saveBaseline;
        _getRunRecord = getRunRecord;
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _randomHex = randomHex ?? (() => Guid.NewGuid().ToString("N")[..8]);
    }

    public async Task<IReadOnlyList<GovernanceRunBaselineItem>> RefreshBaselinesAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        return await _listBaselines(baseUrl, apiKey, 120, cancellationToken);
    }

    public async Task<GovernanceRunBaselineItem> SaveCurrentRunAsBaselineAsync(
        string baseUrl,
        string? apiKey,
        string runId,
        string? name = null,
        string? notes = null,
        CancellationToken cancellationToken = default)
    {
        var record = await _getRunRecord(baseUrl, apiKey, runId, cancellationToken);
        var effectiveName = string.IsNullOrWhiteSpace(name)
            ? $"baseline_{runId[..Math.Min(8, runId.Length)]}"
            : name.Trim();
        return await _saveBaseline(
            baseUrl,
            apiKey,
            BuildBaselineId(),
            effectiveName,
            record.RunId,
            record.WorkflowId,
            _now().ToString("O"),
            (notes ?? string.Empty).Trim(),
            cancellationToken);
    }

    public async Task<WorkflowRunCompareResult> CompareRunsAsync(
        string baseUrl,
        string? apiKey,
        string runA,
        string runB,
        CancellationToken cancellationToken = default)
    {
        var recordA = await _getRunRecord(baseUrl, apiKey, runA, cancellationToken);
        var recordB = await _getRunRecord(baseUrl, apiKey, runB, cancellationToken);
        return BuildRunCompare(recordA, recordB);
    }

    public async Task<WorkflowRunBaselineRegressionResult> CompareRunWithBaselineAsync(
        string baseUrl,
        string? apiKey,
        string runId,
        string? baselineId = null,
        CancellationToken cancellationToken = default)
    {
        var baselines = await _listBaselines(baseUrl, apiKey, 200, cancellationToken);
        var selectedBaseline = string.IsNullOrWhiteSpace(baselineId)
            ? baselines.FirstOrDefault()
            : baselines.FirstOrDefault(item => string.Equals(item.BaselineId, baselineId.Trim(), StringComparison.Ordinal));
        if (selectedBaseline is null)
        {
            throw new InvalidOperationException(string.IsNullOrWhiteSpace(baselineId)
                ? "baseline not found"
                : $"baseline not found: {baselineId}");
        }

        var compare = await CompareRunsAsync(baseUrl, apiKey, selectedBaseline.RunId, runId, cancellationToken);
        var changed = compare.NodeDiff.Where(item => item.StatusChanged || Math.Abs(item.SecondsDelta) > 0.001).ToArray();
        var statusFlip = changed.Count(item => item.StatusChanged && !string.IsNullOrWhiteSpace(item.StatusA) && !string.IsNullOrWhiteSpace(item.StatusB));
        var perfHot = changed.Count(item => !string.IsNullOrWhiteSpace(item.StatusA) && !string.IsNullOrWhiteSpace(item.StatusB) && item.SecondsDelta > 0.5);
        return new WorkflowRunBaselineRegressionResult(
            Ok: compare.Ok,
            BaselineId: selectedBaseline.BaselineId,
            BaselineName: selectedBaseline.Name,
            BaselineRunId: selectedBaseline.RunId,
            RunId: runId,
            Compare: compare,
            ChangedNodes: changed.Length,
            StatusFlipNodes: statusFlip,
            PerfHotNodes: perfHot);
    }

    public async Task<WorkflowRunLineageResult> LoadLineageAsync(
        string baseUrl,
        string? apiKey,
        string runId,
        string currentRawResponseJson,
        string currentRunId,
        CancellationToken cancellationToken = default)
    {
        var record = await _getRunRecord(baseUrl, apiKey, runId, cancellationToken);
        var lineage = ResolveLineage(record.ResultPayload);
        if (lineage is null
            && string.Equals(runId.Trim(), (currentRunId ?? string.Empty).Trim(), StringComparison.Ordinal))
        {
            lineage = ResolveLineage(ParseRawResponse(currentRawResponseJson));
        }

        if (lineage is null)
        {
            return new WorkflowRunLineageResult(
                false,
                runId,
                0,
                0,
                "{}",
                "Lineage is not available for the selected run.");
        }

        var nodeCount = lineage["nodes"] is JsonArray nodes ? nodes.Count : lineage["node_count"]?.GetValue<int?>() ?? 0;
        var edgeCount = lineage["edges"] is JsonArray edges ? edges.Count : lineage["edge_count"]?.GetValue<int?>() ?? 0;
        return new WorkflowRunLineageResult(
            true,
            runId,
            nodeCount,
            edgeCount,
            lineage.ToJsonString(new JsonSerializerOptions { WriteIndented = true }),
            $"Lineage loaded: nodes={nodeCount}, edges={edgeCount}");
    }

    internal static WorkflowRunCompareResult BuildRunCompare(
        GovernanceWorkflowRunRecordDetail recordA,
        GovernanceWorkflowRunRecordDetail recordB)
    {
        var mapA = recordA.Steps
            .GroupBy(item => item.StepId, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Last(), StringComparer.Ordinal);
        var mapB = recordB.Steps
            .GroupBy(item => item.StepId, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Last(), StringComparer.Ordinal);
        var stepIds = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var step in recordA.Steps.Concat(recordB.Steps))
        {
            var id = (step.StepId ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(id) || !seen.Add(id))
            {
                continue;
            }

            stepIds.Add(id);
        }

        var nodeDiff = stepIds
            .Select(stepId =>
            {
                mapA.TryGetValue(stepId, out var stepA);
                mapB.TryGetValue(stepId, out var stepB);
                var statusA = stepA?.Status ?? string.Empty;
                var statusB = stepB?.Status ?? string.Empty;
                var secondsA = stepA?.Seconds ?? 0;
                var secondsB = stepB?.Seconds ?? 0;
                return new WorkflowRunCompareNodeDiffItem(
                    Id: stepId,
                    Type: stepId,
                    StatusA: statusA,
                    StatusB: statusB,
                    StatusChanged: !string.Equals(statusA, statusB, StringComparison.Ordinal),
                    SecondsA: secondsA,
                    SecondsB: secondsB,
                    SecondsDelta: Math.Round(secondsB - secondsA, 3));
            })
            .ToArray();

        return new WorkflowRunCompareResult(
            true,
            new WorkflowRunCompareSummary(
                RunA: recordA.RunId,
                RunB: recordB.RunId,
                StatusA: recordA.Status,
                StatusB: recordB.Status,
                OkA: recordA.Ok,
                OkB: recordB.Ok,
                NodeCountA: recordA.Steps.Count,
                NodeCountB: recordB.Steps.Count,
                ChangedNodes: nodeDiff.Count(item => item.StatusChanged || Math.Abs(item.SecondsDelta) > 0.001)),
            nodeDiff);
    }

    private string BuildBaselineId()
    {
        return $"{_now():yyyyMMddHHmmss}_{_randomHex()}";
    }

    private static JsonObject? ParseRawResponse(string raw)
    {
        try
        {
            return JsonNode.Parse(raw) as JsonObject;
        }
        catch
        {
            return null;
        }
    }

    private static JsonObject? ResolveLineage(JsonObject? source)
    {
        return source?["lineage"] as JsonObject;
    }
}

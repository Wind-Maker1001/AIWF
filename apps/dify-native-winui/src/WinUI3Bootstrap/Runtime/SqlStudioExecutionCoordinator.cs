using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record SqlStudioPreviewExecutionResult(
    SqlTextDraft EffectiveTextDraft,
    SqlPreviewState PreviewState,
    string RawJson,
    bool Success,
    int RowCount,
    string ExecutedSql);

public sealed record SqlStudioExplainExecutionResult(
    SqlTextDraft EffectiveTextDraft,
    string ExplainText,
    string StatusMessage,
    bool Success);

public sealed class SqlStudioExecutionCoordinator
{
    private readonly WorkflowRunnerAdapter _runnerAdapter;

    public SqlStudioExecutionCoordinator(WorkflowRunnerAdapter runnerAdapter)
    {
        _runnerAdapter = runnerAdapter;
    }

    public async Task<SqlStudioPreviewExecutionResult> PreviewAsync(
        SqlConnectionProfile profile,
        SqlBuilderDraft draft,
        SqlTextDraft textDraft,
        string bridgeBaseUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        var effectiveTextDraft = SqlStudioDraftController.SyncGeneratedSql(draft, profile, textDraft);
        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                profile.ResolveAccelUrl(bridgeBaseUrl),
                apiKey,
                "/operators/load_rows_v3",
                BuildLoadRowsPayload(profile, effectiveTextDraft.Text, Math.Max(1, draft.Limit), maxRetries: 2, retryBackoffMs: 150),
                cancellationToken);

            var previewState = SqlStudioResultMapper.FromLoadRowsResponse(payload, effectiveTextDraft.Text);
            return new SqlStudioPreviewExecutionResult(
                effectiveTextDraft,
                previewState,
                previewState.RawJson,
                previewState.Ok,
                previewState.GridRows.Count,
                effectiveTextDraft.Text);
        }
        catch (Exception ex)
        {
            return new SqlStudioPreviewExecutionResult(
                effectiveTextDraft,
                new SqlPreviewState(
                    Ok: false,
                    StatusText: $"Preview failed: {ex.Message}",
                    GeneratedSql: effectiveTextDraft.Text,
                    RawJson: string.Empty,
                    Diagnostics: ex.Message,
                    RowDisplayItems: Array.Empty<string>(),
                    ColumnHeaders: Array.Empty<string>(),
                    GridRows: Array.Empty<IReadOnlyList<string>>()),
                RawJson: string.Empty,
                Success: false,
                RowCount: 0,
                ExecutedSql: effectiveTextDraft.Text);
        }
    }

    public async Task<SqlStudioExplainExecutionResult> ExplainAsync(
        SqlConnectionProfile profile,
        SqlBuilderDraft draft,
        SqlTextDraft textDraft,
        string bridgeBaseUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        var effectiveTextDraft = SqlStudioDraftController.SyncGeneratedSql(draft, profile, textDraft);
        var explainPrefix = profile.NormalizedSourceType switch
        {
            SqlConnectionProfile.SqlServer => "SET SHOWPLAN_TEXT ON; ",
            SqlConnectionProfile.Postgres => "EXPLAIN ANALYZE ",
            _ => "EXPLAIN QUERY PLAN ",
        };

        try
        {
            var payload = await _runnerAdapter.PostJsonAsync(
                profile.ResolveAccelUrl(bridgeBaseUrl),
                apiKey,
                "/operators/load_rows_v3",
                BuildLoadRowsPayload(profile, explainPrefix + effectiveTextDraft.Text, 200, maxRetries: 1, retryBackoffMs: 100),
                cancellationToken);

            return new SqlStudioExplainExecutionResult(
                effectiveTextDraft,
                FormatExplainText(payload),
                "执行计划已加载。",
                true);
        }
        catch (Exception ex)
        {
            return new SqlStudioExplainExecutionResult(
                effectiveTextDraft,
                $"EXPLAIN failed: {ex.Message}",
                $"执行计划失败: {ex.Message}",
                false);
        }
    }

    private static JsonObject BuildLoadRowsPayload(
        SqlConnectionProfile profile,
        string query,
        int limit,
        int maxRetries,
        int retryBackoffMs)
    {
        return new JsonObject
        {
            ["source_type"] = profile.NormalizedSourceType,
            ["source"] = profile.BuildRuntimeSource(),
            ["query"] = query,
            ["limit"] = limit,
            ["max_retries"] = maxRetries,
            ["retry_backoff_ms"] = retryBackoffMs,
        };
    }

    private static string FormatExplainText(JsonObject payload)
    {
        var rows = payload["rows"] as JsonArray;
        var lines = rows?.OfType<JsonObject>()
            .Select(row => string.Join(" | ", row.Select(kv => kv.Value?.ToString() ?? string.Empty)))
            .ToArray() ?? Array.Empty<string>();

        return lines.Length > 0
            ? string.Join("\n", lines)
            : payload.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }
}

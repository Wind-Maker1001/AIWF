using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class GovernanceAuditExportSupport
{
    public static JsonObject BuildExportEnvelope(
        string runId,
        string actionFilter,
        GovernanceAuditRefreshResult state)
    {
        return new JsonObject
        {
            ["exported_at"] = DateTimeOffset.UtcNow.ToString("O"),
            ["run_id"] = runId ?? string.Empty,
            ["action_filter"] = actionFilter ?? string.Empty,
            ["summary_text"] = state.SummaryText,
            ["runs"] = new JsonArray(state.Runs.Select(run => new JsonObject
            {
                ["run_id"] = run.RunId,
                ["workflow_id"] = run.WorkflowId,
                ["status"] = run.Status,
                ["ok"] = run.Ok,
                ["timestamp"] = run.Timestamp,
            }).ToArray()),
            ["timeline"] = new JsonArray(state.Timeline.Select(item => new JsonObject
            {
                ["node_id"] = item.NodeId,
                ["type"] = item.Type,
                ["status"] = item.Status,
                ["started_at"] = item.StartedAt,
                ["ended_at"] = item.EndedAt,
                ["seconds"] = item.Seconds,
            }).ToArray()),
            ["failure_summary"] = new JsonArray(state.Failures.Select(item => new JsonObject
            {
                ["node_type"] = item.NodeType,
                ["failed"] = item.Failed,
                ["sample"] = item.Sample,
            }).ToArray()),
            ["audit_events"] = new JsonArray(state.AuditEvents.Select(item => new JsonObject
            {
                ["timestamp"] = item.Timestamp,
                ["action"] = item.Action,
                ["detail_summary"] = item.DetailSummary,
            }).ToArray()),
        };
    }

    public static string RenderMarkdown(
        string runId,
        string actionFilter,
        GovernanceAuditRefreshResult state)
    {
        var lines = new List<string>
        {
            "# AIWF Governance Audit Export",
            string.Empty,
            $"- Exported At: {DateTimeOffset.UtcNow:O}",
            $"- Run ID Filter: {(string.IsNullOrWhiteSpace(runId) ? "-" : runId)}",
            $"- Action Filter: {(string.IsNullOrWhiteSpace(actionFilter) ? "-" : actionFilter)}",
            $"- Summary: {state.SummaryText}",
            string.Empty,
            "## Timeline",
            string.Empty,
            "| Node | Type | Status | Seconds |",
            "|---|---|---|---:|",
        };

        foreach (var item in state.Timeline)
        {
            lines.Add($"| {item.NodeId} | {item.Type} | {item.Status} | {item.Seconds:0.###} |");
        }

        lines.Add(string.Empty);
        lines.Add("## Failure Summary");
        lines.Add(string.Empty);
        lines.Add("| Node Type | Failed | Sample |");
        lines.Add("|---|---:|---|");
        foreach (var item in state.Failures)
        {
            lines.Add($"| {item.NodeType} | {item.Failed} | {EscapePipe(item.Sample)} |");
        }

        lines.Add(string.Empty);
        lines.Add("## Audit Events");
        lines.Add(string.Empty);
        lines.Add("| Timestamp | Action | Detail |");
        lines.Add("|---|---|---|");
        foreach (var item in state.AuditEvents)
        {
            lines.Add($"| {item.Timestamp} | {item.Action} | {EscapePipe(item.DetailSummary)} |");
        }

        return string.Join("\n", lines) + "\n";
    }

    private static string EscapePipe(string value)
    {
        return (value ?? string.Empty).Replace("|", "\\|", StringComparison.Ordinal);
    }
}

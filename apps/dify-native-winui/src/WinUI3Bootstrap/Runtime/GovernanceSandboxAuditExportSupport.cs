using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class GovernanceSandboxAuditExportSupport
{
    public static JsonObject BuildExportEnvelope(GovernanceSandboxAlertRefreshResult state)
    {
        return new JsonObject
        {
            ["exported_at"] = DateTimeOffset.UtcNow.ToString("O"),
            ["total"] = state.Health.Total,
            ["rules"] = state.Rules.DeepClone(),
            ["health"] = new JsonObject
            {
                ["level"] = state.Health.Level,
                ["total"] = state.Health.Total,
                ["thresholds"] = new JsonObject
                {
                    ["yellow"] = state.Health.Yellow,
                    ["red"] = state.Health.Red,
                },
                ["dedup_window_sec"] = state.Health.DedupWindowSec,
                ["suppressed"] = state.Health.Suppressed,
                ["suppressed_dedup"] = state.Health.SuppressedDedup,
                ["suppressed_whitelist"] = state.Health.SuppressedWhitelist,
                ["suppressed_muted"] = state.Health.SuppressedMuted,
            },
            ["by_node"] = new JsonArray(state.ByNode.Select(item => new JsonObject
            {
                ["node_type"] = item.NodeType,
                ["node_id"] = item.NodeId,
                ["count"] = item.Count,
                ["last_run_id"] = item.LastRunId,
                ["last_ts"] = item.LastTimestamp,
            }).ToArray()),
            ["items"] = new JsonArray(state.Items.Select(item => item.DeepClone()).ToArray()),
        };
    }

    public static string RenderMarkdown(GovernanceSandboxAlertRefreshResult state)
    {
        var lines = new List<string>
        {
            "# AIWF Sandbox Audit Report",
            string.Empty,
            $"- Exported At: {DateTimeOffset.UtcNow:O}",
            $"- Level: {state.Health.Level.ToUpperInvariant()}",
            $"- Total Alerts: {state.Health.Total}",
            $"- Thresholds: yellow={state.Health.Yellow}, red={state.Health.Red}",
            $"- Suppressed: {state.Health.Suppressed} (dedup={state.Health.SuppressedDedup}, whitelist={state.Health.SuppressedWhitelist}, muted={state.Health.SuppressedMuted})",
            string.Empty,
            "| Node | Count | Last Run |",
            "|---|---:|---|",
        };

        foreach (var item in state.ByNode)
        {
            lines.Add($"| {item.NodeType}({item.NodeId}) | {item.Count} | {item.LastRunId} |");
        }

        return string.Join("\n", lines) + "\n";
    }
}

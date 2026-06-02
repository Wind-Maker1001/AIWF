using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceSandboxAlertRow(
    string NodeType,
    string NodeId,
    int Count,
    string LastRunId,
    string LastTimestamp)
{
    public string DisplayText => $"{NodeType}({NodeId}) | count={Count} | last={LastRunId}";

    public override string ToString() => DisplayText;
}

public sealed record GovernanceSandboxAlertHealth(
    string Level,
    int Total,
    int Yellow,
    int Red,
    int DedupWindowSec,
    int Suppressed,
    int SuppressedDedup,
    int SuppressedWhitelist,
    int SuppressedMuted)
{
    public string DisplayText =>
        $"Sandbox health: {Level.ToUpperInvariant()} | total={Total} | suppressed={Suppressed} | thresholds y={Yellow}, r={Red} | dedup={DedupWindowSec}s";
}

public sealed record GovernanceSandboxAlertRefreshResult(
    IReadOnlyList<JsonObject> Items,
    IReadOnlyList<GovernanceSandboxAlertRow> ByNode,
    JsonObject Rules,
    GovernanceSandboxAlertHealth Health);

public sealed class GovernanceSandboxAlertCoordinator
{
    private readonly WorkflowRunAuditStoreService _runAuditStoreService;

    public GovernanceSandboxAlertCoordinator(WorkflowRunAuditStoreService runAuditStoreService)
    {
        _runAuditStoreService = runAuditStoreService;
    }

    public GovernanceSandboxAlertRefreshResult Refresh(
        int limit,
        JsonObject rules,
        int yellowThreshold,
        int redThreshold,
        int dedupWindowSec)
    {
        var runs = _runAuditStoreService.ListRuns(limit);
        return BuildFromRuns(runs, rules, yellowThreshold, redThreshold, dedupWindowSec);
    }

    internal static GovernanceSandboxAlertRefreshResult BuildFromRuns(
        IReadOnlyList<GovernanceWorkflowRunRecordDetail> runs,
        JsonObject rules,
        int yellowThreshold,
        int redThreshold,
        int dedupWindowSec)
    {
        var normalizedRules = NormalizeRules(rules);
        var allItems = new List<JsonObject>();
        foreach (var run in runs)
        {
            foreach (var item in ExtractViolations(run))
            {
                allItems.Add(item);
            }
        }

        var filtered = new List<JsonObject>();
        var suppressedWhitelist = 0;
        var suppressedMuted = 0;
        foreach (var item in allItems)
        {
            var reason = SuppressionReason(item, normalizedRules, DateTimeOffset.UtcNow);
            if (string.IsNullOrWhiteSpace(reason))
            {
                filtered.Add(item);
                continue;
            }

            if (string.Equals(reason, "whitelist", StringComparison.Ordinal))
            {
                suppressedWhitelist += 1;
            }
            else if (string.Equals(reason, "muted", StringComparison.Ordinal))
            {
                suppressedMuted += 1;
            }
        }

        var dedup = DedupSandboxItems(filtered, dedupWindowSec);
        var byNode = dedup.Items
            .GroupBy(item => $"{ReadString(item["node_type"])}::{ReadString(item["node_id"])}", StringComparer.Ordinal)
            .Select(group =>
            {
                var latest = group.OrderByDescending(item => ReadString(item["ts"])).First();
                return new GovernanceSandboxAlertRow(
                    NodeType: ReadString(latest["node_type"]),
                    NodeId: ReadString(latest["node_id"]),
                    Count: group.Count(),
                    LastRunId: ReadString(latest["run_id"]),
                    LastTimestamp: ReadString(latest["ts"]));
            })
            .OrderByDescending(item => item.Count)
            .Take(80)
            .ToArray();

        var health = new GovernanceSandboxAlertHealth(
            Level: dedup.Items.Count >= redThreshold ? "red" : dedup.Items.Count >= yellowThreshold ? "yellow" : "green",
            Total: dedup.Items.Count,
            Yellow: yellowThreshold,
            Red: redThreshold,
            DedupWindowSec: dedupWindowSec,
            Suppressed: dedup.Suppressed + suppressedWhitelist + suppressedMuted,
            SuppressedDedup: dedup.Suppressed,
            SuppressedWhitelist: suppressedWhitelist,
            SuppressedMuted: suppressedMuted);

        return new GovernanceSandboxAlertRefreshResult(
            Items: dedup.Items,
            ByNode: byNode,
            Rules: normalizedRules,
            Health: health);
    }

    private static IReadOnlyList<JsonObject> ExtractViolations(GovernanceWorkflowRunRecordDetail run)
    {
        var direct = (run.ResultPayload["violations"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select(item => new JsonObject
            {
                ["ts"] = run.Timestamp,
                ["run_id"] = run.RunId,
                ["workflow_id"] = run.WorkflowId,
                ["node_id"] = item["node_id"]?.DeepClone(),
                ["node_type"] = item["node_type"]?.DeepClone(),
                ["error"] = item["error"]?.DeepClone(),
            })
            .ToList();
        if (direct is { Count: > 0 })
        {
            return direct;
        }

        return (run.ResultPayload["node_runs"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select(node =>
            {
                var error = ReadString(node["error"]);
                var detail = ReadString((node["output"] as JsonObject)?["detail"]);
                var code = SandboxAlertCode(error, detail);
                if (string.IsNullOrWhiteSpace(code))
                {
                    return null;
                }

                return new JsonObject
                {
                    ["ts"] = run.Timestamp,
                    ["run_id"] = run.RunId,
                    ["workflow_id"] = run.WorkflowId,
                    ["node_id"] = ReadString(node["id"]),
                    ["node_type"] = ReadString(node["type"]),
                    ["error"] = error ?? detail ?? string.Empty,
                };
            })
            .Where(static item => item is not null)
            .Cast<JsonObject>()
            .ToArray() ?? Array.Empty<JsonObject>();
    }

    private static string SandboxAlertCode(string? error, string? detail)
    {
        var text = $"{error} {detail}";
        var match = System.Text.RegularExpressions.Regex.Match(
            text,
            "(sandbox_(?:limit_exceeded|egress_blocked)(?::[a-z_]+)?)",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value.ToLowerInvariant() : string.Empty;
    }

    private static JsonObject NormalizeRules(JsonObject rules)
    {
        var normalized = new JsonObject
        {
            ["whitelist_codes"] = new JsonArray(),
            ["whitelist_node_types"] = new JsonArray(),
            ["whitelist_keys"] = new JsonArray(),
            ["mute_until_by_key"] = new JsonObject(),
        };

        void FillStringArray(string key)
        {
            if (rules[key] is JsonArray array)
            {
                foreach (var item in array.OfType<JsonValue>())
                {
                    if (item.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text))
                    {
                        ((JsonArray)normalized[key]!).Add(text.Trim().ToLowerInvariant());
                    }
                }
            }
        }

        FillStringArray("whitelist_codes");
        FillStringArray("whitelist_node_types");
        FillStringArray("whitelist_keys");

        if (rules["mute_until_by_key"] is JsonObject muteMap)
        {
            foreach (var property in muteMap)
            {
                var key = property.Key.Trim().ToLowerInvariant();
                var value = ReadString(property.Value);
                if (string.IsNullOrWhiteSpace(key) || string.IsNullOrWhiteSpace(value))
                {
                    continue;
                }

                ((JsonObject)normalized["mute_until_by_key"]!)[key] = value;
            }
        }

        return normalized;
    }

    private static string SuppressionReason(JsonObject item, JsonObject rules, DateTimeOffset now)
    {
        var code = SandboxAlertCode(ReadString(item["error"]), string.Empty);
        var nodeType = ReadString(item["node_type"]).ToLowerInvariant();
        if (((JsonArray)rules["whitelist_codes"]!).Any(entry => string.Equals(ReadString(entry), code, StringComparison.Ordinal)))
        {
            return "whitelist";
        }
        if (((JsonArray)rules["whitelist_node_types"]!).Any(entry => string.Equals(ReadString(entry), nodeType, StringComparison.Ordinal)))
        {
            return "whitelist";
        }
        var variants = SandboxRuleKeyVariants(item, code);
        if (((JsonArray)rules["whitelist_keys"]!).Any(entry => variants.Contains(ReadString(entry), StringComparer.Ordinal)))
        {
            return "whitelist";
        }
        foreach (var variant in variants)
        {
            var value = ReadString(((JsonObject)rules["mute_until_by_key"]!)[variant]);
            if (DateTimeOffset.TryParse(value, out var until) && now < until)
            {
                return "muted";
            }
        }
        return string.Empty;
    }

    private static (IReadOnlyList<JsonObject> Items, int Suppressed) DedupSandboxItems(
        IReadOnlyList<JsonObject> items,
        int dedupWindowSec)
    {
        if (dedupWindowSec <= 0)
        {
            return (items, 0);
        }

        var sorted = items
            .OrderBy(item => ReadString(item["ts"]), StringComparer.Ordinal)
            .ToArray();
        var lastByKey = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        var kept = new List<JsonObject>();
        var suppressed = 0;
        foreach (var item in sorted)
        {
            var code = SandboxAlertCode(ReadString(item["error"]), string.Empty);
            var key = $"{ReadString(item["node_type"])}::{ReadString(item["node_id"])}::{code}";
            if (DateTimeOffset.TryParse(ReadString(item["ts"]), out var current)
                && lastByKey.TryGetValue(key, out var previous)
                && (current - previous).TotalSeconds < dedupWindowSec)
            {
                suppressed += 1;
                continue;
            }

            if (DateTimeOffset.TryParse(ReadString(item["ts"]), out var parsedCurrent))
            {
                lastByKey[key] = parsedCurrent;
            }
            kept.Add(item);
        }

        kept.Reverse();
        return (kept, suppressed);
    }

    private static IReadOnlyList<string> SandboxRuleKeyVariants(JsonObject item, string code)
    {
        var nodeType = string.IsNullOrWhiteSpace(ReadString(item["node_type"])) ? "*" : ReadString(item["node_type"]).ToLowerInvariant();
        var nodeId = string.IsNullOrWhiteSpace(ReadString(item["node_id"])) ? "*" : ReadString(item["node_id"]).ToLowerInvariant();
        return
        [
            $"{nodeType}::{nodeId}::{code}",
            $"{nodeType}::{nodeId}::*",
            $"{nodeType}::*::{code}",
            $"{nodeType}::*::*",
            $"*::{nodeId}::{code}",
            $"*::{nodeId}::*",
            $"*::*::{code}",
            $"*::*::*",
        ];
    }

    private static string ReadString(JsonNode? node)
    {
        return node is JsonValue value && value.TryGetValue<string>(out var text)
            ? text ?? string.Empty
            : string.Empty;
    }
}

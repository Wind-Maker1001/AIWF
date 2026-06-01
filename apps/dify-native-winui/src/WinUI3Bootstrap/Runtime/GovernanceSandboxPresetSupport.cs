using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceSandboxPresetViewState(
    int Yellow,
    int Red,
    int DedupWindowSec,
    string WhitelistCodes,
    string WhitelistNodeTypes);

public static class GovernanceSandboxPresetSupport
{
    public static GovernanceSandboxPresetViewState BalancedPreset { get; } = new(
        Yellow: 1,
        Red: 3,
        DedupWindowSec: 600,
        WhitelistCodes: string.Empty,
        WhitelistNodeTypes: string.Empty);

    public static GovernanceSandboxPresetViewState StrictPreset { get; } = BalancedPreset with
    {
        Red = 2,
        DedupWindowSec = 60,
    };

    public static GovernanceSandboxPresetViewState LoosePreset { get; } = BalancedPreset with
    {
        Yellow = 3,
        Red = 8,
        DedupWindowSec = 1800,
        WhitelistCodes = "sandbox_limit_exceeded:output",
    };

    public static GovernanceSandboxPresetViewState ResolvePreset(string? name)
    {
        return (name ?? string.Empty).Trim().ToLowerInvariant() switch
        {
            "strict" => StrictPreset,
            "loose" => LoosePreset,
            _ => BalancedPreset,
        };
    }

    public static IReadOnlyList<string> ParseCsvList(string? raw)
    {
        return (raw ?? string.Empty)
            .Split([';', ','], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static item => item.ToLowerInvariant())
            .Where(static item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    public static JsonObject BuildRules(
        string? whitelistCodes,
        string? whitelistNodeTypes,
        JsonObject? currentRules = null)
    {
        var result = currentRules?.DeepClone() as JsonObject ?? new JsonObject();
        result["whitelist_codes"] = ToJsonArray(ParseCsvList(whitelistCodes));
        result["whitelist_node_types"] = ToJsonArray(ParseCsvList(whitelistNodeTypes));
        if (result["whitelist_keys"] is not JsonArray)
        {
            result["whitelist_keys"] = new JsonArray();
        }
        if (result["mute_until_by_key"] is not JsonObject)
        {
            result["mute_until_by_key"] = new JsonObject();
        }
        return result;
    }

    public static GovernanceSandboxPresetViewState FromRulesAndAutoFix(
        JsonObject? rules,
        string? yellowText,
        string? redText,
        string? dedupText)
    {
        return new GovernanceSandboxPresetViewState(
            Yellow: ParsePositiveInt(yellowText, BalancedPreset.Yellow, min: 1),
            Red: ParsePositiveInt(redText, BalancedPreset.Red, min: 2),
            DedupWindowSec: ParsePositiveInt(dedupText, BalancedPreset.DedupWindowSec, min: 0),
            WhitelistCodes: string.Join(",", (rules?["whitelist_codes"] as JsonArray)?.OfType<JsonValue>().Select(static item => item.GetValue<string>()) ?? Array.Empty<string>()),
            WhitelistNodeTypes: string.Join(",", (rules?["whitelist_node_types"] as JsonArray)?.OfType<JsonValue>().Select(static item => item.GetValue<string>()) ?? Array.Empty<string>()));
    }

    public static JsonObject BuildPresetPayload(GovernanceSandboxPresetViewState state)
    {
        return new JsonObject
        {
            ["thresholds"] = new JsonObject
            {
                ["yellow"] = state.Yellow,
                ["red"] = state.Red,
            },
            ["dedup_window_sec"] = state.DedupWindowSec,
            ["rules"] = new JsonObject
            {
                ["whitelist_codes"] = ToJsonArray(ParseCsvList(state.WhitelistCodes)),
                ["whitelist_node_types"] = ToJsonArray(ParseCsvList(state.WhitelistNodeTypes)),
                ["whitelist_keys"] = new JsonArray(),
                ["mute_until_by_key"] = new JsonObject(),
            }
        };
    }

    public static GovernanceSandboxPresetViewState ParsePresetPayload(JsonObject payload)
    {
        var thresholds = payload["thresholds"] as JsonObject;
        var rules = payload["rules"] as JsonObject;
        return new GovernanceSandboxPresetViewState(
            Yellow: thresholds?["yellow"]?.GetValue<int?>() ?? BalancedPreset.Yellow,
            Red: thresholds?["red"]?.GetValue<int?>() ?? BalancedPreset.Red,
            DedupWindowSec: payload["dedup_window_sec"]?.GetValue<int?>() ?? BalancedPreset.DedupWindowSec,
            WhitelistCodes: string.Join(",", (rules?["whitelist_codes"] as JsonArray)?.OfType<JsonValue>().Select(static item => item.GetValue<string>()) ?? Array.Empty<string>()),
            WhitelistNodeTypes: string.Join(",", (rules?["whitelist_node_types"] as JsonArray)?.OfType<JsonValue>().Select(static item => item.GetValue<string>()) ?? Array.Empty<string>()));
    }

    private static int ParsePositiveInt(string? raw, int fallback, int min)
    {
        return int.TryParse((raw ?? string.Empty).Trim(), out var parsed)
            ? Math.Max(min, parsed)
            : fallback;
    }

    private static JsonArray ToJsonArray(IReadOnlyList<string> items)
    {
        var result = new JsonArray();
        foreach (var item in items)
        {
            result.Add(JsonValue.Create(item));
        }

        return result;
    }
}

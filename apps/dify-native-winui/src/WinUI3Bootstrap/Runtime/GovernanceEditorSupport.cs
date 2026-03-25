using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public static class GovernanceEditorSupport
{
    public static JsonObject ApplySandboxMute(
        JsonObject? rules,
        string? nodeType,
        string? nodeId,
        string? code,
        int minutes,
        DateTimeOffset nowUtc)
    {
        var safeRules = (rules?.DeepClone() as JsonObject) ?? new JsonObject();
        var muteMap = safeRules["mute_until_by_key"] as JsonObject ?? new JsonObject();
        safeRules["mute_until_by_key"] = muteMap;

        var normalizedNodeType = NormalizeOrWildcard(nodeType);
        var normalizedNodeId = NormalizeOrWildcard(nodeId);
        var normalizedCode = NormalizeOrWildcard(code);
        var normalizedMinutes = Math.Max(1, minutes);
        var muteUntil = nowUtc
            .ToUniversalTime()
            .AddMinutes(normalizedMinutes)
            .ToString("yyyy-MM-ddTHH:mm:ssZ");
        var key = $"{normalizedNodeType}::{normalizedNodeId}::{normalizedCode}";
        muteMap[key] = muteUntil;
        return safeRules;
    }

    private static string NormalizeOrWildcard(string? value)
    {
        var text = (value ?? string.Empty).Trim().ToLowerInvariant();
        return string.IsNullOrWhiteSpace(text) ? "*" : text;
    }
}

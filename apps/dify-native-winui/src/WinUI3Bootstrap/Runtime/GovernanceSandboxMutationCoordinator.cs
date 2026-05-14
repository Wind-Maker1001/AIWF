using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceSandboxMuteResult(
    JsonObject Rules,
    string VersionId);

public sealed class GovernanceSandboxMutationCoordinator
{
    private readonly Func<string, string?, JsonObject, string, CancellationToken, Task<string>> _saveRules;
    private readonly Func<string, string?, string, CancellationToken, Task<string>> _rollbackRules;
    private readonly Func<string, string?, GovernanceSandboxAutoFixState, CancellationToken, Task<GovernanceSandboxAutoFixState>> _saveAutoFixState;

    public GovernanceSandboxMutationCoordinator(GovernanceBridgeClient client)
        : this(
            client.SaveWorkflowSandboxRulesAsync,
            client.RollbackWorkflowSandboxRuleVersionAsync,
            client.SaveWorkflowSandboxAutoFixStateAsync)
    {
    }

    public GovernanceSandboxMutationCoordinator(
        Func<string, string?, JsonObject, string, CancellationToken, Task<string>> saveRules,
        Func<string, string?, string, CancellationToken, Task<string>> rollbackRules,
        Func<string, string?, GovernanceSandboxAutoFixState, CancellationToken, Task<GovernanceSandboxAutoFixState>> saveAutoFixState)
    {
        _saveRules = saveRules;
        _rollbackRules = rollbackRules;
        _saveAutoFixState = saveAutoFixState;
    }

    public Task<string> SaveRulesAsync(
        string baseUrl,
        string? apiKey,
        JsonObject rules,
        string reason = "winui_governance_edit",
        CancellationToken cancellationToken = default)
    {
        return _saveRules(baseUrl, apiKey, rules, reason, cancellationToken);
    }

    public Task<string> RollbackRulesAsync(
        string baseUrl,
        string? apiKey,
        string versionId,
        CancellationToken cancellationToken = default)
    {
        var normalizedVersionId = (versionId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedVersionId))
        {
            throw new InvalidOperationException("sandbox rule version id is required");
        }

        return _rollbackRules(baseUrl, apiKey, normalizedVersionId, cancellationToken);
    }

    public async Task<GovernanceSandboxMuteResult> ApplyMuteAsync(
        string baseUrl,
        string? apiKey,
        JsonObject currentRules,
        string? nodeType,
        string? nodeId,
        string? code,
        string? minutesText,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken = default)
    {
        var minutes = int.TryParse((minutesText ?? string.Empty).Trim(), out var parsedMinutes)
            ? parsedMinutes
            : 60;
        var nextRules = GovernanceEditorSupport.ApplySandboxMute(
            currentRules,
            nodeType,
            nodeId,
            code,
            minutes,
            nowUtc);
        var versionId = await _saveRules(
            baseUrl,
            apiKey,
            nextRules,
            "winui_governance_mute",
            cancellationToken);
        return new GovernanceSandboxMuteResult(nextRules, versionId);
    }

    public Task<GovernanceSandboxAutoFixState> SaveAutoFixStateAsync(
        string baseUrl,
        string? apiKey,
        GovernanceSandboxAutoFixState currentState,
        string? forcedIsolationMode,
        string? forcedUntil,
        string? greenStreakText,
        CancellationToken cancellationToken = default)
    {
        var greenStreak = int.TryParse((greenStreakText ?? string.Empty).Trim(), out var parsedGreen)
            ? Math.Max(0, parsedGreen)
            : 0;
        var nextState = currentState with
        {
            ForcedIsolationMode = (forcedIsolationMode ?? string.Empty).Trim(),
            ForcedUntil = (forcedUntil ?? string.Empty).Trim(),
            GreenStreak = greenStreak,
        };
        return _saveAutoFixState(baseUrl, apiKey, nextState, cancellationToken);
    }
}

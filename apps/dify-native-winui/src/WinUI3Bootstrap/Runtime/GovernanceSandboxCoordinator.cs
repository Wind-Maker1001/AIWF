using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceSandboxRefreshResult(
    JsonObject Rules,
    IReadOnlyList<GovernanceSandboxRuleVersionItem> Versions,
    GovernanceSandboxAutoFixState AutoFixState,
    IReadOnlyList<GovernanceSandboxAutoFixActionItem> AutoFixActions);

public sealed class GovernanceSandboxCoordinator
{
    private readonly Func<string, string?, CancellationToken, Task<JsonObject>> _getRules;
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceSandboxRuleVersionItem>>> _listVersions;
    private readonly Func<string, string?, CancellationToken, Task<GovernanceSandboxAutoFixState>> _getAutoFixState;
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceSandboxAutoFixActionItem>>> _listAutoFixActions;

    public GovernanceSandboxCoordinator(GovernanceBridgeClient client)
        : this(
            client.GetWorkflowSandboxRulesAsync,
            client.ListWorkflowSandboxRuleVersionsAsync,
            client.GetWorkflowSandboxAutoFixStateAsync,
            client.ListWorkflowSandboxAutoFixActionsAsync)
    {
    }

    public GovernanceSandboxCoordinator(
        Func<string, string?, CancellationToken, Task<JsonObject>> getRules,
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceSandboxRuleVersionItem>>> listVersions,
        Func<string, string?, CancellationToken, Task<GovernanceSandboxAutoFixState>> getAutoFixState,
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceSandboxAutoFixActionItem>>> listAutoFixActions)
    {
        _getRules = getRules;
        _listVersions = listVersions;
        _getAutoFixState = getAutoFixState;
        _listAutoFixActions = listAutoFixActions;
    }

    public async Task<GovernanceSandboxRefreshResult> RefreshAsync(
        string baseUrl,
        string? apiKey,
        CancellationToken cancellationToken = default)
    {
        var rulesTask = _getRules(baseUrl, apiKey, cancellationToken);
        var versionsTask = _listVersions(baseUrl, apiKey, 80, cancellationToken);
        var autoFixStateTask = _getAutoFixState(baseUrl, apiKey, cancellationToken);
        var autoFixActionsTask = _listAutoFixActions(baseUrl, apiKey, 60, cancellationToken);

        await Task.WhenAll(rulesTask, versionsTask, autoFixStateTask, autoFixActionsTask);

        return new GovernanceSandboxRefreshResult(
            Rules: await rulesTask,
            Versions: await versionsTask,
            AutoFixState: await autoFixStateTask,
            AutoFixActions: await autoFixActionsTask);
    }
}

using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record GovernanceQualityRuleSetRefreshResult(
    IReadOnlyList<GovernanceQualityRuleSetItem> Items,
    GovernanceQualityRuleSetItem? SelectedItem);

public sealed class GovernanceQualityRuleSetCoordinator
{
    private readonly Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceQualityRuleSetItem>>> _listQualityRuleSets;
    private readonly Func<string, string?, string, string, string, string, JsonObject, CancellationToken, Task<GovernanceQualityRuleSetItem>> _saveQualityRuleSet;
    private readonly Func<string, string?, string, CancellationToken, Task> _deleteQualityRuleSet;

    public GovernanceQualityRuleSetCoordinator(GovernanceBridgeClient client)
        : this(
            client.ListQualityRuleSetsAsync,
            client.SaveQualityRuleSetAsync,
            client.DeleteQualityRuleSetAsync)
    {
    }

    public GovernanceQualityRuleSetCoordinator(
        Func<string, string?, int, CancellationToken, Task<IReadOnlyList<GovernanceQualityRuleSetItem>>> listQualityRuleSets,
        Func<string, string?, string, string, string, string, JsonObject, CancellationToken, Task<GovernanceQualityRuleSetItem>> saveQualityRuleSet,
        Func<string, string?, string, CancellationToken, Task> deleteQualityRuleSet)
    {
        _listQualityRuleSets = listQualityRuleSets;
        _saveQualityRuleSet = saveQualityRuleSet;
        _deleteQualityRuleSet = deleteQualityRuleSet;
    }

    public async Task<GovernanceQualityRuleSetRefreshResult> RefreshAsync(
        string baseUrl,
        string? apiKey,
        string? selectedId,
        int limit = 80,
        CancellationToken cancellationToken = default)
    {
        var items = await _listQualityRuleSets(baseUrl, apiKey, limit, cancellationToken);
        var normalizedSelectedId = (selectedId ?? string.Empty).Trim();
        GovernanceQualityRuleSetItem? selectedItem = null;
        if (!string.IsNullOrWhiteSpace(normalizedSelectedId))
        {
            selectedItem = items.FirstOrDefault(item => string.Equals(item.Id, normalizedSelectedId, StringComparison.Ordinal));
        }

        selectedItem ??= items.FirstOrDefault();
        return new GovernanceQualityRuleSetRefreshResult(items, selectedItem);
    }

    public Task<GovernanceQualityRuleSetItem> SaveAsync(
        string baseUrl,
        string? apiKey,
        string id,
        string? name,
        string? version,
        JsonObject rules,
        CancellationToken cancellationToken = default)
    {
        var normalizedId = (id ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedId))
        {
            throw new InvalidOperationException("quality rule set id is required");
        }

        var normalizedName = string.IsNullOrWhiteSpace(name) ? normalizedId : name.Trim();
        var normalizedVersion = string.IsNullOrWhiteSpace(version) ? "v1" : version.Trim();
        return _saveQualityRuleSet(
            baseUrl,
            apiKey,
            normalizedId,
            normalizedName,
            normalizedVersion,
            "workflow",
            rules,
            cancellationToken);
    }

    public Task DeleteAsync(
        string baseUrl,
        string? apiKey,
        string id,
        CancellationToken cancellationToken = default)
    {
        var normalizedId = (id ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedId))
        {
            throw new InvalidOperationException("quality rule set id is required");
        }

        return _deleteQualityRuleSet(baseUrl, apiKey, normalizedId, cancellationToken);
    }
}

namespace AIWF.Native.Runtime;

public sealed record WorkflowVersionAdminRefreshResult(
    IReadOnlyList<GovernanceWorkflowVersionItem> Items,
    WorkflowVersionCacheStats Stats);

public sealed record WorkflowVersionAdminCompareResult(
    GovernanceWorkflowVersionCompareResult? Result,
    string ErrorCode,
    string ErrorMessage,
    IReadOnlyList<GovernanceErrorItem> ErrorItems,
    WorkflowVersionCacheStats Stats,
    bool FromCache);

internal sealed class WorkflowVersionAdminCoordinator
{
    private readonly GovernanceBridgeClient _governanceClient;
    private readonly WorkflowVersionCacheService _cacheService;

    public WorkflowVersionAdminCoordinator(
        GovernanceBridgeClient governanceClient,
        WorkflowVersionCacheService cacheService)
    {
        _governanceClient = governanceClient;
        _cacheService = cacheService;
    }

    public async Task<WorkflowVersionAdminRefreshResult> RefreshVersionsAsync(
        string baseUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        var items = await _governanceClient.ListWorkflowVersionsAsync(baseUrl, apiKey, 120, cancellationToken);
        _cacheService.SaveVersionList(items);
        return new WorkflowVersionAdminRefreshResult(items, _cacheService.GetStats());
    }

    public async Task<WorkflowVersionAdminCompareResult> CompareVersionsAsync(
        string baseUrl,
        string apiKey,
        string versionA,
        string versionB,
        CancellationToken cancellationToken = default)
    {
        var normalizedA = (versionA ?? string.Empty).Trim();
        var normalizedB = (versionB ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedA) || string.IsNullOrWhiteSpace(normalizedB))
        {
            return new WorkflowVersionAdminCompareResult(
                Result: null,
                ErrorCode: string.Empty,
                ErrorMessage: "Workflow version compare requires version A and B.",
                ErrorItems: Array.Empty<GovernanceErrorItem>(),
                Stats: _cacheService.GetStats(),
                FromCache: false);
        }

        if (_cacheService.TryGetCompareResult(normalizedA, normalizedB, out var cached) && cached is not null)
        {
            return new WorkflowVersionAdminCompareResult(
                cached,
                string.Empty,
                string.Empty,
                Array.Empty<GovernanceErrorItem>(),
                _cacheService.GetStats(),
                FromCache: true);
        }

        try
        {
            var result = await _governanceClient.CompareWorkflowVersionsAsync(
                baseUrl,
                apiKey,
                normalizedA,
                normalizedB,
                cancellationToken);
            _cacheService.SaveCompareResult(normalizedA, normalizedB, result);
            return new WorkflowVersionAdminCompareResult(
                result,
                string.Empty,
                string.Empty,
                Array.Empty<GovernanceErrorItem>(),
                _cacheService.GetStats(),
                FromCache: false);
        }
        catch (GovernanceRequestFailureException ex)
        {
            return new WorkflowVersionAdminCompareResult(
                Result: null,
                ErrorCode: ex.ErrorCode,
                ErrorMessage: ex.Message,
                ErrorItems: ex.ErrorItems,
                Stats: _cacheService.GetStats(),
                FromCache: false);
        }
        catch (InvalidOperationException ex)
        {
            return new WorkflowVersionAdminCompareResult(
                Result: null,
                ErrorCode: string.Empty,
                ErrorMessage: ex.Message,
                ErrorItems: Array.Empty<GovernanceErrorItem>(),
                Stats: _cacheService.GetStats(),
                FromCache: false);
        }
    }

    public Task<WorkflowVersionCacheStats> GetCacheStatsAsync()
    {
        return Task.FromResult(_cacheService.GetStats());
    }

    public Task<WorkflowVersionCacheStats> ClearCacheAsync()
    {
        return Task.FromResult(_cacheService.Clear());
    }
}

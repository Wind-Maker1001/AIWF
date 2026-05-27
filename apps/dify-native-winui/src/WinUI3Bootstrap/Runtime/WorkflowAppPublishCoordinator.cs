using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowAppPublishResult(
    bool Ok,
    GovernanceWorkflowAppItem? PublishedApp,
    GovernanceSavedWorkflowVersionItem? PublishedVersion,
    WorkflowAppPublishPreflightReport? PreflightReport,
    IReadOnlyList<GovernanceWorkflowAppItem> Items,
    string ErrorCode,
    string ErrorMessage,
    IReadOnlyList<GovernanceErrorItem> ErrorItems);

public sealed record WorkflowAppPublishFormState(
    string Name,
    string AppId,
    bool RequirePreflight,
    JsonObject ParamsSchema,
    JsonObject RuntimeDefaults,
    JsonObject TemplatePolicyPreview);

public sealed class WorkflowAppPublishCoordinator
{
    private readonly GovernanceBridgeClient _governanceClient;
    private readonly WorkflowAppPublishPreflightCoordinator _preflightCoordinator;
    private readonly Func<DateTimeOffset> _now;
    private readonly Func<string> _randomHex;

    public WorkflowAppPublishCoordinator(
        GovernanceBridgeClient governanceClient,
        WorkflowAppPublishPreflightCoordinator preflightCoordinator,
        Func<DateTimeOffset>? now = null,
        Func<string>? randomHex = null)
    {
        _governanceClient = governanceClient;
        _preflightCoordinator = preflightCoordinator;
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _randomHex = randomHex ?? (() => Guid.NewGuid().ToString("N")[..8]);
    }

    public async Task<IReadOnlyList<GovernanceWorkflowAppItem>> RefreshAppsAsync(
        string baseUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        return await _governanceClient.ListWorkflowAppsAsync(baseUrl, apiKey, 120, cancellationToken);
    }

    public async Task<WorkflowAppPublishResult> PublishAsync(
        string baseUrl,
        string apiKey,
        WorkflowAppPublishSourceContext sourceContext,
        WorkflowAppPublishFormState formState,
        CancellationToken cancellationToken = default)
    {
        WorkflowAppPublishPreflightReport? preflightReport = null;
        if (formState.RequirePreflight)
        {
            preflightReport = await _preflightCoordinator.RunPublishPreflightAsync(sourceContext, apiKey, cancellationToken);
            if (!preflightReport.Ok)
            {
                return new WorkflowAppPublishResult(
                    false,
                    null,
                    null,
                    preflightReport,
                    Array.Empty<GovernanceWorkflowAppItem>(),
                    string.Empty,
                    "Workflow app publish preflight failed.",
                    Array.Empty<GovernanceErrorItem>());
            }
        }

        var workflowDefinition = WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(sourceContext.Document);
        var workflowId = (sourceContext.Document.WorkflowId ?? string.Empty).Trim();
        var workflowName = string.IsNullOrWhiteSpace(formState.Name)
            ? workflowId
            : formState.Name.Trim();
        var versionId = BuildVersionId(
            workflowId,
            formState.Name,
            formState.AppId);
        var appId = BuildAppId(
            formState.AppId,
            formState.Name,
            workflowId);

        try
        {
            var savedVersion = await _governanceClient.SaveWorkflowVersionAsync(
                baseUrl,
                apiKey,
                versionId,
                workflowId,
                workflowName,
                workflowDefinition,
                cancellationToken);
            var savedApp = await _governanceClient.SaveWorkflowAppAsync(
                baseUrl,
                apiKey,
                appId,
                workflowName,
                workflowId,
                savedVersion.VersionId,
                formState.ParamsSchema,
                formState.TemplatePolicyPreview,
                cancellationToken);
            var items = await RefreshAppsAsync(baseUrl, apiKey, cancellationToken);
            return new WorkflowAppPublishResult(
                true,
                savedApp,
                savedVersion,
                preflightReport,
                items,
                string.Empty,
                string.Empty,
                Array.Empty<GovernanceErrorItem>());
        }
        catch (GovernanceRequestFailureException ex)
        {
            return new WorkflowAppPublishResult(
                false,
                null,
                null,
                preflightReport,
                Array.Empty<GovernanceWorkflowAppItem>(),
                ex.ErrorCode,
                ex.Message,
                ex.ErrorItems);
        }
        catch (InvalidOperationException ex)
        {
            return new WorkflowAppPublishResult(
                false,
                null,
                null,
                preflightReport,
                Array.Empty<GovernanceWorkflowAppItem>(),
                string.Empty,
                ex.Message,
                Array.Empty<GovernanceErrorItem>());
        }
    }

    private string BuildVersionId(string workflowId, string name, string appId)
    {
        var source = SanitizeKey(string.IsNullOrWhiteSpace(workflowId) ? name : workflowId);
        if (string.IsNullOrWhiteSpace(source))
        {
            source = SanitizeKey(appId);
        }
        if (string.IsNullOrWhiteSpace(source))
        {
            source = "workflow";
        }
        return $"{source}_published_{_now():yyyyMMddHHmmss}_{_randomHex()}";
    }

    private string BuildAppId(string appId, string name, string workflowId)
    {
        var explicitId = SanitizeKey(appId);
        if (!string.IsNullOrWhiteSpace(explicitId))
        {
            return explicitId;
        }
        var source = SanitizeKey(name);
        if (string.IsNullOrWhiteSpace(source))
        {
            source = SanitizeKey(workflowId);
        }
        if (string.IsNullOrWhiteSpace(source))
        {
            source = "workflow";
        }
        return $"{source}_app_{_now():yyyyMMddHHmmss}_{_randomHex()}";
    }

    private static string SanitizeKey(string value)
    {
        var source = (value ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(source))
        {
            return string.Empty;
        }

        var chars = source.Select(ch => (char.IsLetterOrDigit(ch) || ch is '.' or '_' or '-') ? ch : '_').ToArray();
        var normalized = new string(chars);
        while (normalized.Contains("__", StringComparison.Ordinal))
        {
            normalized = normalized.Replace("__", "_", StringComparison.Ordinal);
        }
        normalized = normalized.Trim('_');
        if (normalized.Length > 80)
        {
            normalized = normalized[..80];
        }
        return normalized;
    }
}

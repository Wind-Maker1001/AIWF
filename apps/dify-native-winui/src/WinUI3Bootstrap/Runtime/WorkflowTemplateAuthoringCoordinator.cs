using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowTemplateApplyResult(
    bool Ok,
    WorkflowGraphDocument? Document,
    WorkflowTemplateCatalogItem? Template,
    string StatusMessage);

public sealed record WorkflowTemplateMutationResult(
    bool Ok,
    IReadOnlyList<WorkflowTemplateCatalogItem> Items,
    string SelectedTemplateId,
    string StatusMessage,
    string OutputPath = "");

public sealed class WorkflowTemplateAuthoringCoordinator
{
    private readonly WorkflowRunnerAdapter _runnerAdapter;
    private readonly WorkflowTemplateCatalogService _catalogService;
    private readonly WorkflowTemplateLocalStoreService _localStoreService;
    private readonly WorkflowTemplatePackService _packService;
    private readonly Func<DateTimeOffset> _now;
    private readonly Func<string> _randomHex;

    public WorkflowTemplateAuthoringCoordinator(
        WorkflowRunnerAdapter runnerAdapter,
        WorkflowTemplateCatalogService catalogService,
        WorkflowTemplateLocalStoreService localStoreService,
        WorkflowTemplatePackService packService,
        Func<DateTimeOffset>? now = null,
        Func<string>? randomHex = null)
    {
        _runnerAdapter = runnerAdapter;
        _catalogService = catalogService;
        _localStoreService = localStoreService;
        _packService = packService;
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _randomHex = randomHex ?? (() => Guid.NewGuid().ToString("N")[..8]);
    }

    public Task<IReadOnlyList<WorkflowTemplateCatalogItem>> RefreshTemplatesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_catalogService.LoadAll());
    }

    public async Task<WorkflowTemplateApplyResult> ApplySelectedTemplateAsync(
        WorkflowTemplateCatalogItem template,
        JsonObject templateParams,
        string accelUrl,
        string apiKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(templateParams);

        WorkflowAppSchemaSupport.ValidateParams(template.ParamsSchema, templateParams);
        var applied = WorkflowTemplateContractSupport.ApplyTemplateVars(template.WorkflowDefinition, templateParams) as JsonObject
            ?? throw new InvalidOperationException("template workflow_definition is required");

        var validation = await _runnerAdapter.PostJsonAsync(
            accelUrl,
            apiKey,
            "/operators/workflow_contract_v1/validate",
            new JsonObject
            {
                ["workflow_definition"] = applied,
                ["allow_version_migration"] = false,
                ["require_non_empty_nodes"] = true,
                ["validation_scope"] = "authoring",
            },
            cancellationToken);

        var valid = validation["valid"]?.GetValue<bool?>() != false
            && !string.Equals(validation["status"]?.GetValue<string>(), "invalid", StringComparison.OrdinalIgnoreCase);
        if (!valid)
        {
            var message = validation["error_items"] is JsonArray errorItems && errorItems.OfType<JsonObject>().FirstOrDefault() is JsonObject firstError
                ? firstError["message"]?.GetValue<string>() ?? "Template validation failed."
                : validation["error"]?.GetValue<string>() ?? "Template validation failed.";
            return new WorkflowTemplateApplyResult(false, null, template, message);
        }

        var effectiveDefinition = validation["workflow_definition"] as JsonObject ?? applied;
        var document = WorkflowCanvasDocumentBuilder.ImportWorkflowDefinition(effectiveDefinition);
        return new WorkflowTemplateApplyResult(
            true,
            document,
            template,
            $"Workflow template applied: {template.Name}");
    }

    public Task<WorkflowTemplateMutationResult> SaveCurrentAsTemplateAsync(
        WorkflowGraphDocument document,
        string templateName,
        JsonObject paramsSchema,
        JsonObject runtimeDefaults,
        bool requirePreflight,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(document);

        var normalizedName = string.IsNullOrWhiteSpace(templateName)
            ? document.WorkflowId
            : templateName.Trim();
        var item = new WorkflowTemplateCatalogItem(
            Id: BuildTemplateId(document.WorkflowId, normalizedName),
            Name: normalizedName,
            Origin: "local",
            PackId: string.Empty,
            PackName: string.Empty,
            WorkflowDefinition: WorkflowCanvasDocumentBuilder.SerializeWorkflowDefinition(document),
            ParamsSchema: WorkflowAppSchemaSupport.NormalizeSchemaObject(paramsSchema),
            Governance: WorkflowAppSchemaSupport.BuildGovernance(requirePreflight),
            RuntimeDefaults: WorkflowTemplateContractSupport.CloneObject(runtimeDefaults),
            TemplateSpecVersion: 1,
            CreatedAt: _now().ToString("O"));
        _localStoreService.SaveTemplate(item);
        var items = _catalogService.LoadAll();
        return Task.FromResult(new WorkflowTemplateMutationResult(
            true,
            items,
            item.Id,
            $"Workflow template saved: {item.Name}"));
    }

    public Task<WorkflowTemplateMutationResult> InstallTemplatePackAsync(
        string filePath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pack = _packService.InstallFromPath(filePath);
        var items = _catalogService.LoadAll();
        var selectedId = pack.Templates.FirstOrDefault()?.Id ?? string.Empty;
        return Task.FromResult(new WorkflowTemplateMutationResult(
            true,
            items,
            selectedId,
            $"Workflow template pack installed: {pack.Name}"));
    }

    public Task<WorkflowTemplateMutationResult> RemoveTemplatePackAsync(
        string packId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_packService.RemovePack(packId))
        {
            throw new InvalidOperationException("template pack not found");
        }

        var items = _catalogService.LoadAll();
        return Task.FromResult(new WorkflowTemplateMutationResult(
            true,
            items,
            string.Empty,
            $"Workflow template pack removed: {packId}"));
    }

    public Task<WorkflowTemplateMutationResult> ExportTemplatePackAsync(
        string packId,
        string outputPath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var exported = _packService.ExportPack(packId, outputPath);
        var items = _catalogService.LoadAll();
        return Task.FromResult(new WorkflowTemplateMutationResult(
            true,
            items,
            string.Empty,
            $"Workflow template pack exported: {exported}",
            exported));
    }

    private string BuildTemplateId(string workflowId, string templateName)
    {
        var source = SanitizeKey(templateName);
        if (string.IsNullOrWhiteSpace(source))
        {
            source = SanitizeKey(workflowId);
        }

        if (string.IsNullOrWhiteSpace(source))
        {
            source = "template";
        }

        return $"{source}_{_now():yyyyMMddHHmmss}_{_randomHex()}";
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

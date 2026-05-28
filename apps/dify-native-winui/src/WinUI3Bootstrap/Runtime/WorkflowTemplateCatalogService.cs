using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class WorkflowTemplateCatalogService
{
    private readonly string _builtinSnapshotPath;
    private readonly WorkflowTemplateLocalStoreService _localStoreService;
    private readonly WorkflowTemplatePackService _packService;

    public WorkflowTemplateCatalogService(
        WorkflowTemplateLocalStoreService localStoreService,
        WorkflowTemplatePackService packService,
        string? builtinSnapshotPath = null)
    {
        _localStoreService = localStoreService;
        _packService = packService;
        _builtinSnapshotPath = string.IsNullOrWhiteSpace(builtinSnapshotPath)
            ? Path.Combine(AppContext.BaseDirectory, "contracts", "desktop", "workflow_builtin_templates.v1.json")
            : Path.GetFullPath(builtinSnapshotPath);
    }

    public IReadOnlyList<WorkflowTemplateCatalogItem> LoadAll()
    {
        var items = new List<WorkflowTemplateCatalogItem>();
        items.AddRange(LoadBuiltinTemplates());
        items.AddRange(_localStoreService.LoadAll());
        items.AddRange(_packService.LoadPacks().SelectMany(pack => pack.Templates));
        return items
            .OrderBy(item => OriginOrder(item.Origin))
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private IReadOnlyList<WorkflowTemplateCatalogItem> LoadBuiltinTemplates()
    {
        try
        {
            if (!File.Exists(_builtinSnapshotPath))
            {
                return Array.Empty<WorkflowTemplateCatalogItem>();
            }

            var root = JsonNode.Parse(File.ReadAllText(_builtinSnapshotPath, System.Text.Encoding.UTF8)) as JsonObject;
            if (root is null)
            {
                return Array.Empty<WorkflowTemplateCatalogItem>();
            }

            return WorkflowTemplateContractSupport.ParseBuiltinSnapshot(root);
        }
        catch
        {
            return Array.Empty<WorkflowTemplateCatalogItem>();
        }
    }

    private static int OriginOrder(string origin)
    {
        return origin switch
        {
            "builtin" => 0,
            "local" => 1,
            _ => 2,
        };
    }
}

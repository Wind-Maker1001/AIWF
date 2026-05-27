using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class WorkflowTemplatePackService
{
    private readonly string _catalogPath;

    public WorkflowTemplatePackService(string? catalogPath = null)
    {
        _catalogPath = string.IsNullOrWhiteSpace(catalogPath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AIWF", "workflow_store", "template_marketplace.json")
            : Path.GetFullPath(catalogPath);
    }

    public string CatalogPath => _catalogPath;

    public IReadOnlyList<WorkflowTemplatePackCatalogItem> LoadPacks()
    {
        try
        {
            if (!File.Exists(_catalogPath))
            {
                return Array.Empty<WorkflowTemplatePackCatalogItem>();
            }

            var root = JsonNode.Parse(File.ReadAllText(_catalogPath, Encoding.UTF8)) as JsonObject;
            var items = root?["items"] as JsonArray;
            if (items is null) return Array.Empty<WorkflowTemplatePackCatalogItem>();
            return items
                .OfType<JsonObject>()
                .Select(WorkflowTemplateContractSupport.NormalizeTemplatePackCatalogItem)
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch
        {
            return Array.Empty<WorkflowTemplatePackCatalogItem>();
        }
    }

    public WorkflowTemplatePackCatalogItem InstallFromPath(string filePath)
    {
        var root = JsonNode.Parse(File.ReadAllText(filePath, Encoding.UTF8)) as JsonObject
            ?? throw new InvalidOperationException("template pack must be an object");
        var pack = WorkflowTemplateContractSupport.NormalizeTemplatePackArtifact(root, filePath);
        var packs = LoadPacks().ToList();
        var index = packs.FindIndex(item => string.Equals(item.Id, pack.Id, StringComparison.Ordinal));
        if (index >= 0) packs[index] = pack;
        else packs.Insert(0, pack);
        SavePacks(packs);
        return pack;
    }

    public bool RemovePack(string packId)
    {
        var packs = LoadPacks().ToList();
        var removed = packs.RemoveAll(item => string.Equals(item.Id, packId, StringComparison.Ordinal)) > 0;
        if (removed)
        {
            SavePacks(packs);
        }
        return removed;
    }

    public string ExportPack(string packId, string outputPath)
    {
        var pack = LoadPacks().FirstOrDefault(item => string.Equals(item.Id, packId, StringComparison.Ordinal))
            ?? throw new InvalidOperationException("template pack not found");
        var payload = WorkflowTemplateContractSupport.SerializeTemplatePackArtifact(pack);
        var dir = Path.GetDirectoryName(outputPath) ?? ".";
        Directory.CreateDirectory(dir);
        File.WriteAllText(outputPath, JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }), Encoding.UTF8);
        return outputPath;
    }

    private void SavePacks(IReadOnlyList<WorkflowTemplatePackCatalogItem> packs)
    {
        var payload = new JsonObject
        {
            ["items"] = new JsonArray(packs.Select(WorkflowTemplateContractSupport.SerializeTemplatePackItem).ToArray()),
        };
        var dir = Path.GetDirectoryName(_catalogPath) ?? ".";
        Directory.CreateDirectory(dir);
        File.WriteAllText(_catalogPath, JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }), Encoding.UTF8);
    }
}

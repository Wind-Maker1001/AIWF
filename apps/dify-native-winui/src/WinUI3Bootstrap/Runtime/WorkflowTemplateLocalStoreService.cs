using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed class WorkflowTemplateLocalStoreService
{
    private readonly string _filePath;

    public WorkflowTemplateLocalStoreService(string? filePath = null)
    {
        _filePath = string.IsNullOrWhiteSpace(filePath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "AIWF", "workflow_store", "local_templates.json")
            : Path.GetFullPath(filePath);
    }

    public string FilePath => _filePath;

    public IReadOnlyList<WorkflowTemplateCatalogItem> LoadAll()
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                return Array.Empty<WorkflowTemplateCatalogItem>();
            }

            var root = JsonNode.Parse(File.ReadAllText(_filePath, Encoding.UTF8));
            var items = root switch
            {
                JsonArray rawArray => rawArray,
                JsonObject source => source["items"] as JsonArray,
                _ => null,
            };
            if (items is null) return Array.Empty<WorkflowTemplateCatalogItem>();
            return items
                .OfType<JsonObject>()
                .Select((item, index) => WorkflowTemplateContractSupport.NormalizeTemplateItem(item, "local", $"custom_{index + 1}", $"custom_{index + 1}"))
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch
        {
            return Array.Empty<WorkflowTemplateCatalogItem>();
        }
    }

    public void SaveAll(IReadOnlyList<WorkflowTemplateCatalogItem> items)
    {
        var payload = WorkflowTemplateContractSupport.BuildLocalTemplateStorage(items);
        var dir = Path.GetDirectoryName(_filePath) ?? ".";
        Directory.CreateDirectory(dir);
        File.WriteAllText(_filePath, JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }), Encoding.UTF8);
    }

    public WorkflowTemplateCatalogItem SaveTemplate(WorkflowTemplateCatalogItem item)
    {
        var items = LoadAll().ToList();
        var index = items.FindIndex(existing => string.Equals(existing.Id, item.Id, StringComparison.Ordinal));
        if (index >= 0)
        {
            items[index] = item;
        }
        else
        {
            items.Insert(0, item);
        }

        SaveAll(items);
        return item;
    }
}

using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace AIWF.Native.Runtime;

internal static class WorkflowTemplateContractSupport
{
    public const string LocalTemplateStorageSchemaVersion = "local_template_storage.v1";
    public const string LocalTemplateEntrySchemaVersion = "local_template_entry.v1";
    public const string TemplatePackEntrySchemaVersion = "template_pack_entry.v1";
    public const string TemplatePackArtifactSchemaVersion = "template_pack_artifact.v1";
    private static readonly Regex ExactPlaceholderPattern = new(@"^\{\{\s*([A-Za-z0-9_]+)\s*\}\}$", RegexOptions.Compiled);
    private static readonly Regex PlaceholderPattern = new(@"\{\{\s*([A-Za-z0-9_]+)\s*\}\}", RegexOptions.Compiled);

    public static JsonObject CloneObject(JsonObject? source)
    {
        return source is null
            ? new JsonObject()
            : JsonNode.Parse(source.ToJsonString()) as JsonObject ?? new JsonObject();
    }

    public static JsonNode? CloneNode(JsonNode? source)
    {
        return source?.DeepClone();
    }

    public static JsonObject ResolveWorkflowDefinition(JsonObject source, bool allowLegacyGraphAlias = true)
    {
        if (source["workflow_definition"] is JsonObject workflowDefinition)
        {
            return CloneObject(workflowDefinition);
        }

        if (allowLegacyGraphAlias && source["graph"] is JsonObject legacyGraph)
        {
            return CloneObject(legacyGraph);
        }

        throw new InvalidOperationException("template workflow_definition is required");
    }

    public static IReadOnlyList<WorkflowTemplateCatalogItem> ParseBuiltinSnapshot(JsonObject root)
    {
        var items = root["items"] as JsonArray;
        if (items is null)
        {
            return Array.Empty<WorkflowTemplateCatalogItem>();
        }

        return items
            .OfType<JsonObject>()
            .Select((item, index) => NormalizeTemplateItem(
                item,
                "builtin",
                $"builtin_{index + 1}",
                $"builtin_{index + 1}"))
            .ToArray();
    }

    public static WorkflowTemplateCatalogItem NormalizeTemplateItem(
        JsonObject source,
        string origin,
        string defaultId,
        string defaultName,
        string packId = "",
        string packName = "")
    {
        var workflowDefinition = ResolveWorkflowDefinition(source, allowLegacyGraphAlias: true);
        if (string.IsNullOrWhiteSpace(workflowDefinition["version"]?.GetValue<string>() ?? string.Empty))
        {
            throw new InvalidOperationException("template workflow_definition.version is required");
        }

        var id = (source["id"]?.GetValue<string>() ?? defaultId).Trim();
        var name = (source["name"]?.GetValue<string>() ?? defaultName).Trim();
        if (string.IsNullOrWhiteSpace(id)) throw new InvalidOperationException("template id is required");
        if (string.IsNullOrWhiteSpace(name)) throw new InvalidOperationException("template name is required");

        return new WorkflowTemplateCatalogItem(
            Id: id,
            Name: name,
            Origin: origin,
            PackId: packId,
            PackName: packName,
            WorkflowDefinition: workflowDefinition,
            ParamsSchema: source["params_schema"] as JsonObject is JsonObject schema
                ? WorkflowAppSchemaSupport.NormalizeSchemaObject(schema)
                : new JsonObject(),
            Governance: source["governance"] as JsonObject is JsonObject governance ? CloneObject(governance) : new JsonObject(),
            RuntimeDefaults: source["runtime_defaults"] as JsonObject is JsonObject defaults ? CloneObject(defaults) : new JsonObject(),
            TemplateSpecVersion: source["template_spec_version"]?.GetValue<int?>() ?? 1,
            CreatedAt: source["created_at"]?.GetValue<string>() ?? string.Empty);
    }

    public static WorkflowTemplatePackCatalogItem NormalizeTemplatePackCatalogItem(JsonObject source)
    {
        var packId = (source["id"]?.GetValue<string>() ?? string.Empty).Trim();
        var packName = (source["name"]?.GetValue<string>() ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(packId))
        {
            throw new InvalidOperationException("template pack id is required");
        }

        if (string.IsNullOrWhiteSpace(packName))
        {
            throw new InvalidOperationException("template pack name is required");
        }

        var templates = (source["templates"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select((template, index) => NormalizeTemplateItem(
                template,
                "pack",
                $"tpl_{index + 1}",
                $"template_{index + 1}",
                packId,
                packName))
            .ToArray() ?? Array.Empty<WorkflowTemplateCatalogItem>();
        return new WorkflowTemplatePackCatalogItem(
            packId,
            packName,
            source["version"]?.GetValue<string>() ?? "v1",
            source["source"]?.GetValue<string>() ?? "unknown",
            source["created_at"]?.GetValue<string>() ?? string.Empty,
            templates);
    }

    public static WorkflowTemplatePackCatalogItem NormalizeTemplatePackArtifact(JsonObject source, string sourcePath)
    {
        var packId = (source["id"]?.GetValue<string>() ?? string.Empty).Trim();
        var packName = (source["name"]?.GetValue<string>() ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(packId))
        {
            throw new InvalidOperationException("template pack id is required");
        }

        if (string.IsNullOrWhiteSpace(packName))
        {
            throw new InvalidOperationException("template pack name is required");
        }

        var templates = (source["templates"] as JsonArray)
            ?.OfType<JsonObject>()
            .Select((template, index) => NormalizeTemplateItem(
                template,
                "pack",
                $"tpl_{index + 1}",
                $"template_{index + 1}",
                packId,
                packName))
            .ToArray() ?? Array.Empty<WorkflowTemplateCatalogItem>();
        if (templates.Length == 0)
        {
            throw new InvalidOperationException("template pack templates are required");
        }

        return new WorkflowTemplatePackCatalogItem(
            packId,
            packName,
            source["version"]?.GetValue<string>() ?? "v1",
            string.IsNullOrWhiteSpace(sourcePath) ? "artifact" : sourcePath,
            source["created_at"]?.GetValue<string>() ?? string.Empty,
            templates);
    }

    public static JsonObject SerializeTemplateItem(WorkflowTemplateCatalogItem item)
    {
        return new JsonObject
        {
            ["schema_version"] = LocalTemplateEntrySchemaVersion,
            ["id"] = item.Id,
            ["name"] = item.Name,
            ["workflow_definition"] = item.WorkflowDefinition.DeepClone(),
            ["template_spec_version"] = item.TemplateSpecVersion,
            ["params_schema"] = item.ParamsSchema.DeepClone(),
            ["governance"] = item.Governance.DeepClone(),
            ["runtime_defaults"] = item.RuntimeDefaults.DeepClone(),
            ["created_at"] = item.CreatedAt,
        };
    }

    public static JsonObject SerializeTemplatePackItem(WorkflowTemplatePackCatalogItem pack)
    {
        var templates = new JsonArray();
        foreach (var template in pack.Templates)
        {
            templates.Add(new JsonObject
            {
                ["id"] = template.Id,
                ["name"] = template.Name,
                ["workflow_definition"] = template.WorkflowDefinition.DeepClone(),
                ["template_spec_version"] = template.TemplateSpecVersion,
                ["params_schema"] = template.ParamsSchema.DeepClone(),
                ["governance"] = template.Governance.DeepClone(),
                ["runtime_defaults"] = template.RuntimeDefaults.DeepClone(),
                ["created_at"] = template.CreatedAt,
            });
        }

        return new JsonObject
        {
            ["schema_version"] = TemplatePackEntrySchemaVersion,
            ["id"] = pack.Id,
            ["name"] = pack.Name,
            ["version"] = pack.Version,
            ["source"] = pack.Source,
            ["created_at"] = pack.CreatedAt,
            ["templates"] = templates,
        };
    }

    public static JsonObject SerializeTemplatePackArtifact(WorkflowTemplatePackCatalogItem pack)
    {
        var payload = SerializeTemplatePackItem(pack);
        payload["schema_version"] = TemplatePackArtifactSchemaVersion;
        payload["source"] = "marketplace_export";
        return payload;
    }

    public static JsonObject BuildLocalTemplateStorage(IReadOnlyList<WorkflowTemplateCatalogItem> items)
    {
        return new JsonObject
        {
            ["schema_version"] = LocalTemplateStorageSchemaVersion,
            ["items"] = new JsonArray(items.Select(SerializeTemplateItem).ToArray()),
        };
    }

    public static JsonNode? ApplyTemplateVars(JsonNode? value, JsonObject parameters)
    {
        if (value is null)
        {
            return null;
        }

        if (value is JsonArray array)
        {
            var clone = new JsonArray();
            foreach (var item in array)
            {
                clone.Add(ApplyTemplateVars(item, parameters));
            }

            return clone;
        }

        if (value is JsonObject obj)
        {
            var clone = new JsonObject();
            foreach (var property in obj)
            {
                clone[property.Key] = ApplyTemplateVars(property.Value, parameters);
            }

            return clone;
        }

        if (value is not JsonValue jsonValue || !jsonValue.TryGetValue<string>(out var text))
        {
            return value.DeepClone();
        }

        var exact = ExactPlaceholderPattern.Match(text);
        if (exact.Success)
        {
            var key = exact.Groups[1].Value;
            return parameters.ContainsKey(key)
                ? parameters[key]?.DeepClone()
                : JsonValue.Create(text);
        }

        var replaced = PlaceholderPattern.Replace(text, match =>
        {
            var key = match.Groups[1].Value;
            return parameters.ContainsKey(key)
                ? JsonNodeToTemplateString(parameters[key])
                : match.Value;
        });
        return JsonValue.Create(replaced);
    }

    private static string JsonNodeToTemplateString(JsonNode? value)
    {
        return value switch
        {
            null => string.Empty,
            JsonValue jsonValue when jsonValue.TryGetValue<string>(out var text) => text,
            JsonValue jsonValue when jsonValue.TryGetValue<bool>(out var booleanValue) => booleanValue ? "true" : "false",
            JsonValue jsonValue when jsonValue.TryGetValue<double>(out var numberValue) => numberValue.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => value.ToJsonString(),
        };
    }
}

public sealed record WorkflowTemplateCatalogItem(
    string Id,
    string Name,
    string Origin,
    string PackId,
    string PackName,
    JsonObject WorkflowDefinition,
    JsonObject ParamsSchema,
    JsonObject Governance,
    JsonObject RuntimeDefaults,
    int TemplateSpecVersion,
    string CreatedAt)
{
    public string DisplayText =>
        string.IsNullOrWhiteSpace(PackId)
            ? $"{Name} | {Origin}"
            : $"{Name} | {Origin}:{PackId}";

    public override string ToString() => DisplayText;
}

public sealed record WorkflowTemplatePackCatalogItem(
    string Id,
    string Name,
    string Version,
    string Source,
    string CreatedAt,
    IReadOnlyList<WorkflowTemplateCatalogItem> Templates);

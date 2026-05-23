using System.Text.Json.Nodes;

namespace AIWF.Native.Runtime;

public sealed record WorkflowAppSchemaField(
    string Key,
    string Type,
    bool Required,
    JsonNode? DefaultValue,
    string Description);

public static class WorkflowAppSchemaSupport
{
    public static JsonObject ParseSchemaJson(string raw)
    {
        var text = (raw ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return new JsonObject();
        }

        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(text);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(BuildSchemaErrorText($"Schema JSON parse failed: {ex.Message}"));
        }

        if (parsed is not JsonObject obj)
        {
            throw new InvalidOperationException(BuildSchemaErrorText("Schema JSON must be an object."));
        }

        return NormalizeSchemaObject(obj);
    }

    public static JsonObject NormalizeSchemaObject(JsonObject? source)
    {
        var result = new JsonObject();
        if (source is null)
        {
            return result;
        }

        foreach (var property in source)
        {
            var key = (property.Key ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            var rule = property.Value as JsonObject;
            var type = ((rule?["type"]?.GetValue<string>()) ?? "string").Trim();
            if (string.IsNullOrWhiteSpace(type))
            {
                type = "string";
            }

            var normalized = new JsonObject
            {
                ["type"] = type,
            };

            if (rule?["required"]?.GetValue<bool?>() == true)
            {
                normalized["required"] = true;
            }

            if (rule is not null && rule.ContainsKey("default"))
            {
                normalized["default"] = rule["default"]?.DeepClone();
            }

            var description = (rule?["description"]?.GetValue<string>() ?? string.Empty).Trim();
            if (!string.IsNullOrWhiteSpace(description))
            {
                normalized["description"] = description;
            }

            result[key] = normalized;
        }

        return result;
    }

    public static IReadOnlyList<WorkflowAppSchemaField> EnumerateFields(JsonObject schema)
    {
        var normalized = NormalizeSchemaObject(schema);
        return normalized.Select(property =>
            new WorkflowAppSchemaField(
                property.Key,
                ((property.Value as JsonObject)?["type"]?.GetValue<string>()) ?? "string",
                ((property.Value as JsonObject)?["required"]?.GetValue<bool?>()) == true,
                (property.Value as JsonObject)?["default"]?.DeepClone(),
                ((property.Value as JsonObject)?["description"]?.GetValue<string>()) ?? string.Empty))
            .ToArray();
    }

    public static string BuildSchemaErrorText(string message)
    {
        return string.IsNullOrWhiteSpace(message) ? "Schema JSON is invalid." : message;
    }
}
